// Package orasstate provides an OCI registry client for storing state as
// OCI artifacts using the ORAS (OCI Registry As Storage) protocol.
//
// State is stored as OCI image manifests tagged with a workspace-derived name.
// Locking is implemented via a separate lock manifest with generation-based
// optimistic concurrency control to detect simultaneous lock attempts.
package orasstate

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"golang.org/x/sync/errgroup"
	oras "oras.land/oras-go/v2"
	"oras.land/oras-go/v2/errdef"
	orasErrcode "oras.land/oras-go/v2/registry/remote/errcode"
)

const (
	mediaTypeStateLayer     = "application/vnd.terraform.statefile.v1"
	mediaTypeStateLayerGzip = "application/vnd.terraform.statefile.v1+gzip"
	artifactTypeState       = "application/vnd.terraform.state.v1"
	artifactTypeLock        = "application/vnd.terraform.lock.v1"

	annotationWorkspace    = "org.terraform.workspace"
	annotationUpdatedAt    = "org.terraform.state.updated_at"
	annotationStateVersion = "org.terraform.state.version"
	annotationStateHistory = "org.terraform.state.history"
	annotationLockID       = "org.terraform.lock.id"
	annotationLockInfo     = "org.terraform.lock.info"
	annotationLockGen      = "org.terraform.lock.generation"
)

// annotationCreated is the OCI image-spec created timestamp. PackManifest
// stamps the wall clock when it is absent, which would make every pack of
// the same logical publication produce a different digest; puts fix it to a
// single value per logical Put so packing is reproducible.
const annotationCreated = ocispec.AnnotationCreated

// defaultMaxStateSize is the default upper bound on state data read from the
// registry. It guards against OOM caused by a maliciously large or corrupted
// OCI layer (256 MiB).
const defaultMaxStateSize int64 = 256 * 1024 * 1024

// maxManifestSize bounds manifest reads from the registry. Manifests are tiny
// JSON documents (1 MiB is generous); the cap prevents OOM on a corrupted or
// malicious response.
// ponytail: fixed 1 MiB cap; raise if OCI manifests ever legitimately exceed it.
const maxManifestSize int64 = 1 << 20

// lockStabilityDelay is the wait between the first and second lock-tag reads
// in lock(). It widens the observed window enough to catch a rival that tags
// between our first verification and the stability re-read, without adding a
// noticeable latency to lock acquisition.
const lockStabilityDelay = 100 * time.Millisecond

// lockCleanupTimeout bounds the lock-tag cleanup that runs with a context
// detached from the caller's: by the time cleanup runs, the caller's context
// may already be cancelled (stability window expired, verification failed),
// and a cancelled context would make the cleanup itself fail.
const lockCleanupTimeout = 30 * time.Second

// Tag naming scheme:
//   - State is stored at "state-<workspaceTag>".
//   - State versions are stored at "stver-<workspaceTag>-v<N>".
//   - Lock is stored at "locked-<workspaceTag>".
//   - On registries that don't support manifest deletion (GHCR returns 405),
//     unlock retags to "unlocked-<workspaceTag>" instead.
const (
	stateTagPrefix           = "state-"
	lockTagPrefix            = "locked-"
	unlockedTagPrefix        = "unlocked-"
	stateVersionTagPrefix    = "stver-"
	stateVersionTagSeparator = "-v"
)

// maxStateVersion bounds version numbers (32-bit safe); tags beyond it are
// not recognized as version tags.
const maxStateVersion = 1 << 30

// ─── Exported types ───────────────────────────────────────────────────────────

// LockInfo holds information about a state lock.
type LockInfo struct {
	// ID is the unique identifier for this lock.
	ID string `json:"ID"`
	// Operation is the Terraform operation holding the lock.
	Operation string `json:"Operation"`
	// Info is additional free-form information about the lock holder.
	Info string `json:"Info"`
	// Who is the user/host holding the lock.
	Who string `json:"Who"`
	// Version is the Terraform version.
	Version string `json:"Version"`
	// Created is the time the lock was acquired.
	Created time.Time `json:"Created"`
	// Path is the state path for this lock.
	Path string `json:"Path"`
}

// errStateLocked is the contention error carried inside a *LockError.
var errStateLocked = errors.New("state is locked")

// LockError is returned when a state lock operation fails due to contention.
type LockError struct {
	// Info is the lock information of the current holder, if available.
	Info *LockInfo
	// Err is the underlying error.
	Err error
}

// Error implements the error interface.
func (e *LockError) Error() string {
	if e.Err != nil {
		return e.Err.Error()
	}
	if e.Info != nil {
		return fmt.Sprintf("state locked by %s (%s)", e.Info.Who, e.Info.ID)
	}
	return "state is locked"
}

// Unwrap exposes the underlying error so errors.Is(err, errStateLocked) works.
func (e *LockError) Unwrap() error { return e.Err }

// ErrMutableTagMoved is returned when, after a transient (lost or failed)
// response while publishing to a mutable OCI tag, the tag resolves to a
// foreign manifest — or its state cannot be determined. OCI tags have no CAS,
// so re-applying the publication would blindly overwrite whoever published
// last; the operation fails closed instead. Callers must treat this as
// "another writer may have won the tag": for the lock tag report contention,
// for the state tag refuse the write and ask for a fresh lock.
var ErrMutableTagMoved = errors.New("mutable OCI tag moved or ambiguous")

// PartialPublicationError reports a PARTIAL publication: the current-state
// manifest (C) was published under StateTag during this Put — verified ours
// at publication time — but the subsequent mutable tag of the same
// publication (currently the history/version tag) failed or conflicted (Err
// carries the underlying failure, possibly wrapping ErrMutableTagMoved).
// There is no rollback. The state write is NOT wholly rejected: the state
// was readable at publication time. However, OCI tags are last-writer-wins
// with no CAS, so by the time the caller observes this error StateTag may
// have been superseded by a later writer — the error guarantees only that
// THIS publication succeeded under the tag, not that the tag still holds it
// or is still current when observed. This is deliberately typed (not a
// sentinel) so callers can inspect which tag succeeded and which failed;
// false failure is preferred over silently overwriting the version tag, so
// no retry is made.
type PartialPublicationError struct {
	// StateTag is the mutable tag that published successfully.
	StateTag string
	// VersionTag is the mutable tag whose publication failed.
	VersionTag string
	// Err is the underlying failure of the version-tag publication.
	Err error
}

func (e *PartialPublicationError) Error() string {
	return fmt.Sprintf("state published under %q, but publishing version tag %q failed: %v", e.StateTag, e.VersionTag, e.Err)
}

func (e *PartialPublicationError) Unwrap() error { return e.Err }

// ─── Internal types ───────────────────────────────────────────────────────────

// lockManifestData holds metadata stored in a lock manifest's annotations.
type lockManifestData struct {
	Generation  int64  `json:"generation"`
	LeaseExpiry int64  `json:"lease_expiry,omitempty"`
	HolderID    string `json:"holder_id,omitempty"`
}

// digestGroup groups version tags by their underlying manifest digest.
type digestGroup struct {
	desc ocispec.Descriptor
	tags []string
}

// workspaceClient is an internal per-workspace OCI client. It is created per
// operation on Client.
type workspaceClient struct {
	client         *Client
	stateID        string
	stateTag       string
	versionTagBase string
	lockTag        string
	unlockedTag    string
}

// newWorkspaceClient creates a workspaceClient for the given stateID (workspace name).
func newWorkspaceClient(c *Client, stateID string) *workspaceClient {
	wsTag := c.workspaceTagFor(stateID)
	return &workspaceClient{
		client:         c,
		stateID:        stateID,
		stateTag:       stateTagPrefix + wsTag,
		versionTagBase: stateVersionTagPrefix + wsTag,
		lockTag:        lockTagPrefix + wsTag,
		unlockedTag:    unlockedTagPrefix + wsTag,
	}
}

// ─── Client ───────────────────────────────────────────────────────────────────

// defaultOperationTimeout bounds public operations when the caller's context
// carries no deadline. BuildHTTPClient no longer sets a fixed HTTP client
// timeout (operations are context-driven), so without this a hung registry
// connection could block indefinitely. 10 minutes is generous for the largest
// allowed state push (256 MiB over a slow link) while still bounding every
// operation. Callers that set their own deadline keep it untouched.
const defaultOperationTimeout = 10 * time.Minute

// opContext returns ctx unchanged (with a no-op cancel) when it already has a
// deadline; otherwise it derives one with defaultOperationTimeout.
func (c *Client) opContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, defaultOperationTimeout)
}

// Get retrieves the state for the given stateID. Returns nil if no state exists.
func (c *Client) Get(ctx context.Context, stateID string) ([]byte, error) {
	ctx, cancel := c.opContext(ctx)
	defer cancel()
	wc := newWorkspaceClient(c, stateID)
	if err := wc.checkWorkspace(ctx); err != nil {
		return nil, err
	}
	return retryWithResult(ctx, func(ctx context.Context) ([]byte, error) {
		return wc.get(ctx)
	})
}

// maxStateSize returns the effective maximum state size, applying the default
// when unset or non-positive. Shared by the read and write paths so both
// enforce the same limit.
func (c *Client) maxStateSize() int64 {
	if c.config.MaxStateSize <= 0 {
		return defaultMaxStateSize
	}
	return c.config.MaxStateSize
}

func (wc *workspaceClient) get(ctx context.Context) ([]byte, error) {
	data, err := wc.readStateReference(ctx, wc.stateTag)
	if err != nil {
		if isNotFound(err) && !isMissingContent(err) {
			// The tag itself is absent: genuinely no state.
			return nil, nil
		}
		if isMissingContent(err) {
			// The tag resolved but its content is missing: existing state
			// is incomplete and must NOT be reported as absent.
			return nil, fmt.Errorf("state manifest at tag %q is incomplete: the tag resolved but its content is missing: %w", wc.stateTag, err)
		}
		return nil, err
	}
	return data, nil
}

// missingContentError marks a NotFound that occurred while fetching content
// whose reference had already resolved (the state/version tag exists but its
// manifest or layer is missing). It preserves the wrapped NotFound for
// errors.Is/isNotFound, while letting Get/GetVersion distinguish "absent
// state" from "incomplete existing state".
type missingContentError struct{ err error }

func (e *missingContentError) Error() string { return e.err.Error() }
func (e *missingContentError) Unwrap() error { return e.err }

func isMissingContent(err error) bool {
	var e *missingContentError
	return errors.As(err, &e)
}

// readStateReference reads one verified state manifest reference and returns
// the logical, uncompressed state bytes. Workspace ownership is enforced by
// fetchManifestWithDesc, and the same MaxStateSize limit applies to current
// and historical reads.
func (wc *workspaceClient) readStateReference(ctx context.Context, reference string) ([]byte, error) {
	fm, _, err := wc.fetchManifestWithDesc(ctx, reference)
	if err != nil {
		return nil, err
	}
	return wc.readStateManifest(ctx, reference, fm)
}

func (wc *workspaceClient) readStateManifest(ctx context.Context, reference string, fm ocispec.Manifest) ([]byte, error) {
	if fm.ArtifactType != "" && fm.ArtifactType != artifactTypeState {
		return nil, fmt.Errorf("unexpected state manifest artifactType %q for %q", fm.ArtifactType, reference)
	}
	if len(fm.Layers) == 0 {
		return nil, nil
	}

	layer := fm.Layers[0]
	rc, err := wc.client.repoClient.inner.Fetch(ctx, layer)
	if err != nil {
		if isNotFound(err) {
			return nil, &missingContentError{fmt.Errorf("layer %s of %q: %w", layer.Digest, reference, err)}
		}
		return nil, err
	}
	defer func() { _ = rc.Close() }()

	var r io.Reader = rc
	switch layer.MediaType {
	case mediaTypeStateLayer:
		// no decompression needed
	case mediaTypeStateLayerGzip:
		gz, err := gzip.NewReader(rc)
		if err != nil {
			return nil, err
		}
		defer func() { _ = gz.Close() }()
		r = gz
	default:
		return nil, fmt.Errorf("unsupported state layer media type %q", layer.MediaType)
	}

	limit := wc.client.maxStateSize()
	lr := io.LimitReader(r, limit+1)
	data, err := io.ReadAll(lr)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("state size exceeds maximum allowed size of %d bytes; use the max_state_size option to increase the limit", limit)
	}
	return data, nil
}

// Put stores the state for the given stateID.
//
// Content-addressed steps (layer push, manifest pack, version read) are
// idempotent/retried; the mutable state/version tag publications go through
// publishMutableTag, so a transient tag response is never followed by a blind
// re-apply: own digest ⇒ response-lost success, foreign digest ⇒ fail closed,
// absent tag ⇒ fail closed (no re-apply: a Resolve(404)→Tag reapply is itself
// a clobber window), ambiguous ⇒ fail closed. The old behavior — wrapping the
// whole put in a retry that re-pushed and re-tagged with a fresh manifest
// digest — is gone: it silently overwrote a rival's newer state because
// ownership is verified only once, before the first Put.
func (c *Client) Put(ctx context.Context, stateID string, data []byte) error {
	ctx, cancel := c.opContext(ctx)
	defer cancel()
	wc := newWorkspaceClient(c, stateID)
	if err := wc.checkWorkspace(ctx); err != nil {
		return err
	}
	return wc.put(ctx, data)
}

func (wc *workspaceClient) put(ctx context.Context, state []byte) error {
	if limit := wc.client.maxStateSize(); int64(len(state)) > limit {
		return fmt.Errorf("state size exceeds maximum allowed size of %d bytes; use the max_state_size option to increase the limit", limit)
	}

	stateToPush := state
	layerMediaType := mediaTypeStateLayer

	if wc.client.config.Compression {
		compressed, err := compressGzip(state)
		if err != nil {
			return fmt.Errorf("compressing state: %w", err)
		}
		stateToPush = compressed
		layerMediaType = mediaTypeStateLayerGzip
	}

	// The version number is read under retry, but only ONCE per publication:
	// recomputing it per retry attempt would mint different version tags.
	var nextVersion int
	if wc.client.config.MaxVersions > 0 {
		v, err := retryWithResult(ctx, func(ctx context.Context) (int, error) {
			current, err := wc.currentStateVersion(ctx)
			if err != nil {
				return 0, fmt.Errorf("failed to determine current state version: %w", err)
			}
			return current + 1, nil
		})
		if err != nil {
			return err
		}
		nextVersion = v
		if nextVersion > maxStateVersion {
			return fmt.Errorf("state version %d exceeds the supported maximum of %d", nextVersion, maxStateVersion)
		}
		// Verify on use (ADR-0002 Amendment 1): an existing destination
		// version tag must belong to this workspace before it is overwritten.
		// Checked before publishing state; not atomic with the Tag below.
		if _, _, err := wc.fetchManifestWithDesc(ctx, wc.versionTagFor(nextVersion)); err != nil && !isNotFound(err) {
			return err
		}
	}

	layerDesc, err := retryWithResult(ctx, func(ctx context.Context) (ocispec.Descriptor, error) {
		return oras.PushBytes(ctx, wc.client.repoClient.inner, layerMediaType, stateToPush)
	})
	if err != nil {
		return err
	}

	// Manifest annotations are computed ONCE per logical Put so every push
	// attempt has identical bytes — and therefore one stable digest. created
	// is fixed to the same single RFC3339 value: PackManifest stamps the
	// wall clock when the annotation is absent, which would make each pack
	// of the same logical publication produce a different digest.
	now := time.Now().UTC().Format(time.RFC3339Nano)
	annotations := map[string]string{
		annotationWorkspace: wc.stateID,
		annotationUpdatedAt: now,
		annotationCreated:   now,
	}
	if nextVersion > 0 {
		annotations[annotationStateVersion] = strconv.Itoa(nextVersion)
	}
	// C: the current manifest, published under the state tag. It carries NO
	// history marker.
	manifestDesc, err := retryWithResult(ctx, func(ctx context.Context) (ocispec.Descriptor, error) {
		return wc.packStateManifestAnnotated(ctx, []ocispec.Descriptor{layerDesc}, annotations)
	})
	if err != nil {
		return err
	}

	// H: the distinct history manifest for the version tag, PREPARED (layer
	// and both manifests pushed, content-addressed) before current is
	// published — a history-pack failure leaves the current tag untouched.
	// The dedicated history marker guarantees C and H digests differ even
	// when layer, bytes and timestamps are identical, and lets retention
	// prove a candidate manifest is history before deleting it.
	var historyDesc ocispec.Descriptor
	if wc.client.config.MaxVersions > 0 {
		historyAnnotations := make(map[string]string, len(annotations)+1)
		maps.Copy(historyAnnotations, annotations)
		historyAnnotations[annotationStateHistory] = strconv.Itoa(nextVersion)
		historyDesc, err = retryWithResult(ctx, func(ctx context.Context) (ocispec.Descriptor, error) {
			return wc.packStateManifestAnnotated(ctx, []ocispec.Descriptor{layerDesc}, historyAnnotations)
		})
		if err != nil {
			return err
		}
	}

	if err := wc.publishMutableTag(ctx, manifestDesc, wc.stateTag); err != nil {
		return err
	}

	if wc.client.config.MaxVersions <= 0 {
		return nil
	}

	newVersionTag := wc.versionTagFor(nextVersion)
	if err := wc.publishMutableTag(ctx, historyDesc, newVersionTag); err != nil {
		// The state tag was published and verified ours at publication time
		// (it may be superseded by a later writer by the time the caller
		// observes this): report the failure as PARTIAL (state was visible,
		// history publication failed) instead of a whole-write rejection.
		// There is no rollback: the failed version tag is never re-applied
		// or deleted (false-failure preference: it may belong to a rival).
		return &PartialPublicationError{
			StateTag:   wc.stateTag,
			VersionTag: newVersionTag,
			Err:        err,
		}
	}

	// Automatic retention is disabled for ghcr.io (ghcrRegistry): no
	// automatic Delete, Packages-API fallback or tag cleanup is issued
	// there. History is still published and readable; the consequence is
	// that MaxVersions does not bound history on ghcr.io (it can grow
	// unbounded). Explicit Delete/DeleteWorkspace are unaffected.
	if wc.client.ghcrRegistry() {
		return nil
	}

	// Async retention: limits concurrent goroutines via semaphore.
	// The detached background context (context.Background) is intentional —
	// these cleanup operations must complete even if the parent context is
	// cancelled, ensuring version retention limits are enforced regardless
	// of the calling operation's lifecycle.
	sem := wc.client.retentionSem
	// Register the goroutine with the WaitGroup before attempting the
	// semaphore acquire: adding after the select leaves a window where
	// WaitForRetention can observe a zero counter and return while a
	// goroutine is about to be spawned.
	wc.client.retentionWg.Add(1)
	select {
	case sem <- struct{}{}:
		go func() {
			defer wc.client.retentionWg.Done()
			defer func() { <-sem }()
			asyncCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			existing, listErr := wc.verifiedVersions(asyncCtx)
			if listErr != nil {
				slog.Warn("async retention: failed to list versions", "workspace", wc.stateID, "tag", wc.stateTag, "error", listErr)
				return
			}
			found := false
			for _, v := range existing {
				if v == nextVersion {
					found = true
					break
				}
			}
			if !found {
				existing = append(existing, nextVersion)
			}
			if err := wc.enforceVersionRetention(asyncCtx, manifestDesc, existing); err != nil {
				slog.Warn("async retention cleanup failed", "workspace", wc.stateID, "tag", wc.stateTag, "error", err)
			}
		}()
	default:
		wc.client.retentionWg.Done()
		slog.Debug("async retention skipped: too many pending cleanups")
	}

	return nil
}

// Delete removes the state for the given stateID. Returns nil if no state exists.
//
// The state tag is resolved ONCE and the resulting descriptor is pinned for
// the bounded delete retries: a transient delete failure followed by a
// concurrent writer publishing under the same tag must never make a retry
// delete the later writer's manifest.
func (c *Client) Delete(ctx context.Context, stateID string) error {
	ctx, cancel := c.opContext(ctx)
	defer cancel()
	wc := newWorkspaceClient(c, stateID)
	if err := wc.checkWorkspace(ctx); err != nil {
		return err
	}
	desc, err := retryWithResult(ctx, func(ctx context.Context) (ocispec.Descriptor, error) {
		return wc.client.repoClient.inner.Resolve(ctx, wc.stateTag)
	})
	if err != nil {
		if isNotFound(err) {
			return nil
		}
		return err
	}
	return retry(ctx, func(ctx context.Context) error {
		return wc.delete(ctx, desc)
	})
}

func (wc *workspaceClient) delete(ctx context.Context, desc ocispec.Descriptor) error {
	// Reuse the GHCR Packages API fallback: registries like ghcr.io return
	// HTTP 405 for manifest deletion. Non-GHCR registries without deletion
	// keep a clear error.
	return wc.deleteDigestWithFallback(ctx, desc, wc.stateTag)
}

// DeleteWorkspace removes state, locks, unlocked markers, and retained state
// versions for one logical workspace. It is the operation backend adapters
// should use when implementing workspace deletion.
func (c *Client) DeleteWorkspace(ctx context.Context, stateID string) error {
	ctx, cancel := c.opContext(ctx)
	defer cancel()

	wc := newWorkspaceClient(c, stateID)
	if err := wc.checkWorkspace(ctx); err != nil {
		return err
	}

	var tags []string
	err := retry(ctx, func(ctx context.Context) error {
		tags = tags[:0]
		return c.repoClient.inner.Tags(ctx, "", func(page []string) error {
			tags = append(tags, page...)
			return nil
		})
	})
	if isNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}

	for _, tag := range tags {
		owned := tag == wc.stateTag || tag == wc.lockTag || tag == wc.unlockedTag ||
			strings.HasPrefix(tag, wc.versionTagBase+stateVersionTagSeparator)

		// Legacy implementations before the stver-* layout stored history as
		// state-<workspace>-v<N>. Those tags are ambiguous with valid
		// workspace names, so only delete them after manifest ownership
		// verification.
		if !owned && c.config.WorkspaceLayout == WorkspaceLayoutLegacy &&
			strings.HasPrefix(tag, wc.stateTag+stateVersionTagSeparator) {
			manifest, fetchErr := fetchRawManifest(ctx, c.repoClient, tag)
			if isNotFound(fetchErr) {
				continue
			}
			if fetchErr != nil {
				return fetchErr
			}
			owned = manifest.Annotations[annotationWorkspace] == stateID
		}
		if !owned {
			continue
		}

		desc, resolveErr := c.repoClient.inner.Resolve(ctx, tag)
		if isNotFound(resolveErr) {
			continue
		}
		if resolveErr != nil {
			return resolveErr
		}
		if err := wc.deleteDigestWithFallback(ctx, desc, tag); err != nil {
			return err
		}
	}
	return nil
}

// Lock acquires a lock for the given stateID. Returns the lock ID on success.
func (c *Client) Lock(ctx context.Context, stateID string, info LockInfo) (string, error) {
	ctx, cancel := c.opContext(ctx)
	defer cancel()
	wc := newWorkspaceClient(c, stateID)
	if err := wc.checkWorkspace(ctx); err != nil {
		return "", err
	}
	return wc.lock(ctx, &info)
}

// lock acquires a distributed lock for the workspace using generation-based
// optimistic concurrency control.
//
// Pre:  ctx is non-nil; info is non-nil with a non-empty ID field.
// Post: on success, returns info.ID and the lock tag references a manifest with
//
//	Generation == previous_generation+1 and HolderID == info.ID.
//	On failure with an existing lock, returns *LockError with Info populated.
//
// Bounding function (termination): the function does not loop; it performs at
// most one stale-lock clear followed by one tag+verify attempt. All retries are
// delegated to withRetryNoResult with a finite MaxAttempts bound.
func (wc *workspaceClient) lock(ctx context.Context, info *LockInfo) (string, error) {
	if info == nil {
		return "", fmt.Errorf("lock info is required")
	}

	var currentGen *lockManifestData
	lockM, lockDesc, err := wc.fetchManifestWithDesc(ctx, wc.lockTag)
	if err != nil && !isNotFound(err) {
		return "", fmt.Errorf("failed to read current lock state: %w", err)
	}
	if err == nil {
		currentGen, _ = parseLockManifestData(&lockM)

		existing, parseErr := parseLockInfo(&lockM, wc.stateTag)
		if parseErr != nil {
			return "", fmt.Errorf("failed to parse current lock info: %w", parseErr)
		}
		if existing != nil && existing.ID != "" {
			if !wc.isLockStale(currentGen) {
				return "", &LockError{Info: existing, Err: errStateLocked}
			}
			if err := wc.clearLock(ctx, lockDesc); err != nil {
				return "", err
			}
		}
	}

	newGeneration := int64(1)
	if currentGen != nil && currentGen.Generation > 0 {
		newGeneration = currentGen.Generation + 1
	}

	leaseExpiry := int64(0)
	if wc.client.config.LockTTL > 0 {
		leaseExpiry = time.Now().UTC().Add(wc.client.config.LockTTL).UnixNano()
	}

	info.Path = wc.stateTag
	infoBytes, err := json.Marshal(info)
	if err != nil {
		return "", err
	}

	manifestDesc, err := wc.packLockManifest(ctx, string(infoBytes), newGeneration, leaseExpiry, info.ID)
	if err != nil {
		return "", err
	}

	if err := wc.publishMutableTag(ctx, manifestDesc, wc.lockTag); err != nil {
		if errors.Is(err, ErrMutableTagMoved) {
			// The lock tag points at (or may point at) another holder's
			// manifest: report contention instead of clobbering the rival.
			// The returned *LockError must let errors.Is see BOTH
			// classifications — errStateLocked (contention semantics,
			// unchanged for callers) and ErrMutableTagMoved (moved/ambiguous
			// publication) — while Info keeps the fetched holder.
			if held, _, fetchErr := wc.fetchManifestWithDesc(ctx, wc.lockTag); fetchErr == nil {
				existing, _ := parseLockInfo(&held, wc.stateTag)
				return "", &LockError{
					Info: existing,
					Err:  fmt.Errorf("%w: lock tag %q moved during acquisition: %w", errStateLocked, wc.lockTag, err),
				}
			}
		}
		return "", err
	}

	// Post-write verification: one fetch, both parses — ensure we hold the lock.
	// The cleanup is a no-op whenever the tag has moved, which is the only way
	// parsing can fail: content addressing guarantees our own digest fetches
	// back our own bytes. A cleanup FAILURE is never swallowed: it is
	// surfaced together with the original verification failure so the caller
	// knows the workspace may remain locked and can inspect/recover.
	cleanupOurTag := func() error {
		// Detached, bounded context: the caller's ctx may already be
		// cancelled at every cleanup call site (the stability-window branch
		// literally runs on a cancelled context), and cleanup on a cancelled
		// context is a no-op that strands the lock until TTL expiry. The
		// digest check below still guards against clearing a rival's tag.
		cleanupCtx, cancel := context.WithTimeout(context.Background(), lockCleanupTimeout)
		defer cancel()
		d, err := wc.client.repoClient.inner.Resolve(cleanupCtx, wc.lockTag)
		if err != nil {
			if isNotFound(err) {
				// Confirmed absent: the lock is already gone — idempotent
				// success, nothing stranded to clean.
				return nil
			}
			return fmt.Errorf("lock-tag cleanup after failed verification: could not confirm the state of lock tag %q: %w", wc.lockTag, err)
		}
		if d.Digest != manifestDesc.Digest {
			// The tag moved: nothing of ours to clean, and nothing stranded.
			return nil
		}
		if err := wc.client.repoClient.inner.Delete(cleanupCtx, d); err != nil {
			if isNotFound(err) {
				// Confirmed absent: a concurrent cleanup already deleted our
				// lock manifest — idempotent success.
				return nil
			}
			if isDeleteUnsupported(err) {
				// Registries like GHCR (HTTP 405) can't delete manifests;
				// retag to "unlocked-" so the workspace isn't locked until
				// TTL expiry. The tag still points at our digest (checked
				// above), so that is the expected digest for the preflight.
				if retagErr := wc.retagToUnlocked(cleanupCtx, manifestDesc.Digest.String()); retagErr != nil {
					return fmt.Errorf("lock-tag cleanup after failed verification: manifest deletion is unsupported and retagging the lock tag to the unlocked marker failed: %w", retagErr)
				}
				return nil
			}
			return fmt.Errorf("lock-tag cleanup after failed verification: deleting our lock manifest %s failed: %w", manifestDesc.Digest, err)
		}
		return nil
	}

	// failVerification surfaces the verification failure, attaching any
	// cleanup failure so a stranded lock is visible instead of silent.
	failVerification := func(err error) error {
		if cleanupErr := cleanupOurTag(); cleanupErr != nil {
			return fmt.Errorf("%w; additionally, %w (the workspace may remain locked under tag %q until it is cleared via Unlock or TTL expiry)", err, cleanupErr, wc.lockTag)
		}
		return err
	}

	held, _, err := wc.fetchManifestWithDesc(ctx, wc.lockTag)
	if err != nil {
		return "", failVerification(fmt.Errorf("failed to verify lock acquisition: %w", err))
	}
	verified, err := parseLockManifestData(&held)
	if err != nil {
		return "", failVerification(fmt.Errorf("failed to verify lock acquisition: %w", err))
	}
	if verified.Generation != newGeneration || verified.HolderID != info.ID {
		existing, _ := parseLockInfo(&held, wc.stateTag)
		return "", &LockError{Info: existing, Err: fmt.Errorf("state is locked (lost race)")}
	}

	// Stability re-read after a short, context-cancellable wait. OCI tags are
	// last-writer-wins — registries offer no CAS/If-Match — so a rival that
	// tags the lock after our first verification would otherwise go undetected.
	// This narrows the race window but does NOT eliminate it: a rival can
	// still win immediately after this second read. Generation + read-back is
	// a mitigation, not strong mutual exclusion.
	timer := time.NewTimer(lockStabilityDelay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return "", failVerification(ctx.Err())
	case <-timer.C:
	}

	// If digest/generation/holder are no longer ours, a rival won: report
	// contention and do NOT clear a tag that points at another holder.
	stable, stableDesc, err := wc.fetchManifestWithDesc(ctx, wc.lockTag)
	if err != nil {
		return "", failVerification(fmt.Errorf("failed to confirm lock stability: %w", err))
	}
	if stableDesc.Digest != manifestDesc.Digest {
		existing, _ := parseLockInfo(&stable, wc.stateTag)
		return "", &LockError{Info: existing, Err: fmt.Errorf("state is locked (lost race)")}
	}
	stableData, err := parseLockManifestData(&stable)
	if err != nil || stableData.Generation != newGeneration || stableData.HolderID != info.ID {
		existing, _ := parseLockInfo(&stable, wc.stateTag)
		return "", &LockError{Info: existing, Err: fmt.Errorf("state is locked (lost race)")}
	}

	return info.ID, nil
}

// Unlock releases the lock for the given stateID. Returns nil if no lock exists.
func (c *Client) Unlock(ctx context.Context, stateID, lockID string) error {
	ctx, cancel := c.opContext(ctx)
	defer cancel()
	wc := newWorkspaceClient(c, stateID)
	if err := wc.checkWorkspace(ctx); err != nil {
		return err
	}
	return wc.unlock(ctx, lockID)
}

// VerifyLock reports whether the lock for stateID is still held by lockID: it
// reads the lock tag, compares the lock holder ID, and — after the holder
// matches — rejects a STORED positive lease_expiry already past the
// verifier's wall clock (the stored expiry governs, NOT the verifier's
// configured LockTTL; LeaseExpiry = 0 remains non-expiring). It returns an
// error when the lock is gone, held by a different holder, past its stored
// lease, or the check itself fails.
// There is no CAS on OCI tags and registries do not enforce leases, so a
// caller cannot close the race between a successful VerifyLock and a
// subsequent Put — this is a best-effort ownership check (the W1 window is
// unchanged; clock skew between holders is unmodeled).
func (c *Client) VerifyLock(ctx context.Context, stateID, lockID string) error {
	ctx, cancel := c.opContext(ctx)
	defer cancel()
	wc := newWorkspaceClient(c, stateID)
	if err := wc.checkWorkspace(ctx); err != nil {
		return err
	}
	return wc.verifyLock(ctx, lockID)
}

func (wc *workspaceClient) verifyLock(ctx context.Context, lockID string) error {
	fm, _, err := wc.fetchManifestWithDesc(ctx, wc.lockTag)
	if err != nil {
		if isNotFound(err) {
			return fmt.Errorf("lock for %q no longer exists", wc.lockTag)
		}
		return fmt.Errorf("failed to verify lock: %w", err)
	}
	existing, err := parseLockInfo(&fm, wc.stateTag)
	if err != nil {
		return fmt.Errorf("failed to verify lock: %w", err)
	}
	if existing == nil || existing.ID == "" {
		// Includes the "unlocked-" marker case: the tag points at a
		// manifest with no holder.
		return fmt.Errorf("lock for %q no longer exists", wc.lockTag)
	}
	if existing.ID != lockID {
		return fmt.Errorf("lock for %q is held by %q, not %q", wc.lockTag, existing.ID, lockID)
	}
	// Stored-lease enforcement: the manifest's own lease_expiry decides, not
	// the verifier's configured LockTTL (the holder and a rival may have
	// different TTL configs; the stored value is what was agreed at
	// acquisition). LeaseExpiry = 0 remains non-expiring. Takeover staleness
	// (isLockStale) is a separate, config-TTL-based check on the next Lock.
	lease, err := parseLockManifestData(&fm)
	if err != nil {
		return fmt.Errorf("failed to verify lock: %w", err)
	}
	if isStoredLeaseExpired(lease) {
		return fmt.Errorf("lock for %q expired: stored lease_expiry is in the past (%s; checked with the local clock; clock skew between holders is unmodeled)",
			wc.lockTag, time.Unix(0, lease.LeaseExpiry).UTC().Format(time.RFC3339Nano))
	}
	return nil
}

// isStoredLeaseExpired reports whether a stored positive lease_expiry is
// already past according to the local wall clock. The verifier's configured
// LockTTL is deliberately ignored here: the manifest's stored expiry is the
// shared fact both sides agree on. LeaseExpiry <= 0 means non-expiring.
func isStoredLeaseExpired(data *lockManifestData) bool {
	if data == nil || data.LeaseExpiry <= 0 {
		return false
	}
	return time.Now().UTC().UnixNano() > data.LeaseExpiry
}

// mutableTagObservationLimit bounds how often the tag is OBSERVED after a
// transient mutable-tag publication failure before failing closed. The tag
// is never re-applied after a transient failure: a Resolve→Tag reapply is
// itself a clobber window (rivals can publish between the 404 and a re-Tag).
const mutableTagObservationLimit = 3

// mutableTagObservationDelay is the backoff base between observation
// attempts; the worst-case bounded wait is 1.5 seconds (0.5s + 1.0s before
// the final observation). The publication is never re-applied on an unknown
// tag state, so no longer wait is needed.
const mutableTagObservationDelay = 500 * time.Millisecond

// tagPrePushNotFoundRetryLimit bounds the total Tag attempts made by ONE
// call of the helper below when the pinned ORAS manifestStore.Tag pre-push
// Fetch reports the manifest absent.
const tagPrePushNotFoundRetryLimit = 3

// tagPrePushNotFoundRetryDelay is the fixed, context-cancellable backoff
// between attempts (worst-case added wait: 200ms before the third attempt).
const tagPrePushNotFoundRetryDelay = 100 * time.Millisecond

// tagPrePushNotFoundRetry issues a Tag call on repo, retrying ONLY the
// sentinel errdef.ErrNotFound — which the pinned ORAS manifestStore.Tag
// produces when its PRE-PUSH Fetch (a plain GET of the manifest digest)
// reports the manifest absent (registry/remote/repository.go: Tag → Fetch;
// Fetch maps GET 404 to fmt.Errorf("%s: %w", digest, errdef.ErrNotFound)).
//
// Why retry at all: issue #51 observed an intermittent digest-not-found
// error during the first GHCR lock acquisition (unchanged source, successful
// rerun). It did NOT prove read-after-write lag or that a later PUT would
// succeed; delayed availability of a freshly pushed manifest is the
// hypothesis this retry accommodates. If the hypothesis is right, a manifest
// pushed moments earlier can 404 on that digest GET even though the tag PUT
// itself would succeed; failing hard turns that into intermittent
// publication errors. The helper handles exactly the pinned ORAS pre-PUT
// GET-404 path and nothing else.
//
// Scope of the 3-attempt cap: it bounds ONE helper call. Callers must never
// wrap this helper in an outer blind retry: a transient PUT failure keeps
// its fail-closed observation handling (publishMutableTag), so no retry
// loop replays a mutable-tag PUT whose response was lost. A pure
// ErrNotFound exhaustion is NOT transient and is surfaced as-is.
//
// KNOWN LIMITATION — accepted, NOT concurrency-safe: OCI tags are
// unconditional last-writer-wins (no CAS). Between a 404 and the retried
// PUT, a rival can publish its own intent under the tag, and this retry
// overwrites it; that stale-intent window is deliberately accepted to
// unblock eventually-consistent registries. This must never be described as
// compare-and-swap or as making tag publication concurrency-safe.
// Everything else stays fail-closed: any other error is returned
// immediately — including a tag PUT that failed with an HTTP 404
// ErrorResponse, which is a remote errcode.ErrorResponse, NOT
// errdef.ErrNotFound — and transient PUT failures keep their existing
// per-callsite handling (observation in publishMutableTag).
func tagPrePushNotFoundRetry(ctx context.Context, repo orasRepository, desc ocispec.Descriptor, reference string) error {
	for attempt := 1; ; attempt++ {
		err := repo.Tag(ctx, desc, reference)
		if err == nil || !errors.Is(err, errdef.ErrNotFound) || attempt == tagPrePushNotFoundRetryLimit {
			return err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: tag %q retry aborted before attempt %d after pre-push not-found: %w",
				ctx.Err(), reference, attempt+1, err)
		case <-time.After(tagPrePushNotFoundRetryDelay):
		}
	}
}

// publishMutableTag publishes desc under tag via a plain Tag PUT, with
// observation instead of blind retry after a transient (response-lost or
// failed) result.
//
// This is NOT CAS and does not claim to be: on the normal/ambiguous PUT
// path (success, or transient failure handled by observation) the existing
// last-writer-wins and fail-closed handling is unchanged, and what this
// prevents is the BLIND reapplication of a publication whose response was
// lost: a retried tag PUT cannot silently overwrite a rival that published
// in the gap, and an ambiguous result fails closed instead of being
// re-applied. The one deliberate exception below (pre-push digest GET 404)
// DOES add a stale-intent window; see tagPrePushNotFoundRetry.
//
// The one deliberate exception to "never re-apply" is the pre-push digest
// GET 404 (tagPrePushNotFoundRetry): it is a pre-publication failure — no
// tag PUT was issued yet — so retrying it cannot replay an ambiguous PUT.
// It still adds a stale-intent window (a rival can publish between
// attempts) and is not CAS; see tagPrePushNotFoundRetry.
//
//   - initial Tag succeeds          → done.
//   - Tag fails with pre-push 404   → bounded retry (stale-intent window
//     accepted; see tagPrePushNotFoundRetry).
//   - Tag fails non-transiently     → surface the error (no observation).
//   - Tag fails transiently         → observeMutableTag (below).
func (wc *workspaceClient) publishMutableTag(ctx context.Context, desc ocispec.Descriptor, tag string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	tagErr := tagPrePushNotFoundRetry(ctx, wc.client.repoClient.inner, desc, tag)
	if tagErr == nil {
		return nil
	}
	if !isTransientError(tagErr) {
		return tagErr
	}
	return wc.observeMutableTag(ctx, desc, tag, tagErr)
}

// observeMutableTag reacts to a transient publication failure by observing
// the tag before ever re-tagging:
//
//   - resolves to desc.Digest → the response was lost, the publication landed:
//     success, no second Tag is issued.
//   - resolves to a foreign digest → ErrMutableTagMoved, no re-tag.
//   - not found → the lost response did not publish, but the tag is NEVER
//     re-applied: Resolve→Tag is itself a clobber window (a rival can publish
//     between the 404 and a re-Tag), so the publication fails closed.
//   - ambiguous (transient resolve error, or the bound exhausted) → fail
//     closed with ErrMutableTagMoved: false failure is preferable to
//     silently overwriting a possibly newer publication.
//
// All waits are bounded and context-cancellable; cancellation identity
// (context.Canceled / context.DeadlineExceeded) is preserved on fail-closed
// errors.
func (wc *workspaceClient) observeMutableTag(ctx context.Context, desc ocispec.Descriptor, tag string, initialErr error) error {
	failClosed := func(detail string, cause error) error {
		err := fmt.Errorf("%w: state of tag %q unknown after transient publish error (%w); %s; refusing to re-apply, a re-tag could overwrite a newer publication",
			ErrMutableTagMoved, tag, initialErr, detail)
		if cause != nil {
			return fmt.Errorf("%w: %w", err, cause)
		}
		return err
	}

	for attempt := 1; attempt <= mutableTagObservationLimit; attempt++ {
		if err := ctx.Err(); err != nil {
			return failClosed("observation cancelled", err)
		}
		current, resolveErr := wc.client.repoClient.inner.Resolve(ctx, tag)
		switch {
		case resolveErr == nil:
			if current.Digest == desc.Digest {
				// The initial response was lost, but the publication landed.
				return nil
			}
			return fmt.Errorf("%w: tag %q points at %s, not this attempt's digest %s; refusing to overwrite",
				ErrMutableTagMoved, tag, current.Digest, desc.Digest)
		case isNotFound(resolveErr):
			// KNOWN absent (not unknown state): the lost response did not
			// publish. Fail closed — a Resolve(404)→Tag re-apply is itself a
			// clobber window (a rival can publish between the 404 and a
			// re-Tag).
			return fmt.Errorf("%w: tag %q confirmed absent after transient publish error (%w), observation %d of %d; refusing to re-apply, a Resolve(404)→Tag reapply is a clobber window",
				ErrMutableTagMoved, tag, initialErr, attempt, mutableTagObservationLimit)
		case isTransientError(resolveErr):
			// Observation itself failed transiently: bounded re-observation,
			// never a re-Tag on an unknown tag state.
		default:
			// Non-transient observation error: ambiguous, fail closed.
			return failClosed(fmt.Sprintf("unresolvable after %d attempt(s)", attempt), resolveErr)
		}
		if attempt < mutableTagObservationLimit {
			select {
			case <-ctx.Done():
				return failClosed("observation cancelled", ctx.Err())
			case <-time.After(time.Duration(attempt) * mutableTagObservationDelay):
			}
		}
	}
	return failClosed(fmt.Sprintf("%d observations did not disambiguate", mutableTagObservationLimit), initialErr)
}

func (wc *workspaceClient) unlock(ctx context.Context, id string) error {
	fm, desc, err := wc.fetchManifestWithDesc(ctx, wc.lockTag)
	if err != nil {
		if isNotFound(err) {
			return nil
		}
		return err
	}

	existing, err := parseLockInfo(&fm, wc.stateTag)
	if err != nil {
		return err
	}
	if existing == nil || existing.ID == "" {
		return nil
	}
	if id != "" && existing.ID != id {
		return fmt.Errorf("lock ID mismatch: held by %q", existing.ID)
	}

	err = retry(ctx, func(ctx context.Context) error {
		return wc.client.repoClient.inner.Delete(ctx, desc)
	})
	if err == nil {
		return nil
	}
	if !isDeleteUnsupported(err) {
		return err
	}

	return wc.retagToUnlocked(ctx, desc.Digest.String())
}

// List returns all workspace names stored in the OCI repository.
func (c *Client) List(ctx context.Context) ([]string, error) {
	ctx, cancel := c.opContext(ctx)
	defer cancel()
	if c.config.WorkspaceLayout == WorkspaceLayoutLegacy {
		return retryWithResult(ctx, func(ctx context.Context) ([]string, error) {
			return listLegacyWorkspaces(ctx, c.repoClient)
		})
	}
	return retryWithResult(ctx, func(ctx context.Context) ([]string, error) {
		return listWorkspacesFromTags(ctx, c.repoClient)
	})
}

// Close waits for all in-flight background retention work to finish.
// It is safe to call multiple times. Consumers should prefer Close over
// depending on retention implementation details.
func (c *Client) Close() error {
	c.retentionWg.Wait()
	return nil
}

// WaitForRetention blocks until all in-flight async retention goroutines complete.
// Deprecated: use Close for lifecycle management. Kept for compatibility and tests.
func (c *Client) WaitForRetention() {
	c.retentionWg.Wait()
}

// ─── workspaceClient helpers ──────────────────────────────────────────────────

// packStateManifest packs a state manifest with a fresh updated_at stamp.
// Not used by publications (put fixes created/updated_at per logical Put for
// reproducibility); kept for tests that craft manifests by hand.
func (wc *workspaceClient) packStateManifest(ctx context.Context, layers []ocispec.Descriptor, stateVersion int) (ocispec.Descriptor, error) {
	annotations := map[string]string{
		annotationWorkspace: wc.stateID,
		annotationUpdatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}
	if stateVersion > 0 {
		annotations[annotationStateVersion] = strconv.Itoa(stateVersion)
	}
	return wc.packStateManifestAnnotated(ctx, layers, annotations)
}

// packStateManifestAnnotated packs the state manifest with caller-provided
// annotations, so a publication can be re-pushed with identical bytes (one
// stable digest) instead of minting a fresh updated_at per attempt.
func (wc *workspaceClient) packStateManifestAnnotated(ctx context.Context, layers []ocispec.Descriptor, annotations map[string]string) (ocispec.Descriptor, error) {
	return oras.PackManifest(ctx, wc.client.repoClient.inner, oras.PackManifestVersion1_1, artifactTypeState, oras.PackManifestOptions{
		Layers:              layers,
		ManifestAnnotations: annotations,
	})
}

// packLockManifest builds a lock manifest. holderID doubles as the lock ID
// annotation — the two are always the same value.
func (wc *workspaceClient) packLockManifest(ctx context.Context, infoJSON string, generation, leaseExpiry int64, holderID string) (ocispec.Descriptor, error) {
	lockData := lockManifestData{
		Generation:  generation,
		LeaseExpiry: leaseExpiry,
		HolderID:    holderID,
	}
	lockDataJSON, err := json.Marshal(lockData)
	if err != nil {
		return ocispec.Descriptor{}, fmt.Errorf("failed to marshal lock metadata: %w", err)
	}

	return oras.PackManifest(ctx, wc.client.repoClient.inner, oras.PackManifestVersion1_1, artifactTypeLock, oras.PackManifestOptions{
		ManifestAnnotations: map[string]string{
			annotationWorkspace: wc.stateID,
			annotationLockID:    holderID,
			annotationLockInfo:  infoJSON,
			annotationLockGen:   string(lockDataJSON),
		},
	})
}

func (wc *workspaceClient) versionTagFor(version int) string {
	return fmt.Sprintf("%s%s%d", wc.versionTagBase, stateVersionTagSeparator, version)
}

// currentStateVersion returns the highest known version number for the
// workspace: from the state manifest annotation, falling back to the version
// tags. A missing state manifest yields (0, nil); any other failure is
// propagated so Put fails instead of silently resetting versioning to 1.
func (wc *workspaceClient) currentStateVersion(ctx context.Context) (int, error) {
	fm, _, err := wc.fetchManifestWithDesc(ctx, wc.stateTag)
	if err != nil {
		if isNotFound(err) {
			return 0, nil
		}
		return 0, err
	}
	if v, ok := fm.Annotations[annotationStateVersion]; ok {
		n, parseErr := strconv.Atoi(v)
		if parseErr == nil && n > 0 && n <= maxStateVersion {
			return n, nil
		}
	}
	existing, listErr := wc.verifiedVersions(ctx)
	if listErr != nil {
		return 0, listErr
	}
	max := 0
	for _, v := range existing {
		if v > max {
			max = v
		}
	}
	return max, nil
}

// verifiedVersions is listExistingVersions minus tags whose manifest belongs
// to another workspace (ADR-0002 Amendment 1); 404s are skipped.
func (wc *workspaceClient) verifiedVersions(ctx context.Context) ([]int, error) {
	versions, err := wc.listExistingVersions(ctx)
	if err != nil {
		return nil, err
	}
	verified := versions[:0]
	for _, v := range versions {
		_, _, err := wc.fetchManifestWithDesc(ctx, wc.versionTagFor(v))
		if isNotFound(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		verified = append(verified, v)
	}
	return verified, nil
}

func (wc *workspaceClient) listExistingVersions(ctx context.Context) ([]int, error) {
	var tags []string
	if err := wc.client.repoClient.inner.Tags(ctx, "", func(page []string) error {
		tags = append(tags, page...)
		return nil
	}); err != nil {
		return nil, err
	}

	var existing []int
	for _, t := range tags {
		base, v, ok := splitStateVersionTag(t)
		if !ok || base != wc.versionTagBase {
			continue
		}
		existing = append(existing, v)
	}
	return existing, nil
}

// enforceVersionRetention prunes old state versions, keeping at most
// wc.client.config.MaxVersions versions.
//
// Pre:  wc.client.config.MaxVersions > 0; versions contains the list of known
//
//	version numbers for this workspace.
//
// Post: at most wc.client.config.MaxVersions versions remain, counting only
//
//	exact verified history manifests. The current manifest (current.Digest)
//	is never deleted.
//
// Deletion safety: a digest group is deleted only when (a) every alias tag
// in the group is being retired — a group shared with a retained tag is
// preserved, because OCI delete-by-digest removes ALL aliases to the exact
// manifest (the spec does not promise shared blob GC semantics) — and (b)
// the manifest fetched BY THE GROUPED DESCRIPTOR is an exact verified
// history manifest whose marker/version agrees with EVERY version tag in
// the group (workspace ownership is enforced by the fetch; any logically
// incompatible alias preserves the whole group). Unmarked or unrecognized
// manifests — legacy writers, foreign formats — are preserved. The grouped
// descriptor is what gets deleted and no mutable tag is ever resolved
// again after grouping: a re-resolve could return a different manifest and
// delete the wrong one. No retagging exists in this protocol: current and
// history manifests have distinct digests, so retiring history never
// detaches the current tag.
// Bounding function: len(groups) - index of processed group.
func (wc *workspaceClient) enforceVersionRetention(ctx context.Context, current ocispec.Descriptor, versions []int) error {
	if wc.client.config.MaxVersions <= 0 || len(versions) <= wc.client.config.MaxVersions {
		return nil
	}

	slices.Sort(versions)
	toDeleteCount := len(versions) - wc.client.config.MaxVersions
	deleteVersions := versions[:toDeleteCount]
	keepVersions := versions[toDeleteCount:]

	deleteTagSet := make(map[string]struct{}, len(deleteVersions))
	keepTagSet := make(map[string]struct{}, len(keepVersions))
	for _, v := range deleteVersions {
		deleteTagSet[wc.versionTagFor(v)] = struct{}{}
	}
	for _, v := range keepVersions {
		keepTagSet[wc.versionTagFor(v)] = struct{}{}
	}

	groups, err := wc.groupVersionsByDigest(ctx, versions, current.Digest.String())
	if err != nil {
		return err
	}

	for _, g := range groups {
		tagsToDelete, tagsToKeep := classifyTags(g.tags, deleteTagSet, keepTagSet)
		if len(tagsToDelete) == 0 {
			continue
		}
		if len(tagsToKeep) > 0 {
			// Ambiguous alias group: the digest is shared with a retained
			// tag; deleting it would remove the retained alias too.
			continue
		}

		// Final validation fetches the EXACT grouped descriptor — it never
		// re-resolves a mutable version tag, which may have moved since
		// grouping and must never become a deletion target.
		fm, err := wc.fetchManifestByDesc(ctx, g.desc)
		if isNotFound(err) {
			// A concurrent retention may have deleted it already.
			continue
		}
		if err != nil {
			return err
		}
		if !isVerifiedHistoryGroup(&fm, g.tags) {
			continue
		}

		if err := wc.deleteDigestWithFallback(ctx, g.desc, tagsToDelete[0]); err != nil {
			return err
		}
	}

	return nil
}

// isVerifiedHistoryGroup reports whether the validated manifest m is an
// exact, recognized history manifest for EVERY version tag aliased to its
// digest group: each tag's version must be recognized and agree with m's
// marker (and version annotation). Any logically incompatible alias
// preserves the whole digest group — deletion by digest removes all aliases.
func isVerifiedHistoryGroup(m *ocispec.Manifest, tags []string) bool {
	for _, tag := range tags {
		_, version, ok := splitStateVersionTag(tag)
		if !ok || !isVerifiedHistoryManifest(m, version) {
			return false
		}
	}
	return true
}

// isVerifiedHistoryManifest reports whether m is an exact, recognized history
// manifest for version: exactly the state artifact type (untyped manifests
// are NOT eligible — new H manifests always carry the type; legacy readers
// remain lenient), a dedicated history marker whose value equals the
// version, and (when present) a version annotation that agrees with it.
// Unmarked or unrecognized manifests — legacy writers, foreign formats,
// ambiguous markers — are never eligible for automatic deletion.
func isVerifiedHistoryManifest(m *ocispec.Manifest, version int) bool {
	if m.ArtifactType != artifactTypeState {
		return false
	}
	raw, ok := m.Annotations[annotationStateHistory]
	if !ok {
		return false
	}
	marked, err := strconv.Atoi(raw)
	if err != nil || marked != version {
		return false
	}
	if rawVersion, ok := m.Annotations[annotationStateVersion]; ok {
		annotated, err := strconv.Atoi(rawVersion)
		if err != nil || annotated != version {
			return false
		}
	}
	return true
}

func (wc *workspaceClient) groupVersionsByDigest(ctx context.Context, versions []int, currentDigest string) (map[string]*digestGroup, error) {
	var mu sync.Mutex
	groups := make(map[string]*digestGroup)

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(10)

	for _, v := range versions {
		tag := wc.versionTagFor(v)
		g.Go(func() error {
			desc, err := wc.client.repoClient.inner.Resolve(ctx, tag)
			if err != nil {
				if isNotFound(err) {
					// A concurrent retention (or unlock) may have deleted
					// this tag between the Tags listing and this Resolve:
					// nothing left to group, skip it. Any other failure —
					// auth, network, 5xx — must propagate with tag context
					// or retention would silently stay incomplete.
					return nil
				}
				// Propagate with tag context: a failed Resolve silently
				// skipped would leave that version's tags undeletable and
				// retention perpetually incomplete. The current digest skip
				// below is unaffected.
				return fmt.Errorf("resolving %q: %w", tag, err)
			}
			if desc.Digest.String() == currentDigest {
				return nil
			}
			key := desc.Digest.String()
			mu.Lock()
			if grp, ok := groups[key]; ok {
				grp.tags = append(grp.tags, tag)
			} else {
				groups[key] = &digestGroup{desc: desc, tags: []string{tag}}
			}
			mu.Unlock()
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}
	return groups, nil
}

func classifyTags(tags []string, deleteSet, keepSet map[string]struct{}) (toDelete, toKeep []string) {
	for _, tag := range tags {
		if _, ok := deleteSet[tag]; ok {
			toDelete = append(toDelete, tag)
		} else if _, ok := keepSet[tag]; ok {
			toKeep = append(toKeep, tag)
		}
	}
	return
}

// ghcrRegistry reports whether the configured registry is ghcr.io. Automatic
// retention is disabled there: manifest deletion returns HTTP 405 and the
// Packages-API fallback is reserved for explicit Delete/DeleteWorkspace.
func (c *Client) ghcrRegistry() bool {
	host, _, _, err := parseGHCRRepository(c.repoClient.repository)
	return err == nil && host == ghcrHost
}

func (wc *workspaceClient) deleteDigestWithFallback(ctx context.Context, desc ocispec.Descriptor, fallbackTag string) error {
	err := wc.client.repoClient.inner.Delete(ctx, desc)
	if err == nil || isNotFound(err) {
		return nil
	}
	if !isDeleteUnsupported(err) {
		return err
	}

	ghErr := tryDeleteGHCRTag(ctx, wc.client.repoClient, fallbackTag, desc.Digest.String())
	if errors.Is(ghErr, errNotGHCR) {
		return fmt.Errorf("registry does not support manifest deletion (HTTP 405) and no alternative deletion method is available for %q", fallbackTag)
	}
	if ghErr != nil {
		return fmt.Errorf("registry does not support manifest deletion and GHCR API fallback failed for %q: %w", fallbackTag, ghErr)
	}
	return nil
}

func (wc *workspaceClient) isLockStale(data *lockManifestData) bool {
	if wc.client.config.LockTTL <= 0 {
		return false
	}
	if data == nil || data.LeaseExpiry <= 0 {
		return false
	}
	return time.Now().UTC().UnixNano() > data.LeaseExpiry
}

func (wc *workspaceClient) clearLock(ctx context.Context, desc ocispec.Descriptor) error {
	err := retry(ctx, func(ctx context.Context) error {
		return wc.client.repoClient.inner.Delete(ctx, desc)
	})
	if err == nil || isNotFound(err) {
		return nil
	}
	if !isDeleteUnsupported(err) {
		return err
	}
	return wc.retagToUnlocked(ctx, desc.Digest.String())
}

// retagToUnlocked points the lock tag at the "unlocked-" marker manifest, for
// registries that cannot delete manifests. expectedDigest is the lock digest
// the caller just read; immediately before the retag the tag is re-resolved
// and the operation aborts if it moved. This narrows the check→Tag race but
// does NOT eliminate the residual last-writer-wins window (OCI tags have no
// CAS); the read-back in verifyUnlockedMarker remains the race detector.
func (wc *workspaceClient) retagToUnlocked(ctx context.Context, expectedDigest string) error {
	desc, err := retryWithResult(ctx, func(ctx context.Context) (ocispec.Descriptor, error) {
		return wc.client.repoClient.inner.Resolve(ctx, wc.unlockedTag)
	})
	if isNotFound(err) {
		desc, err = wc.packLockManifest(ctx, "", 0, 0, "")
		if err != nil {
			return err
		}
		if err := retry(ctx, func(ctx context.Context) error {
			return tagPrePushNotFoundRetry(ctx, wc.client.repoClient.inner, desc, wc.unlockedTag)
		}); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}

	// Preflight: re-Resolve the lock tag right before overwriting it. A
	// missing tag is fine (nobody holds the lock); a moved tag means another
	// holder won — never overwrite their manifest with the marker.
	current, err := wc.client.repoClient.inner.Resolve(ctx, wc.lockTag)
	if err != nil && !isNotFound(err) {
		return fmt.Errorf("failed to check lock tag %q before retag to unlocked: %w", wc.lockTag, err)
	}
	if err == nil && current.Digest.String() != expectedDigest {
		return fmt.Errorf("lock tag %q changed before retag to unlocked (another holder may have re-locked)", wc.lockTag)
	}

	// The lock-marker write goes through publishMutableTag: a transient
	// (response-lost or failed) result is OBSERVED, never blindly re-applied —
	// a retried marker PUT could overwrite a rival that re-locked in the gap.
	// The documented no-CAS limit is unchanged: the pre-push digest-GET 404
	// inside tagPrePushNotFoundRetry keeps its bounded, stale-intent retry.
	if err := wc.publishMutableTag(ctx, desc, wc.lockTag); err != nil {
		return err
	}
	return wc.verifyUnlockedMarker(ctx, desc)
}

func (wc *workspaceClient) verifyUnlockedMarker(ctx context.Context, marker ocispec.Descriptor) error {
	// Read-back: OCI tags are last-writer-wins (no CAS), so a rival holder may
	// have re-tagged the lock between our Tag and this check. Re-Resolve and
	// confirm the tag points at the marker we just published — this detects
	// that another holder won, it cannot prevent it.
	got, err := retryWithResult(ctx, func(ctx context.Context) (ocispec.Descriptor, error) {
		return wc.client.repoClient.inner.Resolve(ctx, wc.lockTag)
	})
	if err != nil {
		return fmt.Errorf("failed to verify unlocked marker for %q: %w", wc.lockTag, err)
	}
	if got.Digest != marker.Digest {
		return fmt.Errorf("lock tag %q no longer points at the unlocked marker (another holder may have re-locked)", wc.lockTag)
	}
	return nil
}

func (wc *workspaceClient) fetchManifestWithDesc(ctx context.Context, reference string) (ocispec.Manifest, ocispec.Descriptor, error) {
	var (
		m    ocispec.Manifest
		desc ocispec.Descriptor
	)
	err := retry(ctx, func(ctx context.Context) error {
		var fetchErr error
		m, desc, fetchErr = wc.fetchManifestInternal(ctx, reference)
		return fetchErr
	})
	if err != nil {
		return ocispec.Manifest{}, ocispec.Descriptor{}, err
	}
	return m, desc, nil
}

func (wc *workspaceClient) fetchManifestInternal(ctx context.Context, reference string) (ocispec.Manifest, ocispec.Descriptor, error) {
	desc, err := wc.client.repoClient.inner.Resolve(ctx, reference)
	if err != nil {
		return ocispec.Manifest{}, ocispec.Descriptor{}, err
	}
	m, err := wc.fetchManifestByDesc(ctx, desc)
	if err != nil {
		if isNotFound(err) {
			return ocispec.Manifest{}, ocispec.Descriptor{}, &missingContentError{fmt.Errorf("manifest %s for %q: %w", desc.Digest, reference, err)}
		}
		return ocispec.Manifest{}, ocispec.Descriptor{}, err
	}
	return m, desc, nil
}

// fetchManifestByDesc fetches and parses the manifest identified by an
// ALREADY-RESOLVED descriptor — it never resolves a mutable tag, so callers
// validating a digest group get exactly the grouped bytes. It applies the
// same bounded read, decoding, and workspace validation as the by-tag path.
func (wc *workspaceClient) fetchManifestByDesc(ctx context.Context, desc ocispec.Descriptor) (ocispec.Manifest, error) {
	rc, err := wc.client.repoClient.inner.Fetch(ctx, desc)
	if err != nil {
		return ocispec.Manifest{}, err
	}
	defer func() { _ = rc.Close() }()

	data, err := io.ReadAll(io.LimitReader(rc, maxManifestSize))
	if err != nil {
		return ocispec.Manifest{}, err
	}

	var m ocispec.Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return ocispec.Manifest{}, fmt.Errorf("decoding manifest %s: %w", desc.Digest, err)
	}
	if m.Annotations == nil {
		m.Annotations = map[string]string{}
	}
	if err := validateWorkspaceManifest(m, desc.Digest.String(), wc.stateID); err != nil {
		return ocispec.Manifest{}, err
	}
	return m, nil
}

// ─── Package-level helpers ────────────────────────────────────────────────────

func parseLockInfo(m *ocispec.Manifest, stateTag string) (*LockInfo, error) {
	if m.ArtifactType != "" && m.ArtifactType != artifactTypeLock {
		return nil, fmt.Errorf("unexpected lock manifest artifactType %q", m.ArtifactType)
	}
	if m.Annotations == nil {
		return &LockInfo{}, nil
	}
	if raw, ok := m.Annotations[annotationLockInfo]; ok && raw != "" {
		var info LockInfo
		if err := json.Unmarshal([]byte(raw), &info); err != nil {
			return nil, fmt.Errorf("decoding lock info: %w", err)
		}
		if info.ID == "" {
			info.ID = m.Annotations[annotationLockID]
		}
		if info.Path == "" {
			info.Path = stateTag
		}
		return &info, nil
	}
	id := m.Annotations[annotationLockID]
	if id == "" {
		return &LockInfo{}, nil
	}
	return &LockInfo{ID: id, Path: stateTag}, nil
}

func parseLockManifestData(m *ocispec.Manifest) (*lockManifestData, error) {
	if m.ArtifactType != "" && m.ArtifactType != artifactTypeLock {
		return nil, fmt.Errorf("unexpected lock manifest artifactType %q", m.ArtifactType)
	}
	if raw, ok := m.Annotations[annotationLockGen]; ok && raw != "" {
		var data lockManifestData
		if err := json.Unmarshal([]byte(raw), &data); err != nil {
			return nil, fmt.Errorf("decoding lock generation data: %w", err)
		}
		return &data, nil
	}
	return &lockManifestData{Generation: 0}, nil
}

// workspaceTagFor hashes exact workspace bytes; literals never bypass encoding.
func workspaceTagFor(workspace string) string {
	h := sha256.Sum256([]byte(workspace))
	return hex.EncodeToString(h[:])
}

func (c *Client) workspaceTagFor(workspace string) string {
	// A nil client is used by a few pure tag-mapping tests. Treat it as the
	// zero-value configuration, which is the strict hashed layout.
	if c != nil && c.config.WorkspaceLayout == WorkspaceLayoutLegacy {
		return legacyWorkspaceTagFor(workspace)
	}
	return workspaceTagFor(workspace)
}

// listWorkspacesFromTags discovers workspace names by scanning the repository's
// OCI tags.
//
// Pre:  ctx is non-nil; repo is non-nil with a valid inner repository.
// Post: returns sorted, deduplicated original names from verified manifest
// annotations. Returns nil (not an error) if no workspaces exist. Legacy or
// ambiguous identities fail closed.
func listWorkspacesFromTags(ctx context.Context, repo *orasRepositoryClient) ([]string, error) {
	identities, err := repositoryWorkspaceIdentities(ctx, repo)
	if err != nil {
		return nil, err
	}
	var out []string
	for tag, name := range identities {
		if strings.HasPrefix(tag, stateTagPrefix) {
			out = append(out, name)
		}
	}
	slices.Sort(out)
	return slices.Compact(out), nil
}

func splitStateVersionTag(tag string) (base string, version int, ok bool) {
	if !strings.HasPrefix(tag, stateVersionTagPrefix) {
		return "", 0, false
	}
	idx := strings.LastIndex(tag, stateVersionTagSeparator)
	if idx < 0 {
		return "", 0, false
	}
	base = tag[:idx]
	if base == "" {
		return "", 0, false
	}
	s := tag[idx+len(stateVersionTagSeparator):]
	v, err := strconv.Atoi(s)
	// Version numbers are bounded to prevent overflow on 32-bit systems;
	// only the canonical form counts ("-v02" must not alias "-v2").
	if err != nil || v <= 0 || v > maxStateVersion || strconv.Itoa(v) != s {
		return "", 0, false
	}
	return base, v, true
}

func workspaceNameFromTag(ctx context.Context, repo *orasRepositoryClient, stateTag string) (string, error) {
	// Use retry for transient errors
	return retryWithResult(ctx, func(ctx context.Context) (string, error) {
		desc, err := repo.inner.Resolve(ctx, stateTag)
		if err != nil {
			return "", err
		}
		rc, err := repo.inner.Fetch(ctx, desc)
		if err != nil {
			return "", err
		}
		defer func() { _ = rc.Close() }()

		data, err := io.ReadAll(io.LimitReader(rc, maxManifestSize))
		if err != nil {
			return "", err
		}

		var m ocispec.Manifest
		if err := json.Unmarshal(data, &m); err != nil {
			return "", fmt.Errorf("decoding manifest for workspace tag %q: %w", stateTag, err)
		}
		name, ok := m.Annotations[annotationWorkspace]
		id, reserved := workspaceIDFromTag(stateTag)
		if !ok || !reserved || id != workspaceTagFor(name) {
			return "", fmt.Errorf("workspace identity mismatch at tag %q; stop writers and restore verified metadata (see docs/guides/workspace-migration.md)", stateTag)
		}
		return name, nil
	})
}

// compressGzip compresses data using gzip at the default level.
//
// Pre:  data may be nil or empty (both produce valid gzip output).
// Post: len(result) > 0; gzip.NewReader(bytes.NewReader(result)) succeeds and
//
//	decompressing result reproduces data exactly.
func compressGzip(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write(data); err != nil {
		_ = gz.Close()
		return nil, err
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// ─── Retry helpers ────────────────────────────────────────────────────────────
//
// Retry runs operation up to 3 times with exponential backoff (1s, 2s),
// retrying only on transient errors.
func retry(ctx context.Context, operation func(context.Context) error) error {
	_, err := retryWithResult(ctx, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, operation(ctx)
	})
	return err
}

func retryWithResult[T any](ctx context.Context, operation func(context.Context) (T, error)) (T, error) {
	var zero T
	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		result, err := operation(ctx)
		if err == nil {
			return result, nil
		}
		lastErr = err
		if ctx.Err() != nil {
			// The context was cancelled during the failed attempt: surface
			// the cancellation (not the op error) so errors.Is matches,
			// while keeping the op error inspectable via errors.As/Unwrap.
			return zero, fmt.Errorf("%w: %w", ctx.Err(), err)
		}
		if !isTransientError(err) {
			return zero, err
		}
		if attempt < 3 {
			select {
			case <-ctx.Done():
				return zero, ctx.Err()
			case <-time.After(time.Duration(attempt) * time.Second):
			}
		}
	}
	return zero, lastErr
}

// isTransientError reports whether err is worth retrying: a retryable HTTP
// status, a network-level reset/refusal/DNS failure, a timeout, or a truncated
// read.
func isTransientError(err error) bool {
	if err == nil {
		return false
	}

	var errResp *orasErrcode.ErrorResponse
	if errors.As(err, &errResp) {
		switch errResp.StatusCode {
		case http.StatusTooManyRequests,
			http.StatusRequestTimeout,
			http.StatusInternalServerError,
			http.StatusBadGateway,
			http.StatusServiceUnavailable,
			http.StatusGatewayTimeout:
			return true
		}
		return false
	}

	switch {
	case errors.Is(err, syscall.ECONNRESET),
		errors.Is(err, syscall.ECONNREFUSED),
		errors.Is(err, io.ErrUnexpectedEOF),
		errors.Is(err, io.EOF),
		errors.Is(err, context.DeadlineExceeded):
		return true
	}

	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return true
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		return netErr.Timeout()
	}

	return false
}

// ─── Error helpers ────────────────────────────────────────────────────────────

func isNotFound(err error) bool {
	if errors.Is(err, errdef.ErrNotFound) {
		return true
	}
	var resp *orasErrcode.ErrorResponse
	if errors.As(err, &resp) {
		return resp.StatusCode == 404
	}
	return false
}

func isDeleteUnsupported(err error) bool {
	var resp *orasErrcode.ErrorResponse
	if errors.As(err, &resp) {
		return resp.StatusCode == 405
	}
	return false
}
