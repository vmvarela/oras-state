// Phase 1 failure/recovery tests for issue #9, derived from the public
// contracts: Get/GetVersion absent-vs-incomplete distinction, Put partial
// publication and recovery, lock-cleanup failure surfacing, GHCR-fallback
// CredentialFunc cancellation, Delete descriptor pinning across bounded
// retries, and retagToUnlocked's no-blind-replay rule (the documented
// pre-push-404 stale-intent window stays). Deterministic only: channel gates
// and counted calls, no sleeps to create races.
package orasstate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/errdef"
)

// ─── (1) Get / GetVersion: incomplete state is not "absent" ──────────────────

// seedStateManifest pushes a minimal state manifest (one layer, workspace
// annotation) into the fake repo and returns its descriptor.
func seedStateManifest(ctx context.Context, t *testing.T, fake *fakeORASRepo, data string) ocispec.Descriptor {
	t.Helper()
	layerDesc := ocispec.Descriptor{
		MediaType: mediaTypeStateLayer,
		Digest:    digest.FromBytes([]byte(data)),
		Size:      int64(len(data)),
	}
	if err := fake.Push(ctx, layerDesc, bytes.NewReader([]byte(data))); err != nil {
		t.Fatalf("push layer: %v", err)
	}
	raw, err := json.Marshal(ocispec.Manifest{
		MediaType:    ocispec.MediaTypeImageManifest,
		ArtifactType: artifactTypeState,
		Annotations:  map[string]string{annotationWorkspace: "default"},
		Layers:       []ocispec.Descriptor{layerDesc},
	})
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	desc := ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageManifest,
		Digest:    digest.FromBytes(raw),
		Size:      int64(len(raw)),
	}
	if err := fake.Push(ctx, desc, bytes.NewReader(raw)); err != nil {
		t.Fatalf("push manifest: %v", err)
	}
	return desc
}

// dropBlob removes one blob from the fake repo, leaving any tags that point
// at its digest in place: the tag resolves, the content is gone.
func dropBlob(fake *fakeORASRepo, d digest.Digest) {
	fake.mu.Lock()
	delete(fake.blobs, d)
	fake.mu.Unlock()
}

// TestGet_IncompleteStateNotReportedAsAbsent — the state tag resolves but its
// manifest (or layer) content is missing from the registry. Get must surface
// an error, NOT (nil, nil): reporting the incomplete existing state as "no
// state exists" invites destructive recovery (e.g. re-Put from a stale
// snapshot).
func TestGet_IncompleteStateNotReportedAsAbsent(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		drop func(t *testing.T, fake *fakeORASRepo, wc *workspaceClient)
	}{
		{name: "manifest blob missing", drop: func(t *testing.T, fake *fakeORASRepo, wc *workspaceClient) {
			desc, err := fake.Resolve(ctx, wc.stateTag)
			if err != nil {
				t.Fatalf("resolve state tag: %v", err)
			}
			dropBlob(fake, desc.Digest)
		}},
		{name: "layer blob missing", drop: func(t *testing.T, fake *fakeORASRepo, wc *workspaceClient) {
			fm, _, err := wc.fetchManifestWithDesc(ctx, wc.stateTag)
			if err != nil {
				t.Fatalf("fetch state manifest: %v", err)
			}
			if len(fm.Layers) == 0 {
				t.Fatal("state manifest has no layers")
			}
			dropBlob(fake, fm.Layers[0].Digest)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := newFakeORASRepo()
			repo := &orasRepositoryClient{inner: fake}
			c := newTestClient(repo)
			wc := newRemoteClient(repo, "default")
			desc := seedStateManifest(ctx, t, fake, "state-data")
			if err := fake.Tag(ctx, desc, wc.stateTag); err != nil {
				t.Fatalf("tag state: %v", err)
			}

			tc.drop(t, fake, wc)

			got, err := c.Get(ctx, "default")
			if err == nil {
				t.Fatalf("Get returned (%q, nil) for incomplete state; must not report existing state as absent", got)
			}
		})
	}
}

// mustResolve resolves reference or fails the test.
func mustResolve(ctx context.Context, t *testing.T, fake *fakeORASRepo, reference string) ocispec.Descriptor {
	t.Helper()
	desc, err := fake.Resolve(ctx, reference)
	if err != nil {
		t.Fatalf("resolve %q: %v", reference, err)
	}
	return desc
}

// TestGetVersion_IncompleteVersionNotVersionNotFound — a version tag that
// resolves but whose content is missing must fail with an error DISTINCT from
// ErrStateVersionNotFound (which is reserved for a genuinely absent version
// tag), and present versions keep working.
func TestGetVersion_IncompleteVersionNotVersionNotFound(t *testing.T) {
	ctx := context.Background()
	fake := newFakeORASRepo()
	repo := &orasRepositoryClient{inner: fake}
	c := newTestClient(repo)
	c.config.MaxVersions = 3
	wc := newRemoteClient(repo, "default")

	for _, data := range []string{"state-v1", "state-v2"} {
		if err := c.Put(ctx, "default", []byte(data)); err != nil {
			t.Fatalf("put %q: %v", data, err)
		}
	}
	c.WaitForRetention()

	// Corrupt version 1: tag resolves, manifest content is gone.
	v1 := wc.versionTagFor(1)
	dropBlob(fake, mustResolve(ctx, t, fake, v1).Digest)

	if _, err := c.GetVersion(ctx, "default", 1); err == nil {
		t.Fatal("GetVersion(1) returned nil error for incomplete version content")
	} else if errors.Is(err, ErrStateVersionNotFound) {
		t.Fatalf("GetVersion(1) = %v; incomplete content must be distinguished from ErrStateVersionNotFound", err)
	}

	got, err := c.GetVersion(ctx, "default", 2)
	if err != nil || string(got) != "state-v2" {
		t.Fatalf("GetVersion(2) = (%q, %v), want intact version still readable", got, err)
	}

	// A genuinely absent version tag keeps the documented sentinel.
	if _, err := c.GetVersion(ctx, "default", 9); !errors.Is(err, ErrStateVersionNotFound) {
		t.Fatalf("GetVersion(9) = %v, want ErrStateVersionNotFound", err)
	}
}

// ─── (2) Put: partial publication error and safe continuation ────────────────

// TestPut_PartialPublicationErrorAndRecovery — the version-tag publication
// fails after the state tag landed: Put reports *PartialPublicationError, the
// state stays readable, the failed version tag is never re-applied, and the
// NEXT Put continues safely from the published state's version annotation
// (minting version 2, not blindly replaying version 1).
func TestPut_PartialPublicationErrorAndRecovery(t *testing.T) {
	ctx := context.Background()
	fake := newFakeORASRepo()
	versionTag := stateVersionTagPrefix + workspaceTagFor("default") + stateVersionTagSeparator + "1"
	hook := &hookingRepo{inner: fake, watch: versionTag, failTagCount: 1}
	c := newTestClient(&orasRepositoryClient{inner: hook, repository: "example.com/test/repo"})
	c.config.MaxVersions = 3

	err := c.Put(ctx, "default", []byte("state-v1"))
	var partial *PartialPublicationError
	if !errors.As(err, &partial) {
		t.Fatalf("Put error = %v (%T), want *PartialPublicationError", err, err)
	}
	if partial.StateTag != stateTagPrefix+workspaceTagFor("default") {
		t.Errorf("partial.StateTag = %q", partial.StateTag)
	}
	if partial.VersionTag != versionTag {
		t.Errorf("partial.VersionTag = %q, want %q", partial.VersionTag, versionTag)
	}
	if !errors.Is(partial.Err, ErrMutableTagMoved) {
		t.Errorf("partial.Err = %v, want errors.Is(ErrMutableTagMoved)", partial.Err)
	}

	// The state write is NOT wholly rejected: the state is readable.
	got, gerr := c.Get(ctx, "default")
	if gerr != nil || string(got) != "state-v1" {
		t.Fatalf("Get after partial publication = (%q, %v), want the published state", got, gerr)
	}
	// The failed version tag was never re-applied or deleted.
	if _, rerr := fake.Resolve(ctx, versionTag); !errors.Is(rerr, errdef.ErrNotFound) {
		t.Errorf("version tag resolve err = %v, want still absent (no re-apply)", rerr)
	}

	// Safe continuation: the next Put derives version 2 from the published
	// state annotation and succeeds.
	if err := c.Put(ctx, "default", []byte("state-v2")); err != nil {
		t.Fatalf("recovery Put: %v", err)
	}
	c.WaitForRetention()

	if got, err := c.Get(ctx, "default"); err != nil || string(got) != "state-v2" {
		t.Fatalf("Get after recovery = (%q, %v)", got, err)
	}
	if got, err := c.GetVersion(ctx, "default", 2); err != nil || string(got) != "state-v2" {
		t.Fatalf("GetVersion(2) = (%q, %v), want the recovered write", got, err)
	}
	if _, err := c.GetVersion(ctx, "default", 1); !errors.Is(err, ErrStateVersionNotFound) {
		t.Fatalf("GetVersion(1) = %v, want ErrStateVersionNotFound (never replayed)", err)
	}
	versions, err := c.ListVersions(ctx, "default")
	if err != nil || len(versions) != 1 || versions[0].Version != 2 {
		t.Fatalf("ListVersions = %+v (%v), want exactly [2]", versions, err)
	}
}

// ─── (3) Lock: cleanup failure surfaced with the verification failure ────────

// lockCleanupFailRepo fails the post-Tag verification Fetch of our lock
// manifest (non-transient) and the cleanup Delete of our lock manifest
// (non-transient), everything else delegates.
type lockCleanupFailRepo struct {
	delegatingRepo
	lockTag string

	mu        sync.Mutex
	tagged    bool
	desc      ocispec.Descriptor
	fetchErr  error
	deleteErr error
}

func (r *lockCleanupFailRepo) Tag(ctx context.Context, desc ocispec.Descriptor, reference string) error {
	if err := r.delegatingRepo.Tag(ctx, desc, reference); err != nil {
		return err
	}
	if reference == r.lockTag {
		r.mu.Lock()
		r.tagged = true
		r.desc = desc
		r.mu.Unlock()
	}
	return nil
}

func (r *lockCleanupFailRepo) ours(target ocispec.Descriptor) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.tagged && target.Digest == r.desc.Digest
}

func (r *lockCleanupFailRepo) Fetch(ctx context.Context, target ocispec.Descriptor) (io.ReadCloser, error) {
	if r.ours(target) {
		return nil, r.fetchErr
	}
	return r.delegatingRepo.Fetch(ctx, target)
}

func (r *lockCleanupFailRepo) Delete(ctx context.Context, target ocispec.Descriptor) error {
	if r.ours(target) {
		return r.deleteErr
	}
	return r.delegatingRepo.Delete(ctx, target)
}

// TestLock_VerificationFailureWithFailingCleanupSurfaced — when post-Tag lock
// verification fails AND the detached cleanup also fails, the returned error
// must carry BOTH the original verification failure and the cleanup failure,
// and the remote state must remain inspectable/recoverable: the lock tag
// still points at OUR manifest with OUR holder info.
func TestLock_VerificationFailureWithFailingCleanupSurfaced(t *testing.T) {
	ctx := context.Background()
	verifyErr := errors.New("verification fetch failed (test sentinel)")
	cleanupErr := errors.New("cleanup delete failed (test sentinel)")

	fake := newFakeORASRepo()
	hook := &lockCleanupFailRepo{
		delegatingRepo: delegatingRepo{inner: fake},
		fetchErr:       verifyErr,
		deleteErr:      cleanupErr,
	}
	repo := &orasRepositoryClient{inner: hook}
	c := newRemoteClient(repo, "default")
	hook.lockTag = c.lockTag

	_, err := c.lock(ctx, &LockInfo{ID: "cleanup-fail-lock", Operation: "apply", Created: time.Now()})
	if err == nil {
		t.Fatal("expected lock to fail when verification fails, got nil")
	}
	if !errors.Is(err, verifyErr) {
		t.Errorf("error = %v; original verification failure must be surfaced", err)
	}
	if !errors.Is(err, cleanupErr) {
		t.Errorf("error = %v; the cleanup failure must be surfaced alongside it", err)
	}

	// Inspectable/recoverable remote state: the lock tag still resolves to
	// our manifest with our holder ID (read the blob directly to bypass the
	// hook's failing Fetch).
	desc, rerr := fake.Resolve(ctx, c.lockTag)
	if rerr != nil || desc.Digest != hook.desc.Digest {
		t.Fatalf("lock tag resolves to (%v, %v), want our manifest %s", desc.Digest, rerr, hook.desc.Digest)
	}
	fake.mu.Lock()
	raw := fake.blobs[desc.Digest]
	fake.mu.Unlock()
	var m ocispec.Manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("unmarshal lock manifest: %v", err)
	}
	info, err := parseLockInfo(&m, c.stateTag)
	if err != nil || info == nil || info.ID != "cleanup-fail-lock" {
		t.Fatalf("lock holder = %+v (%v), want our own holder (recoverable via Unlock)", info, err)
	}
}

// ─── (4) GHCR fallback: cancellation reaches the caller CredentialFunc ───────

// TestGHCRFallback_PropagatesCancelToCredentialFunc — the GHCR Packages API
// fallback resolves its token via the caller's CredentialFunc with the
// operation's context: cancelling the caller's context must cancel the
// credential resolution (discovery policy itself is unchanged).
func TestGHCRFallback_PropagatesCancelToCredentialFunc(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	credEntered := make(chan struct{})
	var gotCancellation bool
	credFn, _, _ := resolveCredentials("ghcr.io", "owner/repo", Config{CredentialFunc: func(ctx context.Context, hostport string) (Credential, error) {
		close(credEntered)
		select {
		case <-ctx.Done():
			gotCancellation = true
			return Credential{}, ctx.Err()
		case <-time.After(testObservationTimeout):
			return Credential{}, errors.New("CredentialFunc did not receive a cancellable context")
		}
	}})

	fake := newFakeORASRepo()
	wc := newRemoteClient(&orasRepositoryClient{inner: fake}, "default")
	if err := wc.put(ctx, []byte("state-data")); err != nil {
		t.Fatalf("seed put: %v", err)
	}

	repo := &orasRepositoryClient{
		inner:        &deleteUnsupportedRepo{delegatingRepo: delegatingRepo{inner: fake}},
		repository:   "ghcr.io/owner/repo",
		credentialFn: credFn,
	}
	c := newTestClient(repo)

	go func() {
		<-credEntered
		cancel()
	}()

	err := c.Delete(ctx, "default")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Delete error = %v, want context.Canceled propagated into the GHCR-fallback CredentialFunc", err)
	}
	if !gotCancellation {
		t.Error("CredentialFunc observed no cancellation; the operation context was not propagated")
	}
}

// ─── (5) Delete: pinned descriptor across bounded retries ────────────────────

// retargetingDeleteRepo fails the first Delete transiently, and a concurrent
// writer publishes new state under the state tag in that same window. If
// Delete re-resolves per attempt it retargets the rival's manifest.
type retargetingDeleteRepo struct {
	delegatingRepo
	stateTag  string
	rivalDesc ocispec.Descriptor

	mu          sync.Mutex
	deleteCalls int
}

func (r *retargetingDeleteRepo) Delete(ctx context.Context, target ocispec.Descriptor) error {
	r.mu.Lock()
	r.deleteCalls++
	first := r.deleteCalls == 1
	r.mu.Unlock()
	if first {
		// The rival manifest's content is already in the repo; the writer
		// retargets the tag while our first delete attempt fails transiently.
		if err := r.delegatingRepo.Tag(ctx, r.rivalDesc, r.stateTag); err != nil {
			return err
		}
		return transientHTTPStatus(http.StatusInternalServerError)
	}
	return r.delegatingRepo.Delete(ctx, target)
}

// TestDelete_PinsResolvedDescriptorAcrossRetries — Delete resolves the state
// tag once and pins the descriptor: a transient delete failure followed by a
// rival write under the tag must never make a retry delete the rival's
// manifest.
func TestDelete_PinsResolvedDescriptorAcrossRetries(t *testing.T) {
	ctx := context.Background()
	fake := newFakeORASRepo()
	seeder := newRemoteClient(&orasRepositoryClient{inner: fake}, "default")
	if err := seeder.put(ctx, []byte("state-original")); err != nil {
		t.Fatalf("seed put: %v", err)
	}

	rivalDesc := seedStateManifest(ctx, t, fake, "state-rival")

	originalDigest := mustResolve(ctx, t, fake, seeder.stateTag).Digest

	hook := &retargetingDeleteRepo{
		delegatingRepo: delegatingRepo{inner: fake},
		stateTag:       seeder.stateTag,
		rivalDesc:      rivalDesc,
	}
	c := newTestClient(&orasRepositoryClient{inner: hook, repository: "example.com/test/repo"})

	if err := c.Delete(ctx, "default"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// The rival's manifest must have survived: the retry must not retarget.
	got, gerr := c.Get(ctx, "default")
	if gerr != nil || string(got) != "state-rival" {
		t.Fatalf("Get after Delete = (%q, %v); the concurrent writer's state must not be deleted by a retried delete", got, gerr)
	}
	// And the originally resolved manifest is gone.
	if _, ok := fake.blobs[originalDigest]; ok {
		t.Error("originally resolved manifest blob still present; the pinned delete did not run")
	}
}

// ─── (6) retagToUnlocked: no blind replay after a transient response ─────────

// TestRetagToUnlocked_NoBlindReplayAfterTransientResponse — deterministic
// rival-between-attempts: the lock-marker Tag fails transiently (nothing
// lands), a rival re-locks while the retag is parked observing the tag, and
// the retag must fail closed with ErrMutableTagMoved instead of blindly
// replaying the marker write over the rival.
func TestRetagToUnlocked_NoBlindReplayAfterTransientResponse(t *testing.T) {
	ctx := context.Background()
	fake := newFakeORASRepo()

	setup := &orasRepositoryClient{inner: fake}
	setupWC := newRemoteClient(setup, "default")
	lockDesc, err := setupWC.packLockManifest(ctx, `{"ID":"holder"}`, 4, 0, "holder")
	if err != nil {
		t.Fatalf("pack lock manifest: %v", err)
	}
	if err := fake.Tag(ctx, lockDesc, setupWC.lockTag); err != nil {
		t.Fatalf("seed lock tag: %v", err)
	}

	// blockFrom=2: the first lock-tag Resolve is the preflight (must see our
	// digest); the second is the post-transient observation, parked on the
	// gate so the rival can install strictly between the attempts.
	gate := &gateResolveRepo{
		inner:         fake,
		watch:         setupWC.lockTag,
		blockFrom:     2,
		release:       make(chan struct{}),
		firstObserved: make(chan struct{}),
	}
	hook := &hookingRepo{inner: gate, watch: setupWC.lockTag, failTagCount: 1}
	wc := newRemoteClient(&orasRepositoryClient{inner: hook, repository: "example.com/test/repo"}, "default")

	errCh := make(chan error, 1)
	go func() { errCh <- wc.retagToUnlocked(ctx, lockDesc.Digest.String()) }()

	select {
	case <-gate.firstObserved:
	case <-time.After(testObservationTimeout):
		t.Fatal("timed out waiting for the parked tag observation")
	}
	rivalDesc, _ := newRivalLockManifest(ctx, t, fake, "rival-relock", 9)
	if err := fake.Tag(ctx, rivalDesc, wc.lockTag); err != nil {
		t.Fatalf("rival install: %v", err)
	}
	close(gate.release)

	err = <-errCh
	if !errors.Is(err, ErrMutableTagMoved) {
		t.Fatalf("retagToUnlocked error = %v, want fail-closed ErrMutableTagMoved (no blind replay)", err)
	}
	if calls := hook.tagCallCount(); calls != 1 {
		t.Errorf("lock-tag Tag calls = %d, want exactly 1 (transient response must not be replayed)", calls)
	}
	if got, rerr := fake.Resolve(ctx, wc.lockTag); rerr != nil || got.Digest != rivalDesc.Digest {
		t.Errorf("lock tag = (%v, %v), want rival %s preserved", got.Digest, rerr, rivalDesc.Digest)
	}
}

// lockVanishRepo simulates the lock manifest disappearing between lock
// verification and cleanup: vanishOnVerifyFetch removes the lock tag/blob
// right before failing the verification Fetch (so cleanup's Resolve finds
// the tag genuinely absent); vanishOnDelete removes them when the cleanup
// Delete runs and reports NotFound (already deleted).
type lockVanishRepo struct {
	delegatingRepo
	lockTag        string
	verifyFetchErr error

	vanishOnVerifyFetch bool
	vanishOnDelete      bool

	mu     sync.Mutex
	tagged bool
	desc   ocispec.Descriptor
}

func (r *lockVanishRepo) Tag(ctx context.Context, desc ocispec.Descriptor, reference string) error {
	if err := r.delegatingRepo.Tag(ctx, desc, reference); err != nil {
		return err
	}
	if reference == r.lockTag {
		r.mu.Lock()
		r.tagged = true
		r.desc = desc
		r.mu.Unlock()
	}
	return nil
}

func (r *lockVanishRepo) ours(target ocispec.Descriptor) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.tagged && target.Digest == r.desc.Digest
}

// vanishLock removes the lock manifest and its tag from the inner fake repo,
// making the remote lock actually absent.
func (r *lockVanishRepo) vanishLock() {
	inner := r.delegatingRepo.inner
	inner.mu.Lock()
	delete(inner.blobs, r.desc.Digest)
	delete(inner.tags, r.lockTag)
	inner.mu.Unlock()
}

func (r *lockVanishRepo) Fetch(ctx context.Context, target ocispec.Descriptor) (io.ReadCloser, error) {
	if r.ours(target) {
		if r.vanishOnVerifyFetch {
			r.vanishLock()
		}
		return nil, r.verifyFetchErr
	}
	return r.delegatingRepo.Fetch(ctx, target)
}

func (r *lockVanishRepo) Delete(ctx context.Context, target ocispec.Descriptor) error {
	if r.ours(target) && r.vanishOnDelete {
		r.vanishLock()
		return errdef.ErrNotFound
	}
	return r.delegatingRepo.Delete(ctx, target)
}

// TestLock_CleanupIdempotentWhenLockAlreadyGone — a confirmed cleanup
// NotFound (Resolve or Delete of the lock tag) is idempotent success: only
// the original verification failure is surfaced, no cleanup failure is
// attached, and the remote lock is actually absent.
func TestLock_CleanupIdempotentWhenLockAlreadyGone(t *testing.T) {
	ctx := context.Background()
	verifyErr := errors.New("verification fetch failed (test sentinel)")

	for _, tc := range []struct {
		name                string
		vanishOnVerifyFetch bool
		vanishOnDelete      bool
	}{
		{name: "cleanup resolve not found", vanishOnVerifyFetch: true},
		{name: "cleanup delete not found", vanishOnDelete: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := newFakeORASRepo()
			hook := &lockVanishRepo{
				delegatingRepo:      delegatingRepo{inner: fake},
				lockTag:             lockTagPrefix + workspaceTagFor("default"),
				verifyFetchErr:      verifyErr,
				vanishOnVerifyFetch: tc.vanishOnVerifyFetch,
				vanishOnDelete:      tc.vanishOnDelete,
			}
			c := newRemoteClient(&orasRepositoryClient{inner: hook}, "default")

			_, err := c.lock(ctx, &LockInfo{ID: "vanish-lock", Operation: "apply", Created: time.Now()})
			if err == nil {
				t.Fatal("expected lock to fail when verification fails, got nil")
			}
			if !errors.Is(err, verifyErr) {
				t.Errorf("error = %v; the original verification failure must be preserved", err)
			}
			if strings.Contains(err.Error(), "lock-tag cleanup") {
				t.Errorf("error = %v; a confirmed cleanup NotFound is idempotent success and must not surface a cleanup failure", err)
			}
			// The remote lock is actually absent: recovery needs no cleanup.
			if _, rerr := fake.Resolve(ctx, hook.lockTag); !errors.Is(rerr, errdef.ErrNotFound) {
				t.Errorf("lock tag resolve err = %v, want absent", rerr)
			}
		})
	}
}

// tagNotFoundOnceRepo returns errdef.ErrNotFound for the FIRST Tag call of
// its watched reference only (the pinned ORAS pre-push digest-GET 404) and
// records the injection; other references are never counted or failed.
type tagNotFoundOnceRepo struct {
	delegatingRepo
	watch string

	mu          sync.Mutex
	watchCalls  int
	injected404 bool
}

func (r *tagNotFoundOnceRepo) Tag(ctx context.Context, desc ocispec.Descriptor, reference string) error {
	r.mu.Lock()
	matched := reference == r.watch
	if matched {
		r.watchCalls++
	}
	inject := matched && r.watchCalls == 1
	if inject {
		r.injected404 = true
	}
	r.mu.Unlock()
	if inject {
		return errdef.ErrNotFound
	}
	return r.delegatingRepo.Tag(ctx, desc, reference)
}

// TestRetagToUnlocked_KeepsDocumentedPrePush404Window — the documented no-CAS
// limit is preserved: the lock-marker write still retries the pre-push 404
// (stale-intent window accepted; never described as CAS).
func TestRetagToUnlocked_KeepsDocumentedPrePush404Window(t *testing.T) {
	ctx := context.Background()
	fake := newFakeORASRepo()
	c := newRemoteClient(&orasRepositoryClient{inner: fake}, "default")
	hook := &tagNotFoundOnceRepo{delegatingRepo: delegatingRepo{inner: fake}, watch: c.lockTag}
	wc := newRemoteClient(&orasRepositoryClient{inner: hook, repository: "example.com/test/repo"}, "default")

	// Cold start: no lock tag; the marker write hits the pre-push 404 once
	// and must still succeed on the bounded retry.
	if err := wc.retagToUnlocked(ctx, ""); err != nil {
		t.Fatalf("retagToUnlocked: %v", err)
	}
	hook.mu.Lock()
	calls := hook.watchCalls
	injected := hook.injected404
	hook.mu.Unlock()
	if !injected {
		t.Error("pre-push 404 was never injected for the watched lock tag; the test double did not fire")
	}
	if calls != 2 {
		t.Errorf("watched lock-tag Tag calls = %d, want exactly 2 (one injected pre-push 404 + one retry; the documented stale-intent window)", calls)
	}
}
