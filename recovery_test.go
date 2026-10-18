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
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/errdef"
)

// ─── Phase 2: distinct current/history manifests (issue #9) ──────────────────

// countingOpsRepo counts Tag and Delete calls, delegating everything.
type countingOpsRepo struct {
	delegatingRepo

	mu          sync.Mutex
	tagCalls    int
	deleteCalls int
}

func (r *countingOpsRepo) Tag(ctx context.Context, desc ocispec.Descriptor, reference string) error {
	r.mu.Lock()
	r.tagCalls++
	r.mu.Unlock()
	return r.delegatingRepo.Tag(ctx, desc, reference)
}

func (r *countingOpsRepo) Delete(ctx context.Context, target ocispec.Descriptor) error {
	r.mu.Lock()
	r.deleteCalls++
	r.mu.Unlock()
	return r.delegatingRepo.Delete(ctx, target)
}

func (r *countingOpsRepo) counts() (tags, deletes int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.tagCalls, r.deleteCalls
}

// deleteTrackingRepo records every deleted digest and counts Tag calls,
// delegating everything.
type deleteTrackingRepo struct {
	delegatingRepo

	mu       sync.Mutex
	deleted  []digest.Digest
	tagCalls int
}

func (r *deleteTrackingRepo) Tag(ctx context.Context, desc ocispec.Descriptor, reference string) error {
	r.mu.Lock()
	r.tagCalls++
	r.mu.Unlock()
	return r.delegatingRepo.Tag(ctx, desc, reference)
}

func (r *deleteTrackingRepo) Delete(ctx context.Context, target ocispec.Descriptor) error {
	r.mu.Lock()
	r.deleted = append(r.deleted, target.Digest)
	r.mu.Unlock()
	return r.delegatingRepo.Delete(ctx, target)
}

// errHistoryPackFailed is the non-transient sentinel injected into the Push
// of any manifest carrying a history marker (the H preparation step).
var errHistoryPackFailed = errors.New("history manifest push failed (test sentinel)")

// historyPackFailRepo fails the Push of history manifests only; every other
// payload (layers, config, current manifests) is forwarded. Content readers
// are consumed, so the payload is re-wrapped before delegating.
type historyPackFailRepo struct {
	delegatingRepo
}

func (r *historyPackFailRepo) Push(ctx context.Context, expected ocispec.Descriptor, content io.Reader) error {
	b, err := io.ReadAll(content)
	if err != nil {
		return err
	}
	var m ocispec.Manifest
	if json.Unmarshal(b, &m) == nil && m.Annotations[annotationStateHistory] != "" {
		return errHistoryPackFailed
	}
	return r.delegatingRepo.Push(ctx, expected, bytes.NewReader(b))
}

// seedVersionManifest pushes a state manifest for version N — with an
// optional history marker and the LITERAL artifact type given ("" produces
// an untyped manifest) — and tags it stver-*-vN. Returns the manifest
// descriptor.
func seedVersionManifest(ctx context.Context, t *testing.T, fake *fakeORASRepo, wc *workspaceClient, version int, data string, marker bool, artifactType string) ocispec.Descriptor {
	t.Helper()
	layerDesc := ocispec.Descriptor{
		MediaType: mediaTypeStateLayer,
		Digest:    digest.FromBytes([]byte(data)),
		Size:      int64(len(data)),
	}
	if err := fake.Push(ctx, layerDesc, bytes.NewReader([]byte(data))); err != nil {
		t.Fatalf("push layer: %v", err)
	}
	now := "2026-01-01T00:00:00Z"
	annotations := map[string]string{
		annotationWorkspace:    wc.stateID,
		annotationUpdatedAt:    now,
		annotationCreated:      now,
		annotationStateVersion: strconv.Itoa(version),
	}
	if marker {
		annotations[annotationStateHistory] = strconv.Itoa(version)
	}
	raw, err := json.Marshal(ocispec.Manifest{
		MediaType:    ocispec.MediaTypeImageManifest,
		ArtifactType: artifactType,
		Annotations:  annotations,
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
	if err := fake.Tag(ctx, desc, wc.versionTagFor(version)); err != nil {
		t.Fatalf("tag version %d: %v", version, err)
	}
	return desc
}

// TestPut_CurrentAndHistoryManifestsDistinct — a versioned Put publishes two
// DISTINCT manifests: C (state tag, no history marker) and H (version tag,
// dedicated marker), sharing the same state layer, both keeping the state
// artifact type and workspace/updated_at/version annotations (legacy reader
// compatibility). Bytes and created timestamps are identical under raw and
// gzip layers.
func TestPut_CurrentAndHistoryManifestsDistinct(t *testing.T) {
	ctx := context.Background()
	payload := "history-protocol-state"
	for _, compression := range []bool{false, true} {
		t.Run(fmt.Sprintf("compression=%t", compression), func(t *testing.T) {
			fake := newFakeORASRepo()
			repo := &orasRepositoryClient{inner: fake, repository: "example.com/test/repo"}
			c := newTestClient(repo)
			c.config.MaxVersions = 3
			c.config.Compression = compression
			wc := newRemoteClient(repo, "default")

			if err := c.Put(ctx, "default", []byte(payload)); err != nil {
				t.Fatalf("put: %v", err)
			}
			c.WaitForRetention()

			current, currentDesc, err := wc.fetchManifestWithDesc(ctx, wc.stateTag)
			if err != nil {
				t.Fatalf("fetch current manifest: %v", err)
			}
			history, historyDesc, err := wc.fetchManifestWithDesc(ctx, wc.versionTagFor(1))
			if err != nil {
				t.Fatalf("fetch history manifest: %v", err)
			}

			if currentDesc.Digest == historyDesc.Digest {
				t.Fatal("current and history manifests share a digest; they must be distinct manifests")
			}
			if len(current.Layers) != 1 || len(history.Layers) != 1 {
				t.Fatalf("layers = %d/%d, want 1/1", len(current.Layers), len(history.Layers))
			}
			if current.Layers[0].Digest != history.Layers[0].Digest {
				t.Error("current and history manifests must share the same state layer")
			}
			wantLayer := mediaTypeStateLayer
			if compression {
				wantLayer = mediaTypeStateLayerGzip
			}
			if current.Layers[0].MediaType != wantLayer || history.Layers[0].MediaType != wantLayer {
				t.Errorf("layer media types = %q/%q, want %q", current.Layers[0].MediaType, history.Layers[0].MediaType, wantLayer)
			}
			if _, marked := current.Annotations[annotationStateHistory]; marked {
				t.Error("current manifest must not carry the history marker")
			}
			if history.Annotations[annotationStateHistory] != "1" {
				t.Errorf("history marker = %q, want %q", history.Annotations[annotationStateHistory], "1")
			}
			// Legacy reader compatibility: the shared annotation surface.
			for name, m := range map[string]ocispec.Manifest{"current": current, "history": history} {
				if m.Annotations[annotationWorkspace] != "default" {
					t.Errorf("%s workspace annotation = %q", name, m.Annotations[annotationWorkspace])
				}
				if m.Annotations[annotationStateVersion] != "1" {
					t.Errorf("%s version annotation = %q", name, m.Annotations[annotationStateVersion])
				}
				if m.ArtifactType != "" && m.ArtifactType != artifactTypeState {
					t.Errorf("%s artifactType = %q", name, m.ArtifactType)
				}
				if _, err := time.Parse(time.RFC3339, m.Annotations[annotationCreated]); err != nil {
					t.Errorf("%s created annotation %q is not RFC3339: %v", name, m.Annotations[annotationCreated], err)
				}
			}
			if current.Annotations[annotationCreated] == "" || current.Annotations[annotationCreated] != history.Annotations[annotationCreated] {
				t.Errorf("created annotations differ (%q vs %q); a logical Put must use a single RFC3339 value",
					current.Annotations[annotationCreated], history.Annotations[annotationCreated])
			}

			// Bytes preserved through both readers.
			if got, err := c.Get(ctx, "default"); err != nil || string(got) != payload {
				t.Errorf("Get = (%q, %v), want %q", got, err, payload)
			}
			if got, err := c.GetVersion(ctx, "default", 1); err != nil || string(got) != payload {
				t.Errorf("GetVersion(1) = (%q, %v), want %q", got, err, payload)
			}
		})
	}
}

// TestPackStateManifestReproducibleWithFixedCreated — with created fixed to
// one value per logical Put, packing the same annotations twice yields the
// same manifest digest (ORAS PackManifest would otherwise stamp the wall
// clock per call).
func TestPackStateManifestReproducibleWithFixedCreated(t *testing.T) {
	ctx := context.Background()
	fake := newFakeORASRepo()
	repo := &orasRepositoryClient{inner: fake, repository: "example.com/test/repo"}
	wc := newRemoteClient(repo, "default")

	data := "reproducible-state"
	layerDesc := ocispec.Descriptor{
		MediaType: mediaTypeStateLayer,
		Digest:    digest.FromBytes([]byte(data)),
		Size:      int64(len(data)),
	}
	if err := fake.Push(ctx, layerDesc, bytes.NewReader([]byte(data))); err != nil {
		t.Fatalf("push layer: %v", err)
	}
	annotations := map[string]string{
		annotationWorkspace:    "default",
		annotationUpdatedAt:    "2026-01-01T00:00:00Z",
		annotationCreated:      "2026-01-01T00:00:00Z",
		annotationStateVersion: "1",
	}
	d1, err := wc.packStateManifestAnnotated(ctx, []ocispec.Descriptor{layerDesc}, annotations)
	if err != nil {
		t.Fatalf("pack 1: %v", err)
	}
	d2, err := wc.packStateManifestAnnotated(ctx, []ocispec.Descriptor{layerDesc}, annotations)
	if err != nil {
		t.Fatalf("pack 2: %v", err)
	}
	if d1.Digest != d2.Digest {
		t.Fatalf("digests differ across identical packs: %s vs %s", d1.Digest, d2.Digest)
	}
}

// TestPut_HistoryPreparationFailureLeavesCurrentUntouched — H is prepared
// before any mutable tag is published: a history-pack failure returns an
// error and leaves the current tag exactly as it was.
func TestPut_HistoryPreparationFailureLeavesCurrentUntouched(t *testing.T) {
	ctx := context.Background()
	fake := newFakeORASRepo()
	hook := &historyPackFailRepo{delegatingRepo: delegatingRepo{inner: fake}}
	repo := &orasRepositoryClient{inner: hook, repository: "example.com/test/repo"}
	c := newTestClient(repo)
	c.config.MaxVersions = 3
	wc := newRemoteClient(repo, "default")

	if err := c.Put(ctx, "default", []byte("state-v1")); !errors.Is(err, errHistoryPackFailed) {
		t.Fatalf("Put error = %v, want the history-preparation sentinel", err)
	}
	if _, err := fake.Resolve(ctx, wc.stateTag); !errors.Is(err, errdef.ErrNotFound) {
		t.Errorf("state tag resolve err = %v, want absent (H preparation failure must leave current untouched)", err)
	}
}

// TestRetention_DeletesOnlyVerifiedHistoryDigests — deterministic public
// schedule: with MaxVersions=1, the second Put's retention deletes exactly
// the verified history manifest of v1 and NEVER a current manifest digest;
// current state and retained history stay readable.
func TestRetention_DeletesOnlyVerifiedHistoryDigests(t *testing.T) {
	ctx := context.Background()
	fake := newFakeORASRepo()
	track := &deleteTrackingRepo{delegatingRepo: delegatingRepo{inner: fake}}
	repo := &orasRepositoryClient{inner: track, repository: "example.com/test/repo"}
	c := newTestClient(repo)
	c.config.MaxVersions = 1
	wc := newRemoteClient(repo, "default")

	if err := c.Put(ctx, "default", []byte("v1")); err != nil {
		t.Fatalf("put v1: %v", err)
	}
	c.WaitForRetention()
	currentV1 := mustResolve(ctx, t, fake, wc.stateTag)
	// Capture the history digest now: after the second Put, its async
	// retention may already have deleted this tag.
	historyV1 := mustResolve(ctx, t, fake, wc.versionTagFor(1))

	if err := c.Put(ctx, "default", []byte("v2")); err != nil {
		t.Fatalf("put v2: %v", err)
	}
	currentV2 := mustResolve(ctx, t, fake, wc.stateTag)
	if currentV1.Digest == historyV1.Digest || currentV2.Digest == historyV1.Digest {
		t.Fatal("test setup: current and history digests must differ")
	}
	c.WaitForRetention()

	// Retention ran to completion: exactly one delete, of the verified
	// history digest, never a current manifest digest.
	if len(track.deleted) != 1 || track.deleted[0] != historyV1.Digest {
		t.Fatalf("deleted digests = %v, want exactly [%s]", track.deleted, historyV1.Digest)
	}
	for name, d := range map[string]digest.Digest{"current v1": currentV1.Digest, "current v2": currentV2.Digest} {
		if _, ok := fake.blobs[d]; !ok {
			t.Errorf("%s manifest blob was deleted; retention must never delete a current manifest digest", name)
		}
	}

	// Current state and retained history remain intact.
	if got, err := c.Get(ctx, "default"); err != nil || string(got) != "v2" {
		t.Errorf("Get = (%q, %v), want %q", got, err, "v2")
	}
	if got, err := c.GetVersion(ctx, "default", 2); err != nil || string(got) != "v2" {
		t.Errorf("GetVersion(2) = (%q, %v), want %q", got, err, "v2")
	}
	if _, err := c.GetVersion(ctx, "default", 1); !errors.Is(err, ErrStateVersionNotFound) {
		t.Errorf("GetVersion(1) = %v, want ErrStateVersionNotFound", err)
	}
}

// TestRetention_PreservesLegacyUnmarkedAndSharedAliases — retention deletes
// ONLY exact verified history manifests: unmarked legacy manifests, unknown
// artifact types, and marked-but-untyped manifests are preserved; a digest
// group shared between a retired and a retained version tag (ambiguous alias
// group) is preserved wholesale; no retagging occurs. The current manifest
// digest is never a deletion candidate.
func TestRetention_PreservesLegacyUnmarkedAndSharedAliases(t *testing.T) {
	ctx := context.Background()
	fake := newFakeORASRepo()
	track := &deleteTrackingRepo{delegatingRepo: delegatingRepo{inner: fake}}
	repo := &orasRepositoryClient{inner: track, repository: "example.com/test/repo"}
	// enforceVersionRetention reads the config of wc's own client.
	wc := newRemoteClient(repo, "default")
	wc.client.config.MaxVersions = 2

	// Versions [1..7] with MaxVersions=2: delete set {1,2,3,4,5}, keep {6,7}.
	// v1: legacy unmarked manifest (old writers published one manifest for
	// current and history).
	v1Desc := seedVersionManifest(ctx, t, fake, wc, 1, "legacy-v1", false, artifactTypeState)
	// v2: recognized marker but foreign artifact type.
	v2Desc := seedVersionManifest(ctx, t, fake, wc, 2, "foreign-v2", true, "application/vnd.foreign")
	// v3: recognized marker but UNTYPED manifest — not a verifiable history
	// manifest, so it must be preserved.
	v3Desc := seedVersionManifest(ctx, t, fake, wc, 3, "untyped-v3", true, "")
	// v4: exact verified history manifest — the only deletion candidate.
	v4Desc := seedVersionManifest(ctx, t, fake, wc, 4, "marked-v4", true, artifactTypeState)
	// v5+v6: one manifest aliased under a retired (v5) and a retained (v6)
	// version tag — an ambiguous alias group.
	sharedDesc := seedVersionManifest(ctx, t, fake, wc, 5, "shared-v5", true, artifactTypeState)
	if err := fake.Tag(ctx, sharedDesc, wc.versionTagFor(6)); err != nil {
		t.Fatalf("tag shared alias v6: %v", err)
	}
	// v7: exact verified history manifest, but in the RETAINED set.
	seedVersionManifest(ctx, t, fake, wc, 7, "marked-v7", true, artifactTypeState)

	current := ocispec.Descriptor{Digest: digest.Digest("sha256:" + strings.Repeat("0", 64))}
	if err := wc.enforceVersionRetention(ctx, current, []int{1, 2, 3, 4, 5, 6, 7}); err != nil {
		t.Fatalf("enforceVersionRetention: %v", err)
	}

	if len(track.deleted) != 1 || track.deleted[0] != v4Desc.Digest {
		t.Fatalf("deleted digests = %v, want exactly [%s]", track.deleted, v4Desc.Digest)
	}
	// The verified history candidate is gone by exact digest; everything
	// else — unmarked, unknown, marked-but-untyped, shared aliases,
	// retained — is preserved.
	if _, err := fake.Resolve(ctx, wc.versionTagFor(4)); !errors.Is(err, errdef.ErrNotFound) {
		t.Errorf("verified history v4 still present, want deleted by exact digest")
	}
	for _, tc := range []struct {
		version int
		want    digest.Digest
	}{{1, v1Desc.Digest}, {2, v2Desc.Digest}, {3, v3Desc.Digest}, {5, sharedDesc.Digest}, {6, sharedDesc.Digest}} {
		got, err := fake.Resolve(ctx, wc.versionTagFor(tc.version))
		if err != nil {
			t.Errorf("version tag v%d was removed; unmarked/unknown/untyped/shared manifests must be preserved: %v", tc.version, err)
			continue
		}
		if got.Digest != tc.want {
			t.Errorf("version tag v%d = %s, want preserved digest %s", tc.version, got.Digest, tc.want)
		}
	}
	// The retained verified history manifest is untouched.
	if _, err := fake.Resolve(ctx, wc.versionTagFor(7)); err != nil {
		t.Errorf("retained history v7 must survive retention: %v", err)
	}
	if track.tagCalls != 0 {
		t.Errorf("Tag calls during retention = %d, want 0 (no retagging)", track.tagCalls)
	}
}

// retentionGateRepo gates the Delete of a specific digest (the old verified
// history manifest) after retention's final descriptor validation, giving
// the test a deterministic window; everything else delegates.
type retentionGateRepo struct {
	delegatingRepo
	gateDigest digest.Digest
	paused     chan struct{}
	release    chan struct{}

	mu       sync.Mutex
	gatedOne bool
}

func (r *retentionGateRepo) Delete(ctx context.Context, target ocispec.Descriptor) error {
	r.mu.Lock()
	match := target.Digest == r.gateDigest && !r.gatedOne
	if match {
		r.gatedOne = true
	}
	r.mu.Unlock()
	if match {
		close(r.paused)
		select {
		case <-r.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return r.delegatingRepo.Delete(ctx, target)
}

// TestRetention_DelayedWriterInterleavingKeepsCurrentReadable — deterministic
// interleaving: retention's delete of an old verified history manifest is
// gated AFTER final descriptor validation; while paused, a delayed writer
// publishes that state's distinct C manifest back under the state tag; after
// release, the state tag still resolves and Get still reads the current
// bytes — the exact-digest delete never touched the current manifest.
func TestRetention_DelayedWriterInterleavingKeepsCurrentReadable(t *testing.T) {
	ctx := context.Background()
	fake := newFakeORASRepo()
	gate := &retentionGateRepo{delegatingRepo: delegatingRepo{inner: fake}, paused: make(chan struct{}), release: make(chan struct{})}
	repo := &orasRepositoryClient{inner: gate, repository: "example.com/test/repo"}
	c := newTestClient(repo)
	c.config.MaxVersions = 1
	wc := newRemoteClient(repo, "default")

	if err := c.Put(ctx, "default", []byte("v1")); err != nil {
		t.Fatalf("put v1: %v", err)
	}
	c.WaitForRetention()
	currentV1 := mustResolve(ctx, t, fake, wc.stateTag)
	historyV1 := mustResolve(ctx, t, fake, wc.versionTagFor(1))
	if currentV1.Digest == historyV1.Digest {
		t.Fatal("test setup: current and history digests must be distinct")
	}
	// Arm the gate on the old verified history digest before the second Put
	// schedules its retention goroutine.
	gate.mu.Lock()
	gate.gateDigest = historyV1.Digest
	gate.mu.Unlock()

	if err := c.Put(ctx, "default", []byte("v2")); err != nil {
		t.Fatalf("put v2: %v", err)
	}
	historyV2 := mustResolve(ctx, t, fake, wc.versionTagFor(2))

	// Wait for retention to reach the gated delete (after it validated the
	// history descriptor), then run the delayed writer: republish v1's
	// distinct C manifest under the state tag.
	select {
	case <-gate.paused:
	case <-time.After(testObservationTimeout):
		t.Fatal("timed out waiting for retention to reach the gated delete")
	}
	if err := fake.Tag(ctx, currentV1, wc.stateTag); err != nil {
		t.Fatalf("delayed writer republish: %v", err)
	}
	close(gate.release)

	// Deterministic barrier: retention finished before assertions.
	c.WaitForRetention()

	// The state tag resolves to the delayed writer's manifest and Get still
	// reads the current bytes.
	got, err := fake.Resolve(ctx, wc.stateTag)
	if err != nil || got.Digest != currentV1.Digest {
		t.Errorf("state tag = (%v, %v), want the delayed writer's current manifest %s", got.Digest, err, currentV1.Digest)
	}
	if data, gerr := c.Get(ctx, "default"); gerr != nil || string(data) != "v1" {
		t.Errorf("Get = (%q, %v), want the current bytes %q", data, gerr, "v1")
	}
	// The old history was still retired; the retained history is intact.
	if _, rerr := fake.Resolve(ctx, wc.versionTagFor(1)); !errors.Is(rerr, errdef.ErrNotFound) {
		t.Errorf("old history v1 resolve err = %v, want deleted by exact digest", rerr)
	}
	if gotV2, rerr := fake.Resolve(ctx, wc.versionTagFor(2)); rerr != nil || gotV2.Digest != historyV2.Digest {
		t.Errorf("retained history v2 = (%v, %v), want %s", gotV2.Digest, rerr, historyV2.Digest)
	}
}

// TestRetention_IncompatibleAliasGroupNeverDeleted — deterministic regardless
// of map iteration: MaxVersions=1 over versions [1,2,3]; v1 and v2 are
// aliased to ONE history manifest marked v1. The marker agrees with v1 but
// not with v2, so the alias group is logically incompatible and the WHOLE
// digest group is preserved — nothing is deleted for it.
func TestRetention_IncompatibleAliasGroupNeverDeleted(t *testing.T) {
	ctx := context.Background()
	fake := newFakeORASRepo()
	track := &deleteTrackingRepo{delegatingRepo: delegatingRepo{inner: fake}}
	repo := &orasRepositoryClient{inner: track, repository: "example.com/test/repo"}
	wc := newRemoteClient(repo, "default")
	wc.client.config.MaxVersions = 1

	// One H manifest marked v1, aliased under BOTH stver-v1 and stver-v2.
	aliased := seedVersionManifest(ctx, t, fake, wc, 1, "aliased-v1", true, artifactTypeState)
	if err := fake.Tag(ctx, aliased, wc.versionTagFor(2)); err != nil {
		t.Fatalf("tag alias v2: %v", err)
	}
	// v3: verified marked history, in the RETAINED set.
	v3Desc := seedVersionManifest(ctx, t, fake, wc, 3, "marked-v3", true, artifactTypeState)

	current := ocispec.Descriptor{Digest: digest.Digest("sha256:" + strings.Repeat("0", 64))}
	if err := wc.enforceVersionRetention(ctx, current, []int{1, 2, 3}); err != nil {
		t.Fatalf("enforceVersionRetention: %v", err)
	}

	if len(track.deleted) != 0 {
		t.Fatalf("deleted digests = %v, want none (marker v1 disagrees with alias v2; the whole group must be preserved)", track.deleted)
	}
	for _, v := range []int{1, 2, 3} {
		want := map[int]digest.Digest{1: aliased.Digest, 2: aliased.Digest, 3: v3Desc.Digest}[v]
		if got, err := fake.Resolve(ctx, wc.versionTagFor(v)); err != nil || got.Digest != want {
			t.Errorf("version tag v%d = (%v, %v), want preserved digest %s", v, got.Digest, err, want)
		}
	}
}

// tagMoveTrackingRepo combines the delayed-writer double for retention: it
// moves the watched version tag to another descriptor immediately after the
// FIRST resolve observes it (a concurrent writer landing between retention's
// grouping and its validation), records every deleted digest, and counts Tag
// calls; everything else delegates.
type tagMoveTrackingRepo struct {
	delegatingRepo
	watch     string
	movedDesc ocispec.Descriptor

	mu       sync.Mutex
	resolves int
	deleted  []digest.Digest
	tagCalls int
}

func (r *tagMoveTrackingRepo) Resolve(ctx context.Context, reference string) (ocispec.Descriptor, error) {
	desc, err := r.delegatingRepo.Resolve(ctx, reference)
	if err != nil || reference != r.watch {
		return desc, err
	}
	r.mu.Lock()
	r.resolves++
	first := r.resolves == 1
	r.mu.Unlock()
	if first {
		if terr := r.delegatingRepo.Tag(ctx, r.movedDesc, r.watch); terr != nil {
			return ocispec.Descriptor{}, terr
		}
	}
	return desc, err
}

func (r *tagMoveTrackingRepo) Tag(ctx context.Context, desc ocispec.Descriptor, reference string) error {
	r.mu.Lock()
	r.tagCalls++
	r.mu.Unlock()
	return r.delegatingRepo.Tag(ctx, desc, reference)
}

func (r *tagMoveTrackingRepo) Delete(ctx context.Context, target ocispec.Descriptor) error {
	r.mu.Lock()
	r.deleted = append(r.deleted, target.Digest)
	r.mu.Unlock()
	return r.delegatingRepo.Delete(ctx, target)
}

// TestRetention_TagMovedAfterGroupingDeletesGroupedDigestOnly — regression:
// the version tag moves to another (valid, verified) descriptor right after
// retention's grouping resolve. Retention must fetch and delete ONLY the
// grouped digest it validated and never the tag's new target.
func TestRetention_TagMovedAfterGroupingDeletesGroupedDigestOnly(t *testing.T) {
	ctx := context.Background()
	fake := newFakeORASRepo()
	move := &tagMoveTrackingRepo{delegatingRepo: delegatingRepo{inner: fake}}
	repo := &orasRepositoryClient{inner: move, repository: "example.com/test/repo"}
	wc := newRemoteClient(repo, "default")
	wc.client.config.MaxVersions = 1

	// v1: the grouped verified H; seeding tags stver-v1 at it.
	oldH := seedVersionManifest(ctx, t, fake, wc, 1, "old-h1", true, artifactTypeState)
	// The moving writer publishes ANOTHER verified H for v1, which the tag
	// will point at after the move.
	newH := seedVersionManifest(ctx, t, fake, wc, 1, "new-h1", true, artifactTypeState)
	if err := fake.Tag(ctx, oldH, wc.versionTagFor(1)); err != nil {
		t.Fatalf("restore v1 tag: %v", err)
	}
	// v2: verified marked history, in the RETAINED set.
	v2Desc := seedVersionManifest(ctx, t, fake, wc, 2, "marked-v2", true, artifactTypeState)

	move.mu.Lock()
	move.watch = wc.versionTagFor(1)
	move.movedDesc = newH
	move.mu.Unlock()

	current := ocispec.Descriptor{Digest: digest.Digest("sha256:" + strings.Repeat("0", 64))}
	if err := wc.enforceVersionRetention(ctx, current, []int{1, 2}); err != nil {
		t.Fatalf("enforceVersionRetention: %v", err)
	}

	if len(move.deleted) != 1 || move.deleted[0] != oldH.Digest {
		t.Fatalf("deleted digests = %v, want exactly [%s] (the grouped digest; the moved tag's target must never be deleted)", move.deleted, oldH.Digest)
	}
	// The moved-to manifest is intact and still owns the tag.
	if _, ok := fake.blobs[newH.Digest]; !ok {
		t.Error("moved-to manifest blob was deleted; retention must fetch/delete only the grouped digest")
	}
	if got, err := fake.Resolve(ctx, wc.versionTagFor(1)); err != nil || got.Digest != newH.Digest {
		t.Errorf("version tag v1 = (%v, %v), want the new target %s", got.Digest, err, newH.Digest)
	}
	// The grouped digest itself was deleted; the retained version survives.
	if _, ok := fake.blobs[oldH.Digest]; ok {
		t.Error("grouped digest blob still present, want deleted")
	}
	if got, err := fake.Resolve(ctx, wc.versionTagFor(2)); err != nil || got.Digest != v2Desc.Digest {
		t.Errorf("retained v2 = (%v, %v), want %s", got.Digest, err, v2Desc.Digest)
	}
	if move.tagCalls != 0 {
		t.Errorf("Tag calls during retention = %d, want 0 (no retagging)", move.tagCalls)
	}
}

// TestGHCR_AutomaticRetentionDisabledHistoryReadable — automatic retention is
// disabled for ghcr.io: with MaxVersions=1 and multiple Puts, history is
// still published and readable and current is intact, with ZERO automatic
// retention Delete calls (the Packages-API fallback only runs from a Delete,
// so zero deletes also means zero Packages calls). No live registry test.
func TestGHCR_AutomaticRetentionDisabledHistoryReadable(t *testing.T) {
	ctx := context.Background()
	fake := newFakeORASRepo()
	ops := &countingOpsRepo{delegatingRepo: delegatingRepo{inner: fake}}
	c := newTestClient(&orasRepositoryClient{inner: ops, repository: "ghcr.io/owner/repo"})
	c.config.MaxVersions = 1

	if err := c.Put(ctx, "default", []byte("ghcr-v1")); err != nil {
		t.Fatalf("put v1: %v", err)
	}
	_, deletesAfterV1 := ops.counts()
	if err := c.Put(ctx, "default", []byte("ghcr-v2")); err != nil {
		t.Fatalf("put v2: %v", err)
	}
	tagsAfterPuts, deletesAfterPuts := ops.counts()
	c.WaitForRetention()

	tags, deletes := ops.counts()
	if deletes != 0 || deletesAfterV1 != 0 || deletesAfterPuts != 0 {
		t.Errorf("Delete calls = %d (v1: %d, after v2: %d), want 0 (no automatic retention on ghcr.io)", deletes, deletesAfterV1, deletesAfterPuts)
	}
	if tags != tagsAfterPuts || tags != 4 {
		t.Errorf("Tag calls = %d (right after publications: %d), want exactly 4 publication tags (2 per Put) and no retention tag cleanup", tags, tagsAfterPuts)
	}

	// History is still published and readable; current is intact.
	if got, err := c.Get(ctx, "default"); err != nil || string(got) != "ghcr-v2" {
		t.Errorf("Get = (%q, %v), want %q", got, err, "ghcr-v2")
	}
	if got, err := c.GetVersion(ctx, "default", 1); err != nil || string(got) != "ghcr-v1" {
		t.Errorf("GetVersion(1) = (%q, %v), want %q (history must not be disabled on ghcr.io)", got, err, "ghcr-v1")
	}
	if got, err := c.GetVersion(ctx, "default", 2); err != nil || string(got) != "ghcr-v2" {
		t.Errorf("GetVersion(2) = (%q, %v), want %q", got, err, "ghcr-v2")
	}
}

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
