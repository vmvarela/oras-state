// Tests for publishCurrentState: the current-state manifest (C) is published
// under its mutable tag with ONE tag-addressed manifest PUT when the
// repository implements ORAS ReferencePusher (PackManifestN), and falls back to
// the old content-addressed pack plus a separate tag PUT when it does not.
// Neither path is CAS, and the single PUT is never retried blindly: a
// transient failure is resolved by observing the tag, never by re-publishing.
package orasstate

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"sync"
	"testing"

	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/oras-project/oras-go/v3/errdef"
)

// referencePusherRepo is a fakeORASRepo that also implements ORAS
// ReferencePusher, so PackManifestN publishes a manifest under a tag with one
// PushReference call instead of a digest-addressed Push plus a separate Tag.
// It records what production code asked for, so the tests can prove exactly one
// tagged PUT and no digest-addressed current manifest.
type referencePusherRepo struct {
	*fakeORASRepo

	mu sync.Mutex
	// pushRefs are the references PushReference was called with.
	pushRefs []string
	// tags are the references Tag was called with.
	tags []string
	// manifestPushes are the digests of manifests pushed at a digest.
	manifestPushes []digest.Digest

	// failPushRefCount: the first N PushReference calls fail transiently.
	failPushRefCount int
	// landOnFailure: a failing PushReference stores the manifest first
	// (response-lost simulation).
	landOnFailure bool
	pushRefCalls  int
}

func (r *referencePusherRepo) Exists(ctx context.Context, target ocispec.Descriptor) (bool, error) {
	rc, err := r.Fetch(ctx, target)
	if err != nil {
		if errors.Is(err, errdef.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	_ = rc.Close()
	return true, nil
}

func (r *referencePusherRepo) Push(ctx context.Context, expected ocispec.Descriptor, content io.Reader) error {
	b, err := io.ReadAll(content)
	if err != nil {
		return err
	}
	r.mu.Lock()
	if expected.MediaType == ocispec.MediaTypeImageManifest {
		r.manifestPushes = append(r.manifestPushes, expected.Digest)
	}
	r.mu.Unlock()
	return r.fakeORASRepo.Push(ctx, expected, bytes.NewReader(b))
}

func (r *referencePusherRepo) Tag(ctx context.Context, desc ocispec.Descriptor, reference string) error {
	r.mu.Lock()
	r.tags = append(r.tags, reference)
	r.mu.Unlock()
	return r.fakeORASRepo.Tag(ctx, desc, reference)
}

func (r *referencePusherRepo) PushReference(ctx context.Context, expected ocispec.Descriptor, content io.Reader, reference string) error {
	b, err := io.ReadAll(content)
	if err != nil {
		return err
	}
	r.mu.Lock()
	r.pushRefCalls++
	r.pushRefs = append(r.pushRefs, reference)
	fail := r.pushRefCalls <= r.failPushRefCount
	land := fail && r.landOnFailure
	r.mu.Unlock()

	if fail && !land {
		return transientHTTPStatus(http.StatusInternalServerError)
	}
	if err := r.fakeORASRepo.Push(ctx, expected, bytes.NewReader(b)); err != nil {
		return err
	}
	if err := r.fakeORASRepo.Tag(ctx, expected, reference); err != nil {
		return err
	}
	if fail {
		return transientHTTPStatus(http.StatusInternalServerError)
	}
	return nil
}

// snapshot copies the recorded publication calls.
func (r *referencePusherRepo) snapshot() (pushRefs, tags []string, manifestPushes []digest.Digest) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.pushRefs...), append([]string(nil), r.tags...), append([]digest.Digest(nil), r.manifestPushes...)
}

func containsRef(refs []string, want string) bool {
	for _, ref := range refs {
		if ref == want {
			return true
		}
	}
	return false
}

func containsDigest(digests []digest.Digest, want digest.Digest) bool {
	for _, d := range digests {
		if d == want {
			return true
		}
	}
	return false
}

// TestPutCurrentStateSingleTaggedPutWithReferencePusher — with a repository
// that implements ReferencePusher, C reaches its mutable tag through exactly
// one tagged PUT: no digest-addressed current manifest and no separate Tag for
// the state tag. History publication is untouched (still a separate Tag).
func TestPutCurrentStateSingleTaggedPutWithReferencePusher(t *testing.T) {
	ctx := context.Background()
	fake := &referencePusherRepo{fakeORASRepo: newFakeORASRepo()}
	repo := &orasRepositoryClient{inner: fake, repository: "example.com/test/repo"}
	c := newTestClient(repo)
	c.config.MaxVersions = 2
	wc := newRemoteClient(repo, "default")

	if err := c.Put(ctx, "default", []byte("state-single-put")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	c.WaitForRetention()

	if got, err := c.Get(ctx, "default"); err != nil || string(got) != "state-single-put" {
		t.Fatalf("Get = (%q, %v), want the published state", got, err)
	}
	current := mustResolve(ctx, t, fake.fakeORASRepo, wc.stateTag)

	pushRefs, tags, manifestPushes := fake.snapshot()
	if len(pushRefs) != 1 || pushRefs[0] != wc.stateTag {
		t.Errorf("PushReference refs = %v, want exactly [%s]", pushRefs, wc.stateTag)
	}
	if containsRef(tags, wc.stateTag) {
		t.Errorf("Tag refs = %v, want no separate Tag for the current state tag", tags)
	}
	if containsDigest(manifestPushes, current.Digest) {
		t.Errorf("current manifest %s was pushed at its digest; the tagged publish must be a single PUT", current.Digest)
	}
	// History publication is unchanged: H is packed at its digest and tagged
	// separately.
	history := mustResolve(ctx, t, fake.fakeORASRepo, wc.versionTagFor(1))
	if !containsDigest(manifestPushes, history.Digest) {
		t.Errorf("history manifest %s was not pushed at its digest; history preparation must stay content-addressed", history.Digest)
	}
	if !containsRef(tags, wc.versionTagFor(1)) {
		t.Errorf("Tag refs = %v, want the history tag %s", tags, wc.versionTagFor(1))
	}
	if history.Digest == current.Digest {
		t.Error("current and history digests must differ")
	}
}

// TestPutCurrentStateResponseLostPushReferenceNotReapplied — a transient
// tagged-PUT failure whose publication DID land is treated as success by
// observing the tag with the pre-computed expected digest: exactly one
// PushReference, never a second one.
func TestPutCurrentStateResponseLostPushReferenceNotReapplied(t *testing.T) {
	ctx := context.Background()
	fake := &referencePusherRepo{fakeORASRepo: newFakeORASRepo(), failPushRefCount: 1, landOnFailure: true}
	repo := &orasRepositoryClient{inner: fake, repository: "example.com/test/repo"}
	c := newTestClient(repo)

	if err := c.Put(ctx, "default", []byte("state-response-lost")); err != nil {
		t.Fatalf("response-lost publication not confirmed as success: %v", err)
	}
	if got, err := c.Get(ctx, "default"); err != nil || string(got) != "state-response-lost" {
		t.Fatalf("Get = (%q, %v)", got, err)
	}
	pushRefs, _, _ := fake.snapshot()
	if len(pushRefs) != 1 {
		t.Errorf("PushReference calls = %d (%v), want exactly 1 (observation must not re-publish)", len(pushRefs), pushRefs)
	}
}

// TestPutCurrentStateFailedPushReferenceFailsClosed — a transient tagged-PUT
// failure that did NOT land, with the tag pointing at another manifest, fails
// closed with ErrMutableTagMoved and is never re-applied.
func TestPutCurrentStateFailedPushReferenceFailsClosed(t *testing.T) {
	ctx := context.Background()
	fake := &referencePusherRepo{fakeORASRepo: newFakeORASRepo(), failPushRefCount: 1}
	repo := &orasRepositoryClient{inner: fake, repository: "example.com/test/repo"}
	c := newTestClient(repo)
	wc := newRemoteClient(repo, "default")

	// An existing current state for this workspace (so the preflight passes).
	seeded, err := wc.packStateManifestAnnotated(ctx, nil, map[string]string{
		annotationWorkspace: "default",
		annotationCreated:   "2026-01-01T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("seed manifest: %v", err)
	}
	if err := fake.fakeORASRepo.Tag(ctx, seeded, wc.stateTag); err != nil {
		t.Fatalf("seed tag: %v", err)
	}

	err = c.Put(ctx, "default", []byte("state-lost"))
	if !errors.Is(err, ErrMutableTagMoved) {
		t.Fatalf("Put error = %v, want errors.Is(err, ErrMutableTagMoved)", err)
	}
	pushRefs, _, _ := fake.snapshot()
	if len(pushRefs) != 1 {
		t.Errorf("PushReference calls = %d, want exactly 1 (fail closed, no re-publish)", len(pushRefs))
	}
	if got := mustResolve(ctx, t, fake.fakeORASRepo, wc.stateTag); got.Digest != seeded.Digest {
		t.Errorf("state tag = %s, want the seeded holder %s", got.Digest, seeded.Digest)
	}
}

// digestPackTrackingRepo records digest-addressed manifest pushes and tag
// writes. It deliberately does NOT implement ReferencePusher, so the fallback
// path is exercised.
type digestPackTrackingRepo struct {
	delegatingRepo

	mu             sync.Mutex
	manifestPushes []digest.Digest
	tags           []string
}

func (r *digestPackTrackingRepo) Push(ctx context.Context, expected ocispec.Descriptor, content io.Reader) error {
	b, err := io.ReadAll(content)
	if err != nil {
		return err
	}
	r.mu.Lock()
	if expected.MediaType == ocispec.MediaTypeImageManifest {
		r.manifestPushes = append(r.manifestPushes, expected.Digest)
	}
	r.mu.Unlock()
	return r.inner.Push(ctx, expected, bytes.NewReader(b))
}

func (r *digestPackTrackingRepo) Tag(ctx context.Context, desc ocispec.Descriptor, reference string) error {
	r.mu.Lock()
	r.tags = append(r.tags, reference)
	r.mu.Unlock()
	return r.inner.Tag(ctx, desc, reference)
}

// TestPutCurrentStateFallbackIsNotOnePut — without ReferencePusher the current
// state still publishes correctly, but through TWO operations (digest pack +
// separate tag PUT). The fallback must not pretend to be a single PUT.
func TestPutCurrentStateFallbackIsNotOnePut(t *testing.T) {
	ctx := context.Background()
	fake := newFakeORASRepo()
	track := &digestPackTrackingRepo{delegatingRepo: delegatingRepo{inner: fake}}
	repo := &orasRepositoryClient{inner: track, repository: "example.com/test/repo"}
	c := newTestClient(repo)
	wc := newRemoteClient(repo, "default")

	if err := c.Put(ctx, "default", []byte("state-fallback")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if got, err := c.Get(ctx, "default"); err != nil || string(got) != "state-fallback" {
		t.Fatalf("Get = (%q, %v)", got, err)
	}
	current := mustResolve(ctx, t, fake, wc.stateTag)

	track.mu.Lock()
	defer track.mu.Unlock()
	if !containsDigest(track.manifestPushes, current.Digest) {
		t.Errorf("current manifest %s was not pushed at its digest; the fallback must keep the content-addressed pack", current.Digest)
	}
	if !containsRef(track.tags, wc.stateTag) {
		t.Errorf("Tag refs = %v, want the separate current tag PUT %s", track.tags, wc.stateTag)
	}
}

// TestPublishCurrentStateSingleManifestPUTOverHTTP — the pinned ORAS remote
// repository against a minimal OCI Distribution fixture: publishing C issues
// exactly ONE manifest PUT, at the mutable state tag, with no digest-addressed
// PUT and no digest GET (PushReference has no pre-push fetch).
func TestPublishCurrentStateSingleManifestPUTOverHTTP(t *testing.T) {
	ctx := context.Background()
	s, srv := newTagRetryServer(t)
	wc := newTagRetryRepo(t, srv)

	layerData := []byte("state-layer")
	layerDesc := ocispec.Descriptor{
		MediaType: mediaTypeStateLayer,
		Digest:    digest.FromBytes(layerData),
		Size:      int64(len(layerData)),
	}
	desc, err := wc.publishCurrentState(ctx, []ocispec.Descriptor{layerDesc}, map[string]string{
		annotationWorkspace: "default",
		annotationUpdatedAt: "2026-01-01T00:00:00Z",
		annotationCreated:   "2026-01-01T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("publishCurrentState: %v", err)
	}

	reqs := s.requests()
	if n := countRequests(reqs, http.MethodPut, false); n != 1 {
		t.Errorf("tag-addressed manifest PUTs = %d, want exactly 1", n)
	}
	if n := countRequests(reqs, http.MethodPut, true); n != 0 {
		t.Errorf("digest-addressed manifest PUTs = %d, want 0", n)
	}
	if n := countRequests(reqs, http.MethodGet, true); n != 0 {
		t.Errorf("digest GETs = %d, want 0 (a tagged PUT needs no pre-push fetch)", n)
	}
	for _, req := range reqs {
		if req.Method == http.MethodPut && req.Reference != wc.stateTag {
			t.Errorf("manifest PUT reference = %q, want only the state tag %q", req.Reference, wc.stateTag)
		}
	}
	if got := s.tagDigest(wc.stateTag); got != desc.Digest.String() {
		t.Errorf("tag digest = %s, want %s", got, desc.Digest)
	}
}
