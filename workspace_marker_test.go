// Tests for the prospective workspace witness (wsmeta-<workspaceTag>).
// The marker is a witness, not a fence or a transaction: these tests cover its
// publication order, fail-closed reads, explicit reset, cleanup order, listing,
// validation, and the absence of extra calls on the common paths.
package orasstate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/oras-project/oras-go/v3/errdef"
	orasProperties "github.com/oras-project/oras-go/v3/registry/remote/properties"
)

// markerTestRepo records successful Tag/Delete order and can fail one tag
// reference, one delete digest, or reverse the tag listing.
type markerTestRepo struct {
	delegatingRepo

	mu               sync.Mutex
	tagOrder         []string
	deleteOrder      []digest.Digest
	failTagRef       string
	failTagErr       error
	failDeleteDigest digest.Digest
	reverseTags      bool
}

func (r *markerTestRepo) Tag(ctx context.Context, desc ocispec.Descriptor, reference string) error {
	if reference == r.failTagRef && r.failTagErr != nil {
		return r.failTagErr
	}
	if err := r.delegatingRepo.Tag(ctx, desc, reference); err != nil {
		return err
	}
	r.mu.Lock()
	r.tagOrder = append(r.tagOrder, reference)
	r.mu.Unlock()
	return nil
}

func (r *markerTestRepo) Delete(ctx context.Context, target ocispec.Descriptor) error {
	if r.failDeleteDigest != "" && target.Digest == r.failDeleteDigest {
		return errors.New("delete denied (test sentinel)")
	}
	if err := r.delegatingRepo.Delete(ctx, target); err != nil {
		return err
	}
	r.mu.Lock()
	r.deleteOrder = append(r.deleteOrder, target.Digest)
	r.mu.Unlock()
	return nil
}

func (r *markerTestRepo) Tags(ctx context.Context, last string, fn func(tags []string) error) error {
	if !r.reverseTags {
		return r.delegatingRepo.Tags(ctx, last, fn)
	}
	return r.delegatingRepo.Tags(ctx, last, func(tags []string) error {
		reversed := slices.Clone(tags)
		slices.Reverse(reversed)
		return fn(reversed)
	})
}

func (r *markerTestRepo) tags() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.tagOrder)
}

func (r *markerTestRepo) deletes() []digest.Digest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.deleteOrder)
}

func (r *markerTestRepo) countTag(reference string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, tag := range r.tagOrder {
		if tag == reference {
			n++
		}
	}
	return n
}

func TestWorkspaceMarkerPublishedBeforeState(t *testing.T) {
	ctx := context.Background()
	repo := &markerTestRepo{delegatingRepo: delegatingRepo{inner: newFakeORASRepo()}}
	c := newTestClient(&orasRepositoryClient{inner: repo})
	c.config.MaxVersions = 1
	wc := newWorkspaceClient(c, "default")

	if err := c.Put(ctx, "default", []byte("state")); err != nil {
		t.Fatalf("put: %v", err)
	}

	order := repo.tags()
	markerAt := slices.Index(order, wc.markerTag)
	stateAt := slices.Index(order, wc.stateTag)
	if markerAt < 0 || stateAt < 0 || markerAt > stateAt {
		t.Fatalf("tag order = %v, want marker %q before state %q", order, wc.markerTag, wc.stateTag)
	}

	m, _, err := wc.fetchManifestWithDesc(ctx, wc.markerTag)
	if err != nil {
		t.Fatalf("fetch marker: %v", err)
	}
	if err := validateWorkspaceMarker(m, wc.markerTag, "default"); err != nil {
		t.Fatalf("marker validation: %v", err)
	}
	if len(m.Layers) != 0 {
		t.Fatalf("marker layers = %d, want 0", len(m.Layers))
	}

	// Deterministic identity: repacking the same name yields the same digest.
	again, err := wc.packWorkspaceMarker(ctx)
	if err != nil {
		t.Fatalf("repack marker: %v", err)
	}
	if got := mustResolve(ctx, t, repo.inner, wc.markerTag); got.Digest != again.Digest {
		t.Fatalf("marker digest = %s, want deterministic %s", got.Digest, again.Digest)
	}
}

func TestWorkspaceMarkerFailurePublishesNoState(t *testing.T) {
	ctx := context.Background()
	wc := newWorkspaceClient(newTestClient(&orasRepositoryClient{inner: newFakeORASRepo()}), "default")

	t.Run("non-transient", func(t *testing.T) {
		fake := newFakeORASRepo()
		repo := &markerTestRepo{
			delegatingRepo: delegatingRepo{inner: fake},
			failTagRef:     wc.markerTag,
			failTagErr:     errors.New("marker denied"),
		}
		c := newTestClient(&orasRepositoryClient{inner: repo})
		err := c.Put(ctx, "default", []byte("state"))
		if err == nil || !strings.Contains(err.Error(), "workspace marker") {
			t.Fatalf("put = %v, want marker publication error", err)
		}
		if _, err := fake.Resolve(ctx, wc.stateTag); !errors.Is(err, errdef.ErrNotFound) {
			t.Fatalf("state tag resolve = %v, want absent", err)
		}
		if _, err := fake.Resolve(ctx, wc.markerTag); !errors.Is(err, errdef.ErrNotFound) {
			t.Fatalf("marker tag resolve = %v, want absent", err)
		}
	})

	t.Run("ambiguous", func(t *testing.T) {
		fake := newFakeORASRepo()
		hook := &hookingRepo{inner: fake, watch: wc.markerTag, failTagCount: 1}
		c := newTestClient(&orasRepositoryClient{inner: hook})
		err := c.Put(ctx, "default", []byte("state"))
		if err == nil || !errors.Is(err, ErrMutableTagMoved) {
			t.Fatalf("put = %v, want fail-closed ErrMutableTagMoved", err)
		}
		if _, err := fake.Resolve(ctx, wc.stateTag); !errors.Is(err, errdef.ErrNotFound) {
			t.Fatalf("state tag resolve = %v, want absent", err)
		}
	})
}

func TestWorkspacePutOversizedPublishesNoMarker(t *testing.T) {
	ctx := context.Background()
	fake := newFakeORASRepo()
	c := newTestClient(&orasRepositoryClient{inner: fake})
	c.config.MaxStateSize = 10
	wc := newWorkspaceClient(c, "default")

	err := c.Put(ctx, "default", []byte("this state is longer than ten bytes"))
	if err == nil || !strings.Contains(err.Error(), "exceeds maximum allowed size") {
		t.Fatalf("Put oversized = %v, want size error", err)
	}
	if len(fake.tags) != 0 || len(fake.blobs) != 0 {
		t.Fatalf("oversized Put wrote to repo: %d blobs, %d tags", len(fake.blobs), len(fake.tags))
	}
	if _, err := fake.Resolve(ctx, wc.markerTag); !errors.Is(err, errdef.ErrNotFound) {
		t.Fatalf("oversized Put published a marker: %v", err)
	}
	if _, err := fake.Resolve(ctx, wc.stateTag); !errors.Is(err, errdef.ErrNotFound) {
		t.Fatalf("oversized Put published state: %v", err)
	}
}

// markerAfterListingRepo publishes the marker only after the tag listing has
// been returned, simulating a concurrent initializer that wins between
// checkWorkspace and the state read.
type markerAfterListingRepo struct {
	delegatingRepo
	once sync.Once
	desc ocispec.Descriptor
	tag  string
}

func (r *markerAfterListingRepo) Tags(ctx context.Context, last string, fn func([]string) error) error {
	if err := r.delegatingRepo.Tags(ctx, last, fn); err != nil {
		return err
	}
	r.once.Do(func() {
		_ = r.delegatingRepo.Tag(context.Background(), r.desc, r.tag)
	})
	return nil
}

func TestWorkspaceGetMarkerAppearsAfterObservation(t *testing.T) {
	ctx := context.Background()
	fake := newFakeORASRepo()
	base := newTestClient(&orasRepositoryClient{inner: fake})
	wc := newWorkspaceClient(base, "default")
	markerDesc, err := wc.packWorkspaceMarker(ctx)
	if err != nil {
		t.Fatalf("pack marker: %v", err)
	}

	repo := &markerAfterListingRepo{
		delegatingRepo: delegatingRepo{inner: fake},
		desc:           markerDesc,
		tag:            wc.markerTag,
	}
	c := newTestClient(&orasRepositoryClient{inner: repo})
	data, err := c.Get(ctx, "default")
	if err == nil || !strings.Contains(err.Error(), "workspace marker") {
		t.Fatalf("Get = (%q, %v), want fail-closed marker error for a marker published after the listing", data, err)
	}
}

func TestWorkspacePutRejectsCorruptMarkerAlongsideState(t *testing.T) {
	ctx := context.Background()
	baseMarker := func() ocispec.Manifest {
		return ocispec.Manifest{
			MediaType:    ocispec.MediaTypeImageManifest,
			ArtifactType: artifactTypeWorkspaceMarker,
			Layers:       []ocispec.Descriptor{},
			Annotations: map[string]string{
				annotationWorkspace: "default",
				annotationCreated:   workspaceMarkerCreated,
			},
		}
	}
	cases := map[string]func(*ocispec.Manifest){
		"type": func(m *ocispec.Manifest) { m.ArtifactType = artifactTypeState },
		"layers": func(m *ocispec.Manifest) {
			m.Layers = []ocispec.Descriptor{{MediaType: mediaTypeStateLayer, Digest: digest.FromString("x"), Size: 1}}
		},
	}
	for name, corrupt := range cases {
		t.Run(name, func(t *testing.T) {
			fake := newFakeORASRepo()
			c := newTestClient(&orasRepositoryClient{inner: fake})
			wc := newWorkspaceClient(c, "default")
			if err := c.Put(ctx, "default", []byte("old")); err != nil {
				t.Fatalf("seed put: %v", err)
			}
			oldState := mustResolve(ctx, t, fake, wc.stateTag)
			m := baseMarker()
			corrupt(&m)
			badMarker := pushMarkerManifest(t, fake, wc.markerTag, m)

			err := c.Put(ctx, "default", []byte("new"))
			if err == nil || (!strings.Contains(err.Error(), "artifactType") && !strings.Contains(err.Error(), "layers")) {
				t.Fatalf("Put with corrupt marker = %v, want marker validation error", err)
			}
			if got, err := fake.Resolve(ctx, wc.stateTag); err != nil || got.Digest != oldState.Digest {
				t.Fatal("state was published despite a corrupt marker")
			}
			if got, err := fake.Resolve(ctx, wc.markerTag); err != nil || got.Digest != badMarker.Digest {
				t.Fatal("corrupt marker was overwritten")
			}
		})
	}
}

func TestWorkspaceDeleteWorkspaceDefersMarkerAliasUntilFinalCleanup(t *testing.T) {
	ctx := context.Background()
	fake := newFakeORASRepo()
	repo := &markerTestRepo{delegatingRepo: delegatingRepo{inner: fake}}
	c := newTestClient(&orasRepositoryClient{inner: repo})
	wc := newWorkspaceClient(c, "default")
	if err := c.Put(ctx, "default", []byte("state")); err != nil {
		t.Fatalf("put: %v", err)
	}
	markerDesc := mustResolve(ctx, t, fake, wc.markerTag)
	stateDesc := mustResolve(ctx, t, fake, wc.stateTag)

	// An owned tag (the lock tag) aliases the exact pinned marker digest.
	if err := fake.Tag(ctx, markerDesc, wc.lockTag); err != nil {
		t.Fatalf("tag marker alias: %v", err)
	}

	// The state deletion runs after the aliased lock tag and fails: the
	// witness and its alias must survive, and the marker digest must not have
	// been deleted by the alias.
	repo.failDeleteDigest = stateDesc.Digest
	if err := c.DeleteWorkspace(ctx, "default"); err == nil {
		t.Fatal("DeleteWorkspace with a later delete failure = nil, want error")
	}
	for name, tag := range map[string]string{"marker": wc.markerTag, "alias": wc.lockTag} {
		got, err := fake.Resolve(ctx, tag)
		if err != nil || got.Digest != markerDesc.Digest {
			t.Fatalf("%s tag after failed cleanup = (%v, %v), want witness intact", name, got.Digest, err)
		}
	}
	for _, d := range repo.deletes() {
		if d == markerDesc.Digest {
			t.Fatal("marker digest was deleted before the final cleanup")
		}
	}

	repo.failDeleteDigest = ""
	if err := c.DeleteWorkspace(ctx, "default"); err != nil {
		t.Fatalf("retried DeleteWorkspace: %v", err)
	}
	deletes := repo.deletes()
	if len(deletes) != 2 || deletes[len(deletes)-1] != markerDesc.Digest {
		t.Fatalf("delete order = %v, want state then the marker digest last", deletes)
	}
	markerDeletes := 0
	for _, d := range deletes {
		if d == markerDesc.Digest {
			markerDeletes++
		}
	}
	if markerDeletes != 1 {
		t.Fatalf("marker digest deleted %d times, want exactly once at the final step", markerDeletes)
	}
	for _, tag := range []string{wc.stateTag, wc.lockTag, wc.markerTag} {
		if _, err := fake.Resolve(ctx, tag); !errors.Is(err, errdef.ErrNotFound) {
			t.Fatalf("tag %q after final cleanup = %v, want absent", tag, err)
		}
	}
}

func TestWorkspaceDeleteWorkspaceLegacyPreservesMarkerShapedTag(t *testing.T) {
	ctx := context.Background()
	fake := newFakeORASRepo()
	c := newTestClient(&orasRepositoryClient{inner: fake})
	c.config.WorkspaceLayout = WorkspaceLayoutLegacy
	wc := newWorkspaceClient(c, "default")
	if err := c.Put(ctx, "default", []byte("state")); err != nil {
		t.Fatalf("legacy put: %v", err)
	}
	unrelated, _ := newForeignManifest(ctx, t, fake)
	if err := fake.Tag(ctx, unrelated, wc.markerTag); err != nil {
		t.Fatalf("tag unrelated wsmeta-shaped object: %v", err)
	}

	if err := c.DeleteWorkspace(ctx, "default"); err != nil {
		t.Fatalf("legacy DeleteWorkspace: %v", err)
	}
	if _, err := fake.Resolve(ctx, wc.stateTag); !errors.Is(err, errdef.ErrNotFound) {
		t.Fatalf("legacy state tag = %v, want deleted", err)
	}
	got, err := fake.Resolve(ctx, wc.markerTag)
	if err != nil || got.Digest != unrelated.Digest {
		t.Fatalf("legacy DeleteWorkspace touched unrelated tag %q: (%v, %v)", wc.markerTag, got.Digest, err)
	}
}

func TestWorkspaceMarkerOnlyFailsClosedButRecoveryUsable(t *testing.T) {
	ctx := context.Background()
	fake := newFakeORASRepo()
	c := newTestClient(&orasRepositoryClient{inner: fake})
	wc := newWorkspaceClient(c, "default")
	if err := wc.createWorkspaceMarker(ctx); err != nil {
		t.Fatalf("create marker: %v", err)
	}

	for name, op := range map[string]func() error{
		"get":  func() error { _, err := c.Get(ctx, "default"); return err },
		"put":  func() error { return c.Put(ctx, "default", []byte("must not land")) },
		"lock": func() error { _, err := c.Lock(ctx, "default", LockInfo{ID: "new"}); return err },
	} {
		err := op()
		if err == nil || !strings.Contains(err.Error(), "workspace marker") {
			t.Errorf("%s on marker-only state = %v, want fail-closed marker error", name, err)
		}
	}
	if _, err := fake.Resolve(ctx, wc.stateTag); !errors.Is(err, errdef.ErrNotFound) {
		t.Fatalf("rejected Put/Lock published state: %v", err)
	}

	// Recovery paths stay usable.
	if err := c.Unlock(ctx, "default", "old"); err != nil {
		t.Fatalf("unlock on marker-only state: %v", err)
	}
	if err := c.VerifyLock(ctx, "default", "old"); err == nil || !strings.Contains(err.Error(), "no longer exists") {
		t.Fatalf("verify lock = %v, want the ordinary missing-lock error", err)
	}
	if versions, err := c.ListVersions(ctx, "default"); err != nil || len(versions) != 0 {
		t.Fatalf("ListVersions = %v, %v; want usable empty history", versions, err)
	}
	if _, err := c.GetVersion(ctx, "default", 1); !errors.Is(err, ErrStateVersionNotFound) {
		t.Fatalf("GetVersion = %v, want ErrStateVersionNotFound", err)
	}
	names, err := c.List(ctx)
	if err != nil || !slices.Equal(names, []string{"default"}) {
		t.Fatalf("List = %v, %v; want marker-only identity", names, err)
	}

	// Delete is the explicit reset.
	if err := c.Delete(ctx, "default"); err != nil {
		t.Fatalf("delete marker-only state: %v", err)
	}
	if _, err := fake.Resolve(ctx, wc.markerTag); !errors.Is(err, errdef.ErrNotFound) {
		t.Fatalf("marker after Delete = %v, want absent", err)
	}
	if data, err := c.Get(ctx, "default"); err != nil || data != nil {
		t.Fatalf("Get after reset = (%q, %v), want nil, nil", data, err)
	}
}

func TestWorkspaceExistingUnmarkedStateAdopted(t *testing.T) {
	ctx := context.Background()
	fake := newFakeORASRepo()
	c := newTestClient(&orasRepositoryClient{inner: fake})
	wc := newWorkspaceClient(c, "default")

	// Seed state without a marker, as an older writer would.
	if err := wc.put(ctx, []byte("old")); err != nil {
		t.Fatalf("seed unmarked state: %v", err)
	}
	if _, err := fake.Resolve(ctx, wc.markerTag); !errors.Is(err, errdef.ErrNotFound) {
		t.Fatalf("seed unexpectedly created a marker: %v", err)
	}
	if data, err := c.Get(ctx, "default"); err != nil || string(data) != "old" {
		t.Fatalf("Get unmarked state = (%q, %v)", data, err)
	}

	if err := c.Put(ctx, "default", []byte("new")); err != nil {
		t.Fatalf("adopting Put: %v", err)
	}
	if _, err := fake.Resolve(ctx, wc.markerTag); err != nil {
		t.Fatalf("adopting Put did not create the marker: %v", err)
	}
	if data, err := c.Get(ctx, "default"); err != nil || string(data) != "new" {
		t.Fatalf("Get after adoption = (%q, %v)", data, err)
	}
}

func TestWorkspaceDeleteMarkerLastAndRetainsOnFailure(t *testing.T) {
	ctx := context.Background()
	fake := newFakeORASRepo()
	repo := &markerTestRepo{delegatingRepo: delegatingRepo{inner: fake}}
	c := newTestClient(&orasRepositoryClient{inner: repo})
	wc := newWorkspaceClient(c, "default")
	if err := c.Put(ctx, "default", []byte("state")); err != nil {
		t.Fatalf("put: %v", err)
	}
	stateDesc := mustResolve(ctx, t, fake, wc.stateTag)
	markerDesc := mustResolve(ctx, t, fake, wc.markerTag)

	repo.failDeleteDigest = stateDesc.Digest
	if err := c.Delete(ctx, "default"); err == nil {
		t.Fatal("delete with failing state deletion = nil, want error")
	}
	if _, err := fake.Resolve(ctx, wc.markerTag); err != nil {
		t.Fatalf("marker was not retained after an earlier failure: %v", err)
	}

	repo.failDeleteDigest = ""
	if err := c.Delete(ctx, "default"); err != nil {
		t.Fatalf("retried delete: %v", err)
	}
	if got := repo.deletes(); len(got) != 2 || got[0] != stateDesc.Digest || got[1] != markerDesc.Digest {
		t.Fatalf("delete order = %v, want state %s then marker %s", got, stateDesc.Digest, markerDesc.Digest)
	}
	if _, err := fake.Resolve(ctx, wc.markerTag); !errors.Is(err, errdef.ErrNotFound) {
		t.Fatalf("marker after delete = %v, want absent", err)
	}

	// A failure deleting the marker itself is resumable: the state is already
	// gone, the marker is retained, and a retry finishes the reset.
	if err := c.Put(ctx, "default", []byte("state")); err != nil {
		t.Fatalf("re-put: %v", err)
	}
	markerDesc = mustResolve(ctx, t, fake, wc.markerTag)
	repo.failDeleteDigest = markerDesc.Digest
	if err := c.Delete(ctx, "default"); err == nil {
		t.Fatal("delete with failing marker deletion = nil, want error")
	}
	if _, err := fake.Resolve(ctx, wc.stateTag); !errors.Is(err, errdef.ErrNotFound) {
		t.Fatalf("state after marker-delete failure = %v, want already deleted", err)
	}
	if _, err := fake.Resolve(ctx, wc.markerTag); err != nil {
		t.Fatalf("marker after marker-delete failure = %v, want retained for retry", err)
	}
	repo.failDeleteDigest = ""
	if err := c.Delete(ctx, "default"); err != nil {
		t.Fatalf("retry marker deletion: %v", err)
	}
	if _, err := fake.Resolve(ctx, wc.markerTag); !errors.Is(err, errdef.ErrNotFound) {
		t.Fatalf("marker after retried delete = %v, want absent", err)
	}
}

func TestWorkspaceDeleteWorkspaceMarkerLastDespiteTagOrder(t *testing.T) {
	ctx := context.Background()
	fake := newFakeORASRepo()
	repo := &markerTestRepo{delegatingRepo: delegatingRepo{inner: fake}, reverseTags: true}
	c := newTestClient(&orasRepositoryClient{inner: repo})
	c.config.MaxVersions = 1
	wc := newWorkspaceClient(c, "default")

	if err := c.Put(ctx, "default", []byte("state")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if _, err := c.Lock(ctx, "default", LockInfo{ID: "holder"}); err != nil {
		t.Fatalf("lock: %v", err)
	}
	stateDesc := mustResolve(ctx, t, fake, wc.stateTag)
	markerDesc := mustResolve(ctx, t, fake, wc.markerTag)

	repo.failDeleteDigest = stateDesc.Digest
	if err := c.DeleteWorkspace(ctx, "default"); err == nil {
		t.Fatal("DeleteWorkspace with failing state deletion = nil, want error")
	}
	if _, err := fake.Resolve(ctx, wc.markerTag); err != nil {
		t.Fatalf("marker was not retained after an earlier failure: %v", err)
	}

	repo.failDeleteDigest = ""
	if err := c.DeleteWorkspace(ctx, "default"); err != nil {
		t.Fatalf("retried DeleteWorkspace: %v", err)
	}
	deletes := repo.deletes()
	if len(deletes) == 0 || deletes[len(deletes)-1] != markerDesc.Digest {
		t.Fatalf("delete order = %v, want marker %s last", deletes, markerDesc.Digest)
	}
	if _, err := fake.Resolve(ctx, wc.markerTag); !errors.Is(err, errdef.ErrNotFound) {
		t.Fatalf("marker after DeleteWorkspace = %v, want absent", err)
	}
}

func TestWorkspaceListMarkerOnlyDedup(t *testing.T) {
	ctx := context.Background()
	fake := newFakeORASRepo()
	c := newTestClient(&orasRepositoryClient{inner: fake})
	if err := c.Put(ctx, "other", []byte("state")); err != nil {
		t.Fatalf("put other: %v", err)
	}
	if err := newWorkspaceClient(c, "default").createWorkspaceMarker(ctx); err != nil {
		t.Fatalf("create marker-only workspace: %v", err)
	}
	names, err := c.List(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if !slices.Equal(names, []string{"default", "other"}) {
		t.Fatalf("list = %v, want sorted union without duplicates", names)
	}
}

// pushMarkerManifest pushes an arbitrary manifest under tag.
func pushMarkerManifest(t *testing.T, fake *fakeORASRepo, tag string, m ocispec.Manifest) ocispec.Descriptor {
	t.Helper()
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	desc := ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageManifest,
		Digest:    digest.FromBytes(raw),
		Size:      int64(len(raw)),
	}
	if err := fake.Push(context.Background(), desc, bytes.NewReader(raw)); err != nil {
		t.Fatalf("push manifest: %v", err)
	}
	if err := fake.Tag(context.Background(), desc, tag); err != nil {
		t.Fatalf("tag manifest: %v", err)
	}
	return desc
}

func TestWorkspaceMarkerValidationErrors(t *testing.T) {
	ctx := context.Background()
	base := func() ocispec.Manifest {
		return ocispec.Manifest{
			MediaType:    ocispec.MediaTypeImageManifest,
			ArtifactType: artifactTypeWorkspaceMarker,
			Layers:       []ocispec.Descriptor{},
			Annotations: map[string]string{
				annotationWorkspace: "default",
				annotationCreated:   workspaceMarkerCreated,
			},
		}
	}

	cases := map[string]struct {
		mutate func(*ocispec.Manifest)
		want   string
	}{
		"identity": {func(m *ocispec.Manifest) { m.Annotations[annotationWorkspace] = "other" }, "identity mismatch"},
		"type":     {func(m *ocispec.Manifest) { m.ArtifactType = artifactTypeState }, "artifactType"},
		"layers": {func(m *ocispec.Manifest) {
			m.Layers = []ocispec.Descriptor{{MediaType: mediaTypeStateLayer, Digest: digest.FromString("x"), Size: 1}}
		}, "layers"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			fake := newFakeORASRepo()
			c := newTestClient(&orasRepositoryClient{inner: fake})
			wc := newWorkspaceClient(c, "default")
			m := base()
			tc.mutate(&m)
			desc := pushMarkerManifest(t, fake, wc.markerTag, m)

			if _, err := c.Get(ctx, "default"); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("Get = %v, want %q", err, tc.want)
			}
			if _, err := c.List(ctx); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("List = %v, want %q", err, tc.want)
			}
			if err := c.Delete(ctx, "default"); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("Delete = %v, want %q", err, tc.want)
			}
			if got, err := fake.Resolve(ctx, wc.markerTag); err != nil || got.Digest != desc.Digest {
				t.Fatal("malformed/foreign marker was deleted by tag name")
			}
		})
	}
}

func TestWorkspaceDeleteWorkspaceMarkerOnly(t *testing.T) {
	ctx := context.Background()
	fake := newFakeORASRepo()
	c := newTestClient(&orasRepositoryClient{inner: fake})
	wc := newWorkspaceClient(c, "default")
	if err := wc.createWorkspaceMarker(ctx); err != nil {
		t.Fatalf("create marker: %v", err)
	}
	if err := c.DeleteWorkspace(ctx, "default"); err != nil {
		t.Fatalf("DeleteWorkspace marker-only: %v", err)
	}
	if _, err := fake.Resolve(ctx, wc.markerTag); !errors.Is(err, errdef.ErrNotFound) {
		t.Fatalf("marker after DeleteWorkspace = %v, want absent", err)
	}
	if names, err := c.List(ctx); err != nil || len(names) != 0 {
		t.Fatalf("List after reset = %v, %v; want empty", names, err)
	}
}

func TestWorkspaceDeleteValidatesMarkerBeforeState(t *testing.T) {
	ctx := context.Background()
	base := ocispec.Manifest{
		MediaType:    ocispec.MediaTypeImageManifest,
		ArtifactType: artifactTypeState, // wrong marker type
		Layers:       []ocispec.Descriptor{},
		Annotations: map[string]string{
			annotationWorkspace: "default",
			annotationCreated:   workspaceMarkerCreated,
		},
	}
	ops := map[string]func(*Client) error{
		"delete":           func(c *Client) error { return c.Delete(ctx, "default") },
		"delete-workspace": func(c *Client) error { return c.DeleteWorkspace(ctx, "default") },
	}
	for name, op := range ops {
		t.Run(name, func(t *testing.T) {
			fake := newFakeORASRepo()
			c := newTestClient(&orasRepositoryClient{inner: fake})
			wc := newWorkspaceClient(c, "default")
			if err := c.Put(ctx, "default", []byte("state")); err != nil {
				t.Fatalf("put: %v", err)
			}
			stateDesc := mustResolve(ctx, t, fake, wc.stateTag)
			bad := pushMarkerManifest(t, fake, wc.markerTag, base)

			err := op(c)
			if err == nil || !strings.Contains(err.Error(), "artifactType") {
				t.Fatalf("%s = %v, want marker validation error before deletion", name, err)
			}
			if got, err := fake.Resolve(ctx, wc.stateTag); err != nil || got.Digest != stateDesc.Digest {
				t.Fatal("state was deleted before the marker was validated")
			}
			if got, err := fake.Resolve(ctx, wc.markerTag); err != nil || got.Digest != bad.Digest {
				t.Fatal("malformed marker was deleted by tag name")
			}
		})
	}
}

func TestWorkspaceMarkerMissingContentIsNotAbsent(t *testing.T) {
	ctx := context.Background()
	fake := newFakeORASRepo()
	c := newTestClient(&orasRepositoryClient{inner: fake})
	wc := newWorkspaceClient(c, "default")
	missing := ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageManifest,
		Digest:    digest.FromString("marker manifest never pushed"),
		Size:      10,
	}
	if err := fake.Tag(ctx, missing, wc.markerTag); err != nil {
		t.Fatalf("tag missing marker: %v", err)
	}
	if _, err := c.Get(ctx, "default"); err == nil {
		t.Fatal("Get treated a resolved marker with missing content as absent")
	}
	if _, err := c.List(ctx); err == nil {
		t.Fatal("List hid a resolved marker with missing content")
	}
	if err := c.Delete(ctx, "default"); err == nil {
		t.Fatal("Delete deleted a marker it could not validate")
	}
	if _, err := fake.Resolve(ctx, wc.markerTag); err != nil {
		t.Fatalf("marker tag was removed: %v", err)
	}
}

func TestWorkspaceMarkerMappingDeterministic(t *testing.T) {
	names := []string{"default", "a/b", "ws-c14cddc033f64b9d", strings.Repeat("a", 129), "日本語", workspaceTagFor("default")}
	seen := make(map[string]string, len(names))
	for _, name := range names {
		wc := newWorkspaceClient(nil, name)
		want := markerTagPrefix + workspaceTagFor(name)
		if wc.markerTag != want {
			t.Errorf("%q marker tag = %q, want %q", name, wc.markerTag, want)
		}
		if err := (orasProperties.Reference{Tag: wc.markerTag}).ValidateTag(); err != nil {
			t.Errorf("%q marker tag invalid: %v", name, err)
		}
		if strings.Contains(wc.markerTag, name) {
			t.Errorf("%q raw name leaked into marker tag %q", name, wc.markerTag)
		}
		if other, ok := seen[wc.markerTag]; ok {
			t.Errorf("%q and %q share marker tag %q", name, other, wc.markerTag)
		}
		seen[wc.markerTag] = name
	}
}

func TestWorkspaceMarkerConcurrentFirstPutConverges(t *testing.T) {
	ctx := context.Background()
	fake := newFakeORASRepo()
	c := newTestClient(&orasRepositoryClient{inner: fake})
	wc := newWorkspaceClient(c, "default")

	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = c.Put(ctx, "default", []byte(fmt.Sprintf("state-%d", i)))
		}(i)
	}
	wg.Wait()
	succeeded := 0
	for i, err := range errs {
		if err == nil {
			succeeded++
			continue
		}
		// A rival may observe the freshly published marker-only state and
		// fail closed. This test does NOT claim mutual exclusion or that both
		// writers succeed; it only requires the final state to converge.
		if !strings.Contains(err.Error(), "workspace marker") {
			t.Fatalf("concurrent Put %d = %v", i, err)
		}
	}
	if succeeded == 0 {
		t.Fatalf("no concurrent Put succeeded: %v", errs)
	}

	// Both writers use the same deterministic marker; at least one state won.
	if present, err := wc.verifiedMarkerPresent(ctx); err != nil || !present {
		t.Fatalf("marker after concurrent first Put = %v, %v", present, err)
	}
	data, err := c.Get(ctx, "default")
	if err != nil {
		t.Fatalf("Get after concurrent first Put: %v", err)
	}
	if string(data) != "state-0" && string(data) != "state-1" {
		t.Fatalf("state = %q, want one concurrent writer's payload", data)
	}
}

func TestWorkspaceMarkerLegacyUnchanged(t *testing.T) {
	ctx := context.Background()
	fake := newFakeORASRepo()
	c := newTestClient(&orasRepositoryClient{inner: fake})
	c.config.WorkspaceLayout = WorkspaceLayoutLegacy
	wc := newWorkspaceClient(c, "default")

	if err := c.Put(ctx, "default", []byte("legacy")); err != nil {
		t.Fatalf("legacy put: %v", err)
	}
	if _, err := fake.Resolve(ctx, wc.markerTag); !errors.Is(err, errdef.ErrNotFound) {
		t.Fatalf("legacy Put created a marker: %v", err)
	}
	if data, err := c.Get(ctx, "default"); err != nil || string(data) != "legacy" {
		t.Fatalf("legacy Get = (%q, %v)", data, err)
	}
	if names, err := c.List(ctx); err != nil || !slices.Equal(names, []string{"default"}) {
		t.Fatalf("legacy List = %v, %v", names, err)
	}
	if err := c.Delete(ctx, "default"); err != nil {
		t.Fatalf("legacy delete: %v", err)
	}
}

// markerResolveFailRepo fails only Resolve(markerTag), proving the common Get
// paths reuse the listing instead of issuing an extra marker request.
type markerResolveFailRepo struct {
	delegatingRepo
	markerTag string
}

func (r *markerResolveFailRepo) Resolve(ctx context.Context, reference string) (ocispec.Descriptor, error) {
	if reference == r.markerTag {
		return ocispec.Descriptor{}, errors.New("unexpected marker resolve")
	}
	return r.delegatingRepo.Resolve(ctx, reference)
}

func TestWorkspaceMarkerRequestReuse(t *testing.T) {
	ctx := context.Background()
	fake := newFakeORASRepo()
	c := newTestClient(&orasRepositoryClient{inner: fake})
	wc := newWorkspaceClient(c, "default")
	if err := c.Put(ctx, "default", []byte("state")); err != nil {
		t.Fatalf("put: %v", err)
	}

	repo := &markerResolveFailRepo{delegatingRepo: delegatingRepo{inner: fake}, markerTag: wc.markerTag}
	c = newTestClient(&orasRepositoryClient{inner: repo})

	// Existing state does not need the marker: no marker request.
	if data, err := c.Get(ctx, "default"); err != nil || string(data) != "state" {
		t.Fatalf("Get with existing marker = (%q, %v), want no marker request", data, err)
	}

	// Absent state in the hashed layout re-resolves the marker directly, so a
	// marker published after the listing is not missed. Real absence is nil.
	c = newTestClient(&orasRepositoryClient{inner: fake})
	if data, err := c.Get(ctx, "fresh"); err != nil || data != nil {
		t.Fatalf("Get on an unmarked new workspace = (%q, %v), want nil, nil", data, err)
	}
}

func TestWorkspaceMarkerReusedOnSubsequentPut(t *testing.T) {
	ctx := context.Background()
	repo := &markerTestRepo{delegatingRepo: delegatingRepo{inner: newFakeORASRepo()}}
	c := newTestClient(&orasRepositoryClient{inner: repo})
	wc := newWorkspaceClient(c, "default")
	if err := c.Put(ctx, "default", []byte("v1")); err != nil {
		t.Fatalf("first put: %v", err)
	}
	if got := repo.countTag(wc.markerTag); got != 1 {
		t.Fatalf("marker Tag calls after first Put = %d, want 1", got)
	}
	if err := c.Put(ctx, "default", []byte("v2")); err != nil {
		t.Fatalf("second put: %v", err)
	}
	if got := repo.countTag(wc.markerTag); got != 1 {
		t.Fatalf("marker Tag calls after second Put = %d, want 1 (reused)", got)
	}
}
