package orasstate

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
)

// WorkspaceExists must probe only the named workspace's own tags: neither a
// known nor an unknown lookup may trigger the repository-wide Tags listing,
// while List keeps its enumerating behaviour.
func TestWorkspaceExists_HashedDirectLookupSkipsTagListing(t *testing.T) {
	ctx := context.Background()
	fake := newFakeORASRepo()
	repo := &countingRepo{delegatingRepo: delegatingRepo{inner: fake}}
	c := newTestClient(&orasRepositoryClient{inner: repo})

	if err := c.Put(ctx, "alpha", []byte("state")); err != nil {
		t.Fatal(err)
	}
	repo.tags.Store(0)

	if exists, err := c.WorkspaceExists(ctx, "alpha"); err != nil || !exists {
		t.Fatalf("exists(alpha) = %v, %v; want true, nil", exists, err)
	}
	if tags := repo.tags.Load(); tags != 0 {
		t.Fatalf("known lookup listed tags: Tags=%d, want 0", tags)
	}
	if exists, err := c.WorkspaceExists(ctx, "ghost"); err != nil || exists {
		t.Fatalf("exists(ghost) = %v, %v; want false, nil", exists, err)
	}
	if tags := repo.tags.Load(); tags != 0 {
		t.Fatalf("unknown lookup listed tags: Tags=%d, want 0", tags)
	}

	// Marker-only state still counts as an existing workspace, as in List.
	stateDesc, err := fake.Resolve(ctx, stateTagPrefix+workspaceTagFor("alpha"))
	if err != nil {
		t.Fatal(err)
	}
	if err := fake.Delete(ctx, stateDesc); err != nil {
		t.Fatal(err)
	}
	if exists, err := c.WorkspaceExists(ctx, "alpha"); err != nil || !exists {
		t.Fatalf("marker-only exists = %v, %v; want true, nil", exists, err)
	}
	if tags := repo.tags.Load(); tags != 0 {
		t.Fatalf("marker-only lookup listed tags: Tags=%d, want 0", tags)
	}

	names, err := c.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(names, []string{"alpha"}) {
		t.Fatalf("List = %v, want [alpha]", names)
	}
	if tags := repo.tags.Load(); tags != 1 {
		t.Fatalf("List tags = %d, want 1", tags)
	}
}

// A foreign manifest under the looked-up workspace's state tag fails closed
// with the same identity error List reports, still without a listing.
func TestWorkspaceExists_HashedRejectsForeignIdentity(t *testing.T) {
	ctx := context.Background()
	fake := newFakeORASRepo()
	repo := &countingRepo{delegatingRepo: delegatingRepo{inner: fake}}
	c := newTestClient(&orasRepositoryClient{inner: repo})

	if err := c.Put(ctx, "other", []byte("state")); err != nil {
		t.Fatal(err)
	}
	foreign, err := fake.Resolve(ctx, stateTagPrefix+workspaceTagFor("other"))
	if err != nil {
		t.Fatal(err)
	}
	if err := fake.Tag(ctx, foreign, stateTagPrefix+workspaceTagFor("ghost")); err != nil {
		t.Fatal(err)
	}
	repo.tags.Store(0)

	if _, err := c.WorkspaceExists(ctx, "ghost"); err == nil || !strings.Contains(err.Error(), "identity mismatch") {
		t.Fatalf("exists(ghost) err = %v, want identity mismatch", err)
	}
	if tags := repo.tags.Load(); tags != 0 {
		t.Fatalf("mismatch lookup listed tags: Tags=%d, want 0", tags)
	}
	if _, err := c.List(ctx); err == nil || !strings.Contains(err.Error(), "identity mismatch") {
		t.Fatalf("List err = %v, want identity mismatch", err)
	}
}

func TestWorkspaceExists_LegacyDirectLookupSkipsTagListing(t *testing.T) {
	ctx := context.Background()
	fake := newFakeORASRepo()
	repo := &countingRepo{delegatingRepo: delegatingRepo{inner: fake}}
	c := newTestClient(&orasRepositoryClient{inner: repo})
	c.config.WorkspaceLayout = WorkspaceLayoutLegacy

	if err := c.Put(ctx, "alpha", []byte("state")); err != nil {
		t.Fatal(err)
	}
	repo.tags.Store(0)

	if exists, err := c.WorkspaceExists(ctx, "alpha"); err != nil || !exists {
		t.Fatalf("exists(alpha) = %v, %v; want true, nil", exists, err)
	}
	if tags := repo.tags.Load(); tags != 0 {
		t.Fatalf("known lookup listed tags: Tags=%d, want 0", tags)
	}
	if exists, err := c.WorkspaceExists(ctx, "ghost"); err != nil || exists {
		t.Fatalf("exists(ghost) = %v, %v; want false, nil", exists, err)
	}
	if tags := repo.tags.Load(); tags != 0 {
		t.Fatalf("unknown lookup listed tags: Tags=%d, want 0", tags)
	}

	// A negative legacy lookup of "default" falls back to the listing: with
	// alpha holding state, List has no implicit default to report.
	repo.tags.Store(0)
	if exists, err := c.WorkspaceExists(ctx, "default"); err != nil || exists {
		t.Fatalf("exists(default) = %v, %v; want false, nil", exists, err)
	}
	if tags := repo.tags.Load(); tags != 1 {
		t.Fatalf("default fallback tags = %d, want 1", tags)
	}

	repo.tags.Store(0)
	names, err := c.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(names, []string{"alpha"}) {
		t.Fatalf("List = %v, want [alpha]", names)
	}
	if tags := repo.tags.Load(); tags != 1 {
		t.Fatalf("List tags = %d, want 1", tags)
	}
}

// Legacy hashed names are decided by the pinned workspace annotation, exactly
// as legacyWorkspaceNameFromTag reads them during a listing.
func TestWorkspaceExists_LegacyHashedNameFromManifestAnnotation(t *testing.T) {
	ctx := context.Background()
	fake := newFakeORASRepo()
	repo := &countingRepo{delegatingRepo: delegatingRepo{inner: fake}}
	c := newTestClient(&orasRepositoryClient{inner: repo})
	c.config.WorkspaceLayout = WorkspaceLayoutLegacy

	const hashedName = "my workspace"
	if err := c.Put(ctx, hashedName, []byte("state")); err != nil {
		t.Fatal(err)
	}
	hashTag := legacyWorkspaceTagFor(hashedName) // ws- plus 16 hex chars

	// A manifest without a workspace annotation answers by its ws- suffix,
	// the fallback of legacyWorkspaceNameFromTag.
	plain, err := newWorkspaceClient(c, "unused").packStateManifestAnnotated(ctx, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	const plainName = "ws-0011223344556677"
	if err := fake.Tag(ctx, plain, stateTagPrefix+plainName); err != nil {
		t.Fatal(err)
	}
	repo.tags.Store(0)

	for _, tc := range []struct {
		name string
		want bool
	}{
		{hashedName, true},
		{plainName, true},
		// The hash-shaped literal is not the hashed workspace: the pinned
		// annotation decides, so probing it reports absence.
		{hashTag, false},
		{"ghost", false},
	} {
		exists, err := c.WorkspaceExists(ctx, tc.name)
		if err != nil || exists != tc.want {
			t.Errorf("exists(%q) = %v, %v; want %v, nil", tc.name, exists, err, tc.want)
		}
	}
	if tags := repo.tags.Load(); tags != 0 {
		t.Fatalf("lookups listed tags: Tags=%d, want 0", tags)
	}

	names, err := c.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(names, []string{hashedName, plainName}) {
		t.Fatalf("List = %v, want [%q %q]", names, hashedName, plainName)
	}
}

// state-foo-v1 is workspace foo's retained history while state-foo exists, but
// the literal state of workspace foo-v1 otherwise — the same rule the listing
// applies to version-shaped tags.
func TestWorkspaceExists_LegacyVersionAmbiguity(t *testing.T) {
	ctx := context.Background()
	fake := newFakeORASRepo()
	repo := &countingRepo{delegatingRepo: delegatingRepo{inner: fake}}
	c := newTestClient(&orasRepositoryClient{inner: repo})
	c.config.WorkspaceLayout = WorkspaceLayoutLegacy

	if err := c.Put(ctx, "foo", []byte("state")); err != nil {
		t.Fatal(err)
	}
	// Old history: foo's own state manifest under the version-shaped tag.
	fooState, err := fake.Resolve(ctx, stateTagPrefix+legacyWorkspaceTagFor("foo"))
	if err != nil {
		t.Fatal(err)
	}
	if err := fake.Tag(ctx, fooState, stateTagPrefix+legacyWorkspaceTagFor("foo")+"-v1"); err != nil {
		t.Fatal(err)
	}
	// Literal identities win: bar-v1 is its own workspace even while bar and
	// its -v1-shaped tag both exist.
	if err := c.Put(ctx, "bar", []byte("state")); err != nil {
		t.Fatal(err)
	}
	if err := c.Put(ctx, "bar-v1", []byte("state")); err != nil {
		t.Fatal(err)
	}
	repo.tags.Store(0)

	for _, tc := range []struct {
		name string
		want bool
	}{
		{"foo", true},
		{"foo-v1", false}, // the tag proves foo's history
		{"bar", true},
		{"bar-v1", true}, // full literal identity wins
		{"ghost", false},
	} {
		exists, err := c.WorkspaceExists(ctx, tc.name)
		if err != nil || exists != tc.want {
			t.Errorf("exists(%q) = %v, %v; want %v, nil", tc.name, exists, err, tc.want)
		}
	}
	if tags := repo.tags.Load(); tags != 0 {
		t.Fatalf("lookups listed tags: Tags=%d, want 0", tags)
	}

	names, err := c.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"bar", "bar-v1", "foo"}; !slices.Equal(names, want) {
		t.Fatalf("List = %v, want %v", names, want)
	}
}

// The implicit default of an empty legacy repository is only provable by the
// listing; the hashed layout has no implicit default and never lists here.
func TestWorkspaceExists_LegacyDefaultSemantics(t *testing.T) {
	ctx := context.Background()

	t.Run("empty legacy repository", func(t *testing.T) {
		fake := newFakeORASRepo()
		repo := &countingRepo{delegatingRepo: delegatingRepo{inner: fake}}
		c := newTestClient(&orasRepositoryClient{inner: repo})
		c.config.WorkspaceLayout = WorkspaceLayoutLegacy

		names, err := c.List(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(names, []string{"default"}) {
			t.Fatalf("List = %v, want [default]", names)
		}

		repo.tags.Store(0)
		exists, err := c.WorkspaceExists(ctx, "default")
		if err != nil || !exists {
			t.Fatalf("exists(default) = %v, %v; want true, nil", exists, err)
		}
		if tags := repo.tags.Load(); tags != 1 {
			t.Fatalf("default fallback tags = %d, want 1", tags)
		}
	})

	t.Run("empty hashed repository", func(t *testing.T) {
		fake := newFakeORASRepo()
		repo := &countingRepo{delegatingRepo: delegatingRepo{inner: fake}}
		c := newTestClient(&orasRepositoryClient{inner: repo})

		names, err := c.List(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(names) != 0 {
			t.Fatalf("List = %v, want empty", names)
		}

		repo.tags.Store(0)
		if exists, err := c.WorkspaceExists(ctx, "default"); err != nil || exists {
			t.Fatalf("exists(default) = %v, %v; want false, nil", exists, err)
		}
		if tags := repo.tags.Load(); tags != 0 {
			t.Fatalf("lookup listed tags: Tags=%d, want 0", tags)
		}
	})
}

// listLegacyWorkspaces validates at most
// legacyVersionWorkspaceValidationLimitPerBase version-shaped tags per base
// and drops the middle candidates entirely; the direct lookup still answers
// for an exact literal workspace there. This characterizes that documented
// discrepancy instead of reproducing the listing's read bound.
func TestWorkspaceExists_LegacyMiddleCandidateBeyondListValidationBound(t *testing.T) {
	ctx := context.Background()
	fake := newFakeORASRepo()
	repo := &countingRepo{delegatingRepo: delegatingRepo{inner: fake}}
	c := newTestClient(&orasRepositoryClient{inner: repo})
	c.config.WorkspaceLayout = WorkspaceLayoutLegacy

	if err := c.Put(ctx, "foo", []byte("state")); err != nil {
		t.Fatal(err)
	}
	// The literal workspace whose tag is version-shaped: state-foo-v9.
	if err := c.Put(ctx, "foo-v9", []byte("literal")); err != nil {
		t.Fatal(err)
	}
	fooState, err := fake.Resolve(ctx, stateTagPrefix+legacyWorkspaceTagFor("foo"))
	if err != nil {
		t.Fatal(err)
	}
	// 18 version-shaped candidates for base state-foo: only v9 is literal,
	// every other one is foo's history. With the bound at 16 the listing
	// validates the first and last 8, so v9 falls in the dropped middle.
	for v := 1; v <= 18; v++ {
		if v == 9 {
			continue
		}
		tag := fmt.Sprintf("%s%s-v%d", stateTagPrefix, legacyWorkspaceTagFor("foo"), v)
		if err := fake.Tag(ctx, fooState, tag); err != nil {
			t.Fatal(err)
		}
	}
	repo.tags.Store(0)

	if exists, err := c.WorkspaceExists(ctx, "foo-v9"); err != nil || !exists {
		t.Fatalf("exists(foo-v9) = %v, %v; want true, nil (exact literal lookup)", exists, err)
	}
	if tags := repo.tags.Load(); tags != 0 {
		t.Fatalf("middle-candidate lookup listed tags: Tags=%d, want 0", tags)
	}
	// A validated history candidate still answers false from the probe.
	if exists, err := c.WorkspaceExists(ctx, "foo-v1"); err != nil || exists {
		t.Fatalf("exists(foo-v1) = %v, %v; want false, nil (foo's history)", exists, err)
	}
	if tags := repo.tags.Load(); tags != 0 {
		t.Fatalf("history lookup listed tags: Tags=%d, want 0", tags)
	}

	// The bounded listing omits the middle literal candidate: the documented
	// discrepancy, not a lookup failure.
	names, err := c.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"foo"}; !slices.Equal(names, want) {
		t.Fatalf("List = %v, want %v (middle literal candidate omitted by the validation bound)", names, want)
	}
	if tags := repo.tags.Load(); tags != 1 {
		t.Fatalf("List tags = %d, want 1", tags)
	}
}
