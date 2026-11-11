package orasstate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/opencontainers/go-digest"
	specs "github.com/opencontainers/image-spec/specs-go"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/oras-project/oras-go/v3/errdef"
	orasErrcode "github.com/oras-project/oras-go/v3/registry/remote/errcode"
)

// newCopyClient creates a Client backed by an in-memory fake repository.
func newCopyClient(t *testing.T, repository string, layout WorkspaceLayout) (*Client, *fakeORASRepo) {
	t.Helper()
	repo := newFakeORASRepo()
	return &Client{
		repoClient:   &orasRepositoryClient{repository: repository, inner: repo},
		config:       Config{WorkspaceLayout: layout},
		retentionSem: make(chan struct{}, 3),
	}, repo
}

// seedCopySource publishes one state, one history version, and the workspace
// marker for workspace "default".
func seedCopySource(t *testing.T, c *Client) {
	t.Helper()
	c.config.MaxVersions = 2
	if err := c.Put(context.Background(), "default", []byte(`{"serial":1}`)); err != nil {
		t.Fatalf("seed Put: %v", err)
	}
	c.retentionWg.Wait()
}

// addCopyArtifact pushes a small unknown OCI image manifest and tags it. It is
// used for unknown tags, aliases, tombstones, and lock tags.
func addCopyArtifact(t *testing.T, repo *fakeORASRepo, tag string, annotations map[string]string) ocispec.Descriptor {
	t.Helper()
	ctx := context.Background()

	config := ocispec.DescriptorEmptyJSON
	if err := repo.Push(ctx, config, bytes.NewReader(config.Data)); err != nil {
		t.Fatalf("push config: %v", err)
	}
	m := ocispec.Manifest{
		Versioned:    specs.Versioned{SchemaVersion: 2},
		MediaType:    ocispec.MediaTypeImageManifest,
		ArtifactType: "application/vnd.example.copy-test.v1",
		Config:       config,
		Layers:       []ocispec.Descriptor{},
		Annotations:  annotations,
	}
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal artifact: %v", err)
	}
	desc := ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageManifest,
		Digest:    digest.FromBytes(raw),
		Size:      int64(len(raw)),
	}
	if err := repo.Push(ctx, desc, bytes.NewReader(raw)); err != nil {
		t.Fatalf("push artifact: %v", err)
	}
	if err := repo.Tag(ctx, desc, tag); err != nil {
		t.Fatalf("tag artifact: %v", err)
	}
	return desc
}

func resolveFake(t *testing.T, repo *fakeORASRepo, tag string) ocispec.Descriptor {
	t.Helper()
	desc, err := repo.Resolve(context.Background(), tag)
	if err != nil {
		t.Fatalf("resolve %q: %v", tag, err)
	}
	return desc
}

func fetchFake(t *testing.T, repo *fakeORASRepo, desc ocispec.Descriptor) []byte {
	t.Helper()
	rc, err := repo.Fetch(context.Background(), desc)
	if err != nil {
		t.Fatalf("fetch %s: %v", desc.Digest, err)
	}
	defer func() { _ = rc.Close() }()
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read %s: %v", desc.Digest, err)
	}
	return data
}

func copyPlanTags(plan *CopyPlan) []string {
	tags := make([]string, len(plan.Artifacts))
	for i, artifact := range plan.Artifacts {
		tags[i] = artifact.Tag
	}
	return tags
}

func copyArtifactTags(artifacts []CopyArtifact) []string {
	tags := make([]string, len(artifacts))
	for i, artifact := range artifacts {
		tags[i] = artifact.Tag
	}
	return tags
}

func copyArtifactByTag(t *testing.T, plan *CopyPlan, tag string) CopyArtifact {
	t.Helper()
	for _, artifact := range plan.Artifacts {
		if artifact.Tag == tag {
			return artifact
		}
	}
	t.Fatalf("tag %q not in plan", tag)
	return CopyArtifact{}
}

func TestCopyPlanAndCopyTo_PreservesTagsAndDigests(t *testing.T) {
	ctx := context.Background()
	src, srcRepo := newCopyClient(t, "example/src", WorkspaceLayoutHashed)
	seedCopySource(t, src)

	wsTag := workspaceTagFor("default")
	unknown := addCopyArtifact(t, srcRepo, "custom-artifact", map[string]string{"example": "unknown"})
	if err := srcRepo.Tag(ctx, unknown, "custom-alias"); err != nil {
		t.Fatalf("tag alias: %v", err)
	}
	tombstoneTag := unlockedTagPrefix + workspaceTagFor("gone")
	addCopyUnlockMarker(t, src, srcRepo, tombstoneTag, "gone")

	plan, err := src.PlanCopy(ctx)
	if err != nil {
		t.Fatalf("PlanCopy: %v", err)
	}
	if plan.Source != "example/src" {
		t.Fatalf("plan source = %q, want example/src", plan.Source)
	}
	wantTags := []string{
		"custom-alias",
		"custom-artifact",
		stateTagPrefix + wsTag,
		stateVersionTagPrefix + wsTag + "-v1",
		markerTagPrefix + wsTag,
	}
	slices.Sort(wantTags)
	if got := copyPlanTags(plan); !slices.Equal(got, wantTags) {
		t.Fatalf("plan tags = %v, want %v", got, wantTags)
	}
	if !slices.Contains(plan.Skipped, tombstoneTag) {
		t.Fatalf("plan skipped = %v, want %q", plan.Skipped, tombstoneTag)
	}
	if _, ok := plan.snapshot[tombstoneTag]; !ok {
		t.Fatal("source snapshot must include skipped tombstones")
	}

	stateArtifact := copyArtifactByTag(t, plan, stateTagPrefix+wsTag)
	if stateArtifact.Kind != ArtifactKindState || stateArtifact.Workspace != "default" {
		t.Errorf("state artifact = %+v, want kind state and workspace default", stateArtifact)
	}
	historyArtifact := copyArtifactByTag(t, plan, stateVersionTagPrefix+wsTag+"-v1")
	if historyArtifact.Kind != ArtifactKindHistory || historyArtifact.Workspace != "default" {
		t.Errorf("history artifact = %+v, want kind history and workspace default", historyArtifact)
	}
	markerArtifact := copyArtifactByTag(t, plan, markerTagPrefix+wsTag)
	if markerArtifact.Kind != ArtifactKindMarker || markerArtifact.Workspace != "default" {
		t.Errorf("marker artifact = %+v, want kind marker and workspace default", markerArtifact)
	}
	unknownArtifact := copyArtifactByTag(t, plan, "custom-artifact")
	if unknownArtifact.Kind != ArtifactKindUnknown || unknownArtifact.Workspace != "" {
		t.Errorf("unknown artifact = %+v, want kind unknown and empty workspace", unknownArtifact)
	}

	dst, dstRepo := newCopyClient(t, "example/dst", WorkspaceLayoutHashed)
	report, err := src.CopyTo(ctx, dst, plan)
	if err != nil {
		t.Fatalf("CopyTo: %v", err)
	}
	if got := copyArtifactTags(report.Copied); !slices.Equal(got, copyPlanTags(plan)) {
		t.Fatalf("copied = %v, want %v", got, copyPlanTags(plan))
	}
	if len(report.Existing) != 0 {
		t.Fatalf("existing = %v, want none", report.Existing)
	}
	if len(report.Verified) != len(plan.Artifacts) {
		t.Fatalf("verified = %d, want %d", len(report.Verified), len(plan.Artifacts))
	}

	for _, tag := range copyPlanTags(plan) {
		srcDesc := resolveFake(t, srcRepo, tag)
		dstDesc := resolveFake(t, dstRepo, tag)
		if srcDesc.Digest != dstDesc.Digest {
			t.Errorf("tag %q digest = %s, want %s", tag, dstDesc.Digest, srcDesc.Digest)
			continue
		}
		if !bytes.Equal(fetchFake(t, srcRepo, srcDesc), fetchFake(t, dstRepo, dstDesc)) {
			t.Errorf("tag %q manifest bytes changed during copy", tag)
		}
	}
	if _, err := dstRepo.Resolve(ctx, tombstoneTag); !errors.Is(err, errdef.ErrNotFound) {
		t.Errorf("tombstone %q was copied: %v", tombstoneTag, err)
	}
	for _, tag := range copyPlanTags(plan) {
		if _, err := srcRepo.Resolve(ctx, tag); err != nil {
			t.Errorf("source tag %q was removed: %v", tag, err)
		}
	}
}

func TestCopyPlan_RefusesLiveLock(t *testing.T) {
	ctx := context.Background()
	src, srcRepo := newCopyClient(t, "example/src", WorkspaceLayoutHashed)
	lockTag := lockTagPrefix + workspaceTagFor("default")
	addCopyArtifact(t, srcRepo, lockTag, map[string]string{annotationWorkspace: "default"})

	if _, err := src.PlanCopy(ctx); err == nil || !strings.Contains(err.Error(), "live lock") {
		t.Fatalf("PlanCopy with a lock = %v, want live-lock error", err)
	}
}

func TestCopyPlan_RefusesHashedWorkspaceIdentityMismatch(t *testing.T) {
	ctx := context.Background()
	src, repo := newCopyClient(t, "example/src", WorkspaceLayoutHashed)
	addCopyArtifact(t, repo, stateTagPrefix+workspaceTagFor("expected"), map[string]string{annotationWorkspace: "different"})

	if _, err := src.PlanCopy(ctx); err == nil || !strings.Contains(err.Error(), "workspace identity mismatch") {
		t.Fatalf("PlanCopy with mismatched hashed workspace identity = %v, want identity-mismatch error", err)
	}
}

func TestCopyTo_RefusesLockAcquiredAfterPlanBeforeWrites(t *testing.T) {
	ctx := context.Background()
	src, srcRepo := newCopyClient(t, "example/src", WorkspaceLayoutHashed)
	seedCopySource(t, src)
	plan, err := src.PlanCopy(ctx)
	if err != nil {
		t.Fatalf("PlanCopy: %v", err)
	}

	addCopyArtifact(t, srcRepo, lockTagPrefix+workspaceTagFor("default"), nil)

	dst, dstRepo := newCopyClient(t, "example/dst", WorkspaceLayoutHashed)
	if _, err := src.CopyTo(ctx, dst, plan); err == nil || !strings.Contains(err.Error(), "source changed") {
		t.Fatalf("CopyTo with a late lock = %v, want source-changed error", err)
	}
	if len(dstRepo.tags) != 0 {
		t.Fatalf("destination was written before the late lock was refused: %v", dstRepo.tags)
	}
}

func TestCopyTo_RefusesDestinationUnexpectedTag(t *testing.T) {
	ctx := context.Background()
	src, _ := newCopyClient(t, "example/src", WorkspaceLayoutHashed)
	seedCopySource(t, src)
	plan, err := src.PlanCopy(ctx)
	if err != nil {
		t.Fatalf("PlanCopy: %v", err)
	}

	dst, dstRepo := newCopyClient(t, "example/dst", WorkspaceLayoutHashed)
	addCopyArtifact(t, dstRepo, "stranger", nil)

	if _, err := src.CopyTo(ctx, dst, plan); err == nil || !strings.Contains(err.Error(), "not in the copy plan") {
		t.Fatalf("CopyTo with unexpected destination tag = %v, want refusal", err)
	}
	if len(dstRepo.tags) != 1 {
		t.Fatalf("destination tags = %v, want only the pre-existing stranger", dstRepo.tags)
	}
}

func TestCopyTo_RefusesDestinationDigestConflict(t *testing.T) {
	ctx := context.Background()
	src, _ := newCopyClient(t, "example/src", WorkspaceLayoutHashed)
	seedCopySource(t, src)
	plan, err := src.PlanCopy(ctx)
	if err != nil {
		t.Fatalf("PlanCopy: %v", err)
	}

	dst, dstRepo := newCopyClient(t, "example/dst", WorkspaceLayoutHashed)
	conflictTag := copyPlanTags(plan)[0]
	addCopyArtifact(t, dstRepo, conflictTag, nil)

	if _, err := src.CopyTo(ctx, dst, plan); err == nil || !strings.Contains(err.Error(), "refusing to overwrite") {
		t.Fatalf("CopyTo with destination digest conflict = %v, want refusal", err)
	}
	if len(dstRepo.tags) != 1 {
		t.Fatalf("destination tags = %v, want only the conflicting tag", dstRepo.tags)
	}
}

func TestCopyTo_ResumesMatchingSubset(t *testing.T) {
	ctx := context.Background()
	src, _ := newCopyClient(t, "example/src", WorkspaceLayoutHashed)
	seedCopySource(t, src)
	plan, err := src.PlanCopy(ctx)
	if err != nil {
		t.Fatalf("PlanCopy: %v", err)
	}
	dst, dstRepo := newCopyClient(t, "example/dst", WorkspaceLayoutHashed)
	if _, err := src.CopyTo(ctx, dst, plan); err != nil {
		t.Fatalf("first CopyTo: %v", err)
	}

	removed := copyPlanTags(plan)[0]
	dstRepo.mu.Lock()
	delete(dstRepo.tags, removed)
	dstRepo.mu.Unlock()

	report, err := src.CopyTo(ctx, dst, plan)
	if err != nil {
		t.Fatalf("resume CopyTo: %v", err)
	}
	if got := copyArtifactTags(report.Copied); !slices.Equal(got, []string{removed}) {
		t.Fatalf("copied = %v, want [%q]", got, removed)
	}
	if len(report.Existing) != len(plan.Artifacts)-1 {
		t.Fatalf("existing = %v, want %d tags", report.Existing, len(plan.Artifacts)-1)
	}
	if len(report.Verified) != len(plan.Artifacts) {
		t.Fatalf("verified = %d, want %d", len(report.Verified), len(plan.Artifacts))
	}
	for _, tag := range copyPlanTags(plan) {
		resolveFake(t, dstRepo, tag)
	}
}

// lateTagRepo adds an unplanned tag on the Nth Tags call. Tests arm it after
// PlanCopy so the change is detected by CopyTo's final source inventory.
type lateTagRepo struct {
	delegatingRepo
	remaining int
	lateTag   string
	lateDesc  ocispec.Descriptor
}

func (r *lateTagRepo) Tags(ctx context.Context, last string, fn func(tags []string) error) error {
	if r.remaining > 0 {
		r.remaining--
		if r.remaining == 0 {
			r.inner.mu.Lock()
			r.inner.tags[r.lateTag] = r.lateDesc
			r.inner.mu.Unlock()
		}
	}
	return r.delegatingRepo.Tags(ctx, last, fn)
}

func TestCopyTo_DetectsSourceChangeDuringCopy(t *testing.T) {
	ctx := context.Background()
	inner := newFakeORASRepo()
	late := &lateTagRepo{delegatingRepo: delegatingRepo{inner: inner}}
	src := &Client{
		repoClient:   &orasRepositoryClient{repository: "example/src", inner: late},
		config:       Config{MaxVersions: 2},
		retentionSem: make(chan struct{}, 3),
	}
	seedCopySource(t, src)
	plan, err := src.PlanCopy(ctx)
	if err != nil {
		t.Fatalf("PlanCopy: %v", err)
	}
	late.lateTag = "late-tag"
	late.lateDesc = resolveFake(t, inner, stateTagPrefix+workspaceTagFor("default"))
	// CopyTo inventories the source once before copying and once after.
	late.remaining = 2

	dst, dstRepo := newCopyClient(t, "example/dst", WorkspaceLayoutHashed)
	report, err := src.CopyTo(ctx, dst, plan)
	if err == nil || !strings.Contains(err.Error(), "source changed") {
		t.Fatalf("CopyTo with a source change = %v, want source-changed error", err)
	}
	if report == nil || len(report.Copied) != len(plan.Artifacts) {
		t.Fatalf("report = %+v, want all tags copied before the final check", report)
	}
	for _, tag := range copyPlanTags(plan) {
		resolveFake(t, dstRepo, tag)
	}
}

// corruptingDestRepo returns a wrong digest for one tag once that tag has been
// written, simulating a destination that does not honor the pinned digest.
type corruptingDestRepo struct {
	delegatingRepo
	tag   string
	wrong ocispec.Descriptor
	armed bool
}

func (r *corruptingDestRepo) Tag(ctx context.Context, desc ocispec.Descriptor, reference string) error {
	if err := r.delegatingRepo.Tag(ctx, desc, reference); err != nil {
		return err
	}
	if reference == r.tag {
		r.armed = true
	}
	return nil
}

func (r *corruptingDestRepo) Resolve(ctx context.Context, reference string) (ocispec.Descriptor, error) {
	if r.armed && reference == r.tag {
		return r.wrong, nil
	}
	return r.delegatingRepo.Resolve(ctx, reference)
}

func TestCopyTo_DetectsDestinationVerificationFailure(t *testing.T) {
	ctx := context.Background()
	src, _ := newCopyClient(t, "example/src", WorkspaceLayoutHashed)
	seedCopySource(t, src)
	plan, err := src.PlanCopy(ctx)
	if err != nil {
		t.Fatalf("PlanCopy: %v", err)
	}

	dstRepo := &corruptingDestRepo{
		delegatingRepo: delegatingRepo{inner: newFakeORASRepo()},
		tag:            copyPlanTags(plan)[0],
		wrong: ocispec.Descriptor{
			MediaType: ocispec.MediaTypeImageManifest,
			Digest:    digest.FromString("wrong"),
			Size:      5,
		},
	}
	dst := &Client{
		repoClient:   &orasRepositoryClient{repository: "example/dst", inner: dstRepo},
		config:       Config{WorkspaceLayout: WorkspaceLayoutHashed},
		retentionSem: make(chan struct{}, 3),
	}

	if _, err := src.CopyTo(ctx, dst, plan); err == nil || !strings.Contains(err.Error(), "after copy") {
		t.Fatalf("CopyTo with a bad destination digest = %v, want verification error", err)
	}
}

type deleteCountingRepo struct {
	delegatingRepo
	deletes int
}

func (r *deleteCountingRepo) Delete(ctx context.Context, target ocispec.Descriptor) error {
	r.deletes++
	return r.delegatingRepo.Delete(ctx, target)
}

func TestCopyTo_NeverDeletes(t *testing.T) {
	ctx := context.Background()
	srcRepo := &deleteCountingRepo{delegatingRepo: delegatingRepo{inner: newFakeORASRepo()}}
	src := &Client{
		repoClient:   &orasRepositoryClient{repository: "example/src", inner: srcRepo},
		config:       Config{MaxVersions: 2},
		retentionSem: make(chan struct{}, 3),
	}
	seedCopySource(t, src)
	plan, err := src.PlanCopy(ctx)
	if err != nil {
		t.Fatalf("PlanCopy: %v", err)
	}

	dstRepo := &deleteCountingRepo{delegatingRepo: delegatingRepo{inner: newFakeORASRepo()}}
	dst := &Client{
		repoClient:   &orasRepositoryClient{repository: "example/dst", inner: dstRepo},
		config:       Config{WorkspaceLayout: WorkspaceLayoutHashed},
		retentionSem: make(chan struct{}, 3),
	}
	if _, err := src.CopyTo(ctx, dst, plan); err != nil {
		t.Fatalf("CopyTo: %v", err)
	}
	if srcRepo.deletes != 0 || dstRepo.deletes != 0 {
		t.Fatalf("deletes: source=%d destination=%d, want 0", srcRepo.deletes, dstRepo.deletes)
	}
	for _, tag := range copyPlanTags(plan) {
		resolveFake(t, srcRepo.inner, tag)
	}
}

func TestCopyTo_RejectsSameRepositoryAndLayoutMismatch(t *testing.T) {
	ctx := context.Background()
	src, _ := newCopyClient(t, "example/src", WorkspaceLayoutHashed)
	seedCopySource(t, src)
	plan, err := src.PlanCopy(ctx)
	if err != nil {
		t.Fatalf("PlanCopy: %v", err)
	}

	if _, err := src.CopyTo(ctx, src, plan); err == nil || !strings.Contains(err.Error(), "same repository") {
		t.Fatalf("CopyTo to the same repository = %v, want refusal", err)
	}

	dst, _ := newCopyClient(t, "example/dst", WorkspaceLayoutLegacy)
	if _, err := src.CopyTo(ctx, dst, plan); err == nil || !strings.Contains(err.Error(), "workspace layouts") {
		t.Fatalf("CopyTo with mismatched layout = %v, want refusal", err)
	}
}

func TestCopyTo_RejectsMutatedOrForeignPlan(t *testing.T) {
	ctx := context.Background()
	src, _ := newCopyClient(t, "example/src", WorkspaceLayoutHashed)
	seedCopySource(t, src)

	mutations := []struct {
		name   string
		mutate func(*CopyArtifact)
	}{
		{"digest", func(a *CopyArtifact) { a.Descriptor.Digest = digest.FromString("forged") }},
		{"size", func(a *CopyArtifact) { a.Descriptor.Size++ }},
		{"media type", func(a *CopyArtifact) { a.Descriptor.MediaType = "application/vnd.forged" }},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			plan, err := src.PlanCopy(ctx)
			if err != nil {
				t.Fatalf("PlanCopy: %v", err)
			}
			dst, dstRepo := newCopyClient(t, "example/dst", WorkspaceLayoutHashed)
			mutation.mutate(&plan.Artifacts[0])
			if _, err := src.CopyTo(ctx, dst, plan); err == nil || !strings.Contains(err.Error(), "inconsistent") {
				t.Fatalf("CopyTo with a mutated plan = %v, want refusal", err)
			}
			if len(dstRepo.tags) != 0 {
				t.Fatalf("destination was written for a mutated plan: %v", dstRepo.tags)
			}
		})
	}

	t.Run("foreign plan", func(t *testing.T) {
		dst, _ := newCopyClient(t, "example/dst", WorkspaceLayoutHashed)
		other, _ := newCopyClient(t, "example/other", WorkspaceLayoutHashed)
		otherPlan, err := other.PlanCopy(ctx)
		if err != nil {
			t.Fatalf("PlanCopy: %v", err)
		}
		if _, err := src.CopyTo(ctx, dst, otherPlan); err == nil || !strings.Contains(err.Error(), "different source repository") {
			t.Fatalf("CopyTo with a foreign plan = %v, want refusal", err)
		}
	})
}

// vanishingTagRepo lists a tag but fails to resolve it, simulating a tag
// deletion between the Tags listing and the per-tag Resolve.
type vanishingTagRepo struct {
	delegatingRepo
	tag string
}

func (r *vanishingTagRepo) Resolve(ctx context.Context, reference string) (ocispec.Descriptor, error) {
	if reference == r.tag {
		return ocispec.Descriptor{}, errdef.ErrNotFound
	}
	return r.delegatingRepo.Resolve(ctx, reference)
}

func TestCopyPlan_RefusesTagDisappearingDuringPlanning(t *testing.T) {
	ctx := context.Background()
	inner := newFakeORASRepo()
	addCopyArtifact(t, inner, "vanishing", nil)
	src := &Client{
		repoClient: &orasRepositoryClient{
			repository: "example/src",
			inner:      &vanishingTagRepo{delegatingRepo: delegatingRepo{inner: inner}, tag: "vanishing"},
		},
		config:       Config{},
		retentionSem: make(chan struct{}, 3),
	}

	if _, err := src.PlanCopy(ctx); err == nil || !strings.Contains(err.Error(), "disappeared") {
		t.Fatalf("PlanCopy with a vanishing tag = %v, want fail-closed error", err)
	}
}

// tagsErrorRepo makes tag listing fail with a fixed error.
type tagsErrorRepo struct {
	delegatingRepo
	err error
}

func (r *tagsErrorRepo) Tags(context.Context, string, func([]string) error) error {
	return r.err
}

func TestCopyPlan_MissingRepositoryIsEmptyAndPermissionFails(t *testing.T) {
	ctx := context.Background()

	missing := &Client{
		repoClient: &orasRepositoryClient{
			repository: "example/missing",
			inner:      &tagsErrorRepo{delegatingRepo: delegatingRepo{inner: newFakeORASRepo()}, err: errdef.ErrNotFound},
		},
		config:       Config{},
		retentionSem: make(chan struct{}, 3),
	}
	plan, err := missing.PlanCopy(ctx)
	if err != nil {
		t.Fatalf("PlanCopy on a missing repository: %v", err)
	}
	if plan == nil || len(plan.Artifacts) != 0 {
		t.Fatalf("plan = %+v, want an empty plan", plan)
	}

	denied := &Client{
		repoClient: &orasRepositoryClient{
			repository: "example/denied",
			inner: &tagsErrorRepo{
				delegatingRepo: delegatingRepo{inner: newFakeORASRepo()},
				err:            &orasErrcode.ErrorResponse{StatusCode: http.StatusForbidden},
			},
		},
		config:       Config{},
		retentionSem: make(chan struct{}, 3),
	}
	if _, err := denied.PlanCopy(ctx); err == nil {
		t.Fatal("PlanCopy with a permission error must fail")
	}
}

func TestCopyPlan_HashedLayoutRejectsLegacyTag(t *testing.T) {
	ctx := context.Background()
	src, srcRepo := newCopyClient(t, "example/src", WorkspaceLayoutHashed)
	addCopyArtifact(t, srcRepo, stateTagPrefix+"default", map[string]string{annotationWorkspace: "default"})

	if _, err := src.PlanCopy(ctx); err == nil || !strings.Contains(err.Error(), "legacy workspace tag") {
		t.Fatalf("PlanCopy with a legacy tag under the hashed layout = %v, want refusal", err)
	}
}

func TestCopyPlan_LegacyLayoutRejectsHashedTag(t *testing.T) {
	ctx := context.Background()
	src, srcRepo := newCopyClient(t, "example/src", WorkspaceLayoutLegacy)
	hashedTag := stateTagPrefix + workspaceTagFor("default")
	addCopyArtifact(t, srcRepo, hashedTag, map[string]string{annotationWorkspace: "default"})

	if _, err := src.PlanCopy(ctx); err == nil || !strings.Contains(err.Error(), "hashed workspace layout") {
		t.Fatalf("PlanCopy with a hashed tag under the legacy layout = %v, want refusal", err)
	}
}

func TestCopyPlan_RefusesLegacyWorkspaceIdentityMismatch(t *testing.T) {
	ctx := context.Background()
	src, srcRepo := newCopyClient(t, "example/src", WorkspaceLayoutLegacy)
	addCopyArtifact(t, srcRepo, stateTagPrefix+"foo", map[string]string{annotationWorkspace: "other"})

	if _, err := src.PlanCopy(ctx); err == nil || !strings.Contains(err.Error(), "workspace identity mismatch") {
		t.Fatalf("PlanCopy with a mismatched legacy workspace annotation = %v, want refusal", err)
	}
}

func TestCopyPlan_LegacyLayoutPreservesLiteralAndUnknownTags(t *testing.T) {
	ctx := context.Background()
	src, srcRepo := newCopyClient(t, "example/src", WorkspaceLayoutLegacy)
	seedCopySource(t, src)
	addCopyArtifact(t, srcRepo, "custom-artifact", nil)

	plan, err := src.PlanCopy(ctx)
	if err != nil {
		t.Fatalf("PlanCopy: %v", err)
	}
	wantTags := []string{
		"custom-artifact",
		stateTagPrefix + "default",
		stateVersionTagPrefix + "default-v1",
	}
	slices.Sort(wantTags)
	if got := copyPlanTags(plan); !slices.Equal(got, wantTags) {
		t.Fatalf("plan tags = %v, want %v", got, wantTags)
	}

	dst, dstRepo := newCopyClient(t, "example/dst", WorkspaceLayoutLegacy)
	if _, err := src.CopyTo(ctx, dst, plan); err != nil {
		t.Fatalf("CopyTo: %v", err)
	}
	for _, tag := range wantTags {
		srcDesc := resolveFake(t, srcRepo, tag)
		dstDesc := resolveFake(t, dstRepo, tag)
		if srcDesc.Digest != dstDesc.Digest {
			t.Errorf("tag %q digest = %s, want %s", tag, dstDesc.Digest, srcDesc.Digest)
		}
	}
}

// lateDestMutationRepo runs mutate on the Nth Tags call. Tests arm it so the
// change happens during CopyTo's final destination inventory.
type lateDestMutationRepo struct {
	delegatingRepo
	remaining int
	mutate    func(*fakeORASRepo)
}

func (r *lateDestMutationRepo) Tags(ctx context.Context, last string, fn func(tags []string) error) error {
	if r.remaining > 0 {
		r.remaining--
		if r.remaining == 0 {
			r.inner.mu.Lock()
			r.mutate(r.inner)
			r.inner.mu.Unlock()
		}
	}
	return r.delegatingRepo.Tags(ctx, last, fn)
}

func TestCopyTo_DetectsDestinationUnexpectedTagAfterCopy(t *testing.T) {
	ctx := context.Background()
	src, _ := newCopyClient(t, "example/src", WorkspaceLayoutHashed)
	seedCopySource(t, src)
	plan, err := src.PlanCopy(ctx)
	if err != nil {
		t.Fatalf("PlanCopy: %v", err)
	}

	dstInner := newFakeORASRepo()
	late := &lateDestMutationRepo{
		delegatingRepo: delegatingRepo{inner: dstInner},
		remaining:      2,
		mutate: func(repo *fakeORASRepo) {
			repo.tags["late-dest"] = plan.Artifacts[0].Descriptor
		},
	}
	dst := &Client{
		repoClient:   &orasRepositoryClient{repository: "example/dst", inner: late},
		config:       Config{WorkspaceLayout: WorkspaceLayoutHashed},
		retentionSem: make(chan struct{}, 3),
	}

	report, err := src.CopyTo(ctx, dst, plan)
	if err == nil || !strings.Contains(err.Error(), "unexpected tag") {
		t.Fatalf("CopyTo with a late destination tag = %v, want final-inventory refusal", err)
	}
	if report == nil || len(report.Copied) != len(plan.Artifacts) {
		t.Fatalf("report = %+v, want all planned tags copied before the final check", report)
	}
	if _, err := dstInner.Resolve(ctx, "late-dest"); err != nil {
		t.Fatalf("late tag was removed (copy must not delete): %v", err)
	}
}

func TestCopyTo_DetectsDestinationTagMovementAfterCopy(t *testing.T) {
	ctx := context.Background()
	src, _ := newCopyClient(t, "example/src", WorkspaceLayoutHashed)
	seedCopySource(t, src)
	plan, err := src.PlanCopy(ctx)
	if err != nil {
		t.Fatalf("PlanCopy: %v", err)
	}
	if len(plan.Artifacts) < 2 {
		t.Fatal("need at least two planned artifacts")
	}

	moveTag := plan.Artifacts[0].Tag
	moveDesc := plan.Artifacts[1].Descriptor
	dstInner := newFakeORASRepo()
	late := &lateDestMutationRepo{
		delegatingRepo: delegatingRepo{inner: dstInner},
		remaining:      2,
		mutate: func(repo *fakeORASRepo) {
			repo.tags[moveTag] = moveDesc
		},
	}
	dst := &Client{
		repoClient:   &orasRepositoryClient{repository: "example/dst", inner: late},
		config:       Config{WorkspaceLayout: WorkspaceLayoutHashed},
		retentionSem: make(chan struct{}, 3),
	}

	if _, err := src.CopyTo(ctx, dst, plan); err == nil || !strings.Contains(err.Error(), "moved") {
		t.Fatalf("CopyTo with a late destination tag move = %v, want final-inventory refusal", err)
	}
	if got := resolveFake(t, dstInner, moveTag); got.Digest != moveDesc.Digest {
		t.Fatalf("tag %q = %s, want the concurrent move %s to remain", moveTag, got.Digest, moveDesc.Digest)
	}
}

// addCopyUnlockMarker publishes the exact empty marker retagToUnlocked writes
// and points tag at it. It is used to seed unlocked-* tombstones and locked-*
// aliases without a real registry that rejects DELETE.
func addCopyUnlockMarker(t *testing.T, c *Client, repo *fakeORASRepo, tag, workspace string) ocispec.Descriptor {
	t.Helper()
	ctx := context.Background()
	wc := newWorkspaceClient(c, workspace)
	desc, err := wc.packLockManifest(ctx, "", 0, 0, "")
	if err != nil {
		t.Fatalf("pack unlock marker: %v", err)
	}
	if err := repo.Tag(ctx, desc, tag); err != nil {
		t.Fatalf("tag unlock marker %q: %v", tag, err)
	}
	return desc
}

// addCopyLegacyHistory publishes a state-typed manifest for workspace under a
// literal legacy history tag such as state-foo-v1.
func addCopyLegacyHistory(t *testing.T, c *Client, repo *fakeORASRepo, tag, workspace, version string) ocispec.Descriptor {
	t.Helper()
	ctx := context.Background()
	annotations := map[string]string{annotationWorkspace: workspace}
	if version != "" {
		annotations[annotationStateVersion] = version
	}
	desc, err := newWorkspaceClient(c, workspace).packStateManifestAnnotated(ctx, nil, annotations)
	if err != nil {
		t.Fatalf("pack legacy history %q: %v", tag, err)
	}
	if err := repo.Tag(ctx, desc, tag); err != nil {
		t.Fatalf("tag legacy history %q: %v", tag, err)
	}
	return desc
}

// Regression: a real Lock+Unlock against a registry that rejects DELETE (GHCR
// returns 405) leaves locked-* pointing at the unlocked marker. Planning must
// not mistake that validated marker for a live lock, and must still skip both
// aliases.
func TestCopyAfterRealUnlockOnDeleteUnsupportedRegistry(t *testing.T) {
	for _, tc := range []struct {
		name   string
		layout WorkspaceLayout
	}{
		{"hashed", WorkspaceLayoutHashed},
		{"legacy", WorkspaceLayoutLegacy},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			repo := &deleteUnsupportedRepo{delegatingRepo: delegatingRepo{inner: newFakeORASRepo()}}
			src := &Client{
				repoClient:   &orasRepositoryClient{repository: "example/src", inner: repo},
				config:       Config{WorkspaceLayout: tc.layout, MaxVersions: 2},
				retentionSem: make(chan struct{}, 3),
			}
			if err := src.Put(ctx, "default", []byte(`{"serial":1}`)); err != nil {
				t.Fatalf("seed Put: %v", err)
			}
			src.WaitForRetention()

			lockID, err := src.Lock(ctx, "default", LockInfo{ID: "regression-holder"})
			if err != nil {
				t.Fatalf("Lock: %v", err)
			}
			if err := src.Unlock(ctx, "default", lockID); err != nil {
				t.Fatalf("Unlock on a registry that rejects DELETE: %v", err)
			}

			wsTag := src.workspaceTagFor("default")
			lockTag := lockTagPrefix + wsTag
			unlockedTag := unlockedTagPrefix + wsTag
			lockDesc := resolveFake(t, repo.inner, lockTag)
			unlockedDesc := resolveFake(t, repo.inner, unlockedTag)
			if lockDesc.Digest != unlockedDesc.Digest {
				t.Fatalf("lock tag = %s, unlocked tag = %s; want the same marker", lockDesc.Digest, unlockedDesc.Digest)
			}

			plan, err := src.PlanCopy(ctx)
			if err != nil {
				t.Fatalf("PlanCopy after a real unlock on a registry that rejects DELETE: %v", err)
			}
			for _, tag := range []string{lockTag, unlockedTag} {
				if !slices.Contains(plan.Skipped, tag) {
					t.Fatalf("plan skipped = %v, want validated marker %q", plan.Skipped, tag)
				}
				if _, ok := plan.snapshot[tag]; !ok {
					t.Fatalf("plan snapshot is missing skipped marker %q", tag)
				}
			}

			dst, dstRepo := newCopyClient(t, "example/dst", tc.layout)
			report, err := src.CopyTo(ctx, dst, plan)
			if err != nil {
				t.Fatalf("CopyTo after a real unlock: %v", err)
			}
			if len(report.Copied) != len(plan.Artifacts) {
				t.Fatalf("copied = %v, want %v", copyArtifactTags(report.Copied), copyPlanTags(plan))
			}
			for _, tag := range []string{lockTag, unlockedTag} {
				if _, err := dstRepo.Resolve(ctx, tag); !errors.Is(err, errdef.ErrNotFound) {
					t.Errorf("marker %q was copied: %v", tag, err)
				}
			}
		})
	}
}

// Regression: the old legacy history state-foo-v1 carries workspace annotation
// foo, not the literal tag suffix foo-v1. It must classify as history and copy
// unchanged even when no current state exists.
func TestCopyLegacyHistoryStateFooV1(t *testing.T) {
	ctx := context.Background()
	src, srcRepo := newCopyClient(t, "example/src", WorkspaceLayoutLegacy)
	historyTag := stateTagPrefix + "foo" + stateVersionTagSeparator + "1"
	desc := addCopyLegacyHistory(t, src, srcRepo, historyTag, "foo", "1")

	plan, err := src.PlanCopy(ctx)
	if err != nil {
		t.Fatalf("PlanCopy for legacy history %q: %v", historyTag, err)
	}
	artifact := copyArtifactByTag(t, plan, historyTag)
	if artifact.Kind != ArtifactKindHistory || artifact.Workspace != "foo" {
		t.Fatalf("artifact = %+v, want history for workspace foo", artifact)
	}
	if artifact.Descriptor.Digest != desc.Digest {
		t.Fatalf("pinned digest = %s, want %s", artifact.Descriptor.Digest, desc.Digest)
	}

	dst, dstRepo := newCopyClient(t, "example/dst", WorkspaceLayoutLegacy)
	if _, err := src.CopyTo(ctx, dst, plan); err != nil {
		t.Fatalf("CopyTo legacy history: %v", err)
	}
	if got := resolveFake(t, dstRepo, historyTag); got.Digest != desc.Digest {
		t.Fatalf("destination digest = %s, want %s", got.Digest, desc.Digest)
	}
}

// addCopyTypedArtifact pushes a manifest with an explicit artifact type, layer
// list, and annotations, then tags it. Unlike addCopyArtifact it can reproduce
// lock marker and malformed lock shapes.
func addCopyTypedArtifact(t *testing.T, repo *fakeORASRepo, tag, artifactType string, layers []ocispec.Descriptor, annotations map[string]string) ocispec.Descriptor {
	t.Helper()
	ctx := context.Background()
	config := ocispec.DescriptorEmptyJSON
	if err := repo.Push(ctx, config, bytes.NewReader(config.Data)); err != nil {
		t.Fatalf("push config: %v", err)
	}
	m := ocispec.Manifest{
		Versioned:    specs.Versioned{SchemaVersion: 2},
		MediaType:    ocispec.MediaTypeImageManifest,
		ArtifactType: artifactType,
		Config:       config,
		Layers:       layers,
		Annotations:  annotations,
	}
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal artifact: %v", err)
	}
	desc := ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageManifest,
		Digest:    digest.FromBytes(raw),
		Size:      int64(len(raw)),
	}
	if err := repo.Push(ctx, desc, bytes.NewReader(raw)); err != nil {
		t.Fatalf("push artifact: %v", err)
	}
	if err := repo.Tag(ctx, desc, tag); err != nil {
		t.Fatalf("tag artifact: %v", err)
	}
	return desc
}

// copyMarkerAnnotations returns the exact annotation set packLockManifest
// writes for an empty marker.
func copyMarkerAnnotations(workspace string) map[string]string {
	return map[string]string{
		annotationWorkspace: workspace,
		annotationLockID:    "",
		annotationLockInfo:  "",
		annotationLockGen:   `{"generation":0}`,
	}
}

func copyMarkerLayers() []ocispec.Descriptor {
	return []ocispec.Descriptor{ocispec.DescriptorEmptyJSON}
}

// TestCopyPlan_ValidatesUnlockMarkersByContent proves that tag names under
// locked-*/unlocked-* are not accepted as tombstones: only the exact empty
// marker is skipped, and malformed, active, expired, or unknown content blocks
// the plan in both layouts.
func TestCopyPlan_ValidatesUnlockMarkersByContent(t *testing.T) {
	variants := []struct {
		name         string
		artifactType string
		layers       []ocispec.Descriptor
		annotations  map[string]string
		valid        bool
	}{
		{name: "valid", artifactType: artifactTypeLock, layers: copyMarkerLayers(), annotations: copyMarkerAnnotations("default"), valid: true},
		{name: "missing lock id annotation", artifactType: artifactTypeLock, layers: copyMarkerLayers(), annotations: func() map[string]string {
			a := copyMarkerAnnotations("default")
			delete(a, annotationLockID)
			return a
		}()},
		{name: "missing lock info annotation", artifactType: artifactTypeLock, layers: copyMarkerLayers(), annotations: func() map[string]string {
			a := copyMarkerAnnotations("default")
			delete(a, annotationLockInfo)
			return a
		}()},
		{name: "missing generation annotation", artifactType: artifactTypeLock, layers: copyMarkerLayers(), annotations: func() map[string]string {
			a := copyMarkerAnnotations("default")
			delete(a, annotationLockGen)
			return a
		}()},
		{name: "empty generation object", artifactType: artifactTypeLock, layers: copyMarkerLayers(), annotations: func() map[string]string {
			a := copyMarkerAnnotations("default")
			a[annotationLockGen] = `{}`
			return a
		}()},
		{name: "null generation", artifactType: artifactTypeLock, layers: copyMarkerLayers(), annotations: func() map[string]string {
			a := copyMarkerAnnotations("default")
			a[annotationLockGen] = `null`
			return a
		}()},
		{name: "null generation field", artifactType: artifactTypeLock, layers: copyMarkerLayers(), annotations: func() map[string]string {
			a := copyMarkerAnnotations("default")
			a[annotationLockGen] = `{"generation":null}`
			return a
		}()},
		{name: "invalid generation JSON", artifactType: artifactTypeLock, layers: copyMarkerLayers(), annotations: func() map[string]string {
			a := copyMarkerAnnotations("default")
			a[annotationLockGen] = `{"generation":`
			return a
		}()},
		{name: "nonempty lock info", artifactType: artifactTypeLock, layers: copyMarkerLayers(), annotations: func() map[string]string {
			a := copyMarkerAnnotations("default")
			a[annotationLockInfo] = `{}`
			return a
		}()},
		{name: "nonempty lock id", artifactType: artifactTypeLock, layers: copyMarkerLayers(), annotations: func() map[string]string {
			a := copyMarkerAnnotations("default")
			a[annotationLockID] = "holder"
			return a
		}()},
		{name: "active lock", artifactType: artifactTypeLock, layers: copyMarkerLayers(), annotations: func() map[string]string {
			a := copyMarkerAnnotations("default")
			a[annotationLockID] = "holder"
			a[annotationLockInfo] = `{"ID":"holder"}`
			a[annotationLockGen] = `{"generation":3}`
			return a
		}()},
		{name: "expired lock", artifactType: artifactTypeLock, layers: copyMarkerLayers(), annotations: func() map[string]string {
			a := copyMarkerAnnotations("default")
			a[annotationLockID] = "holder"
			a[annotationLockInfo] = `{"ID":"holder"}`
			a[annotationLockGen] = `{"generation":2,"lease_expiry":1}`
			return a
		}()},
		{name: "holder in generation metadata", artifactType: artifactTypeLock, layers: copyMarkerLayers(), annotations: func() map[string]string {
			a := copyMarkerAnnotations("default")
			a[annotationLockGen] = `{"generation":0,"holder_id":"holder"}`
			return a
		}()},
		{name: "wrong artifact type", artifactType: artifactTypeState, layers: copyMarkerLayers(), annotations: copyMarkerAnnotations("default")},
		{name: "unexpected layer", artifactType: artifactTypeLock, layers: []ocispec.Descriptor{{
			MediaType: mediaTypeStateLayer,
			Digest:    digest.FromString("state"),
			Size:      5,
		}}, annotations: copyMarkerAnnotations("default")},
		{name: "wrong workspace identity", artifactType: artifactTypeLock, layers: copyMarkerLayers(), annotations: copyMarkerAnnotations("other")},
	}

	layouts := []struct {
		name   string
		layout WorkspaceLayout
	}{
		{"hashed", WorkspaceLayoutHashed},
		{"legacy", WorkspaceLayoutLegacy},
	}
	prefixes := []struct {
		name   string
		prefix string
	}{
		{"locked", lockTagPrefix},
		{"unlocked", unlockedTagPrefix},
	}

	for _, layout := range layouts {
		for _, prefix := range prefixes {
			for _, variant := range variants {
				t.Run(layout.name+"/"+prefix.name+"/"+variant.name, func(t *testing.T) {
					ctx := context.Background()
					src, repo := newCopyClient(t, "example/src", layout.layout)
					tag := prefix.prefix + src.workspaceTagFor("default")
					addCopyTypedArtifact(t, repo, tag, variant.artifactType, variant.layers, variant.annotations)

					plan, err := src.PlanCopy(ctx)
					if variant.valid {
						if err != nil {
							t.Fatalf("PlanCopy for a valid marker: %v", err)
						}
						if !slices.Contains(plan.Skipped, tag) {
							t.Fatalf("plan skipped = %v, want %q", plan.Skipped, tag)
						}
						for _, artifact := range plan.Artifacts {
							if artifact.Tag == tag {
								t.Fatalf("marker %q was planned for copying", tag)
							}
						}
						return
					}
					want := "not a valid unlocked marker"
					if prefix.prefix == lockTagPrefix {
						want = "live lock"
					}
					if err == nil || !strings.Contains(err.Error(), want) {
						t.Fatalf("PlanCopy for %s marker content = %v, want %q", variant.name, err, want)
					}
				})
			}
		}
	}
}

// TestCopyPlan_LegacyHistoryIdentity covers old literal history, literal
// workspace names ending in -vN, nested suffixes, and contradictory metadata.
func TestCopyPlan_LegacyHistoryIdentity(t *testing.T) {
	tests := []struct {
		name         string
		tag          string
		workspace    string
		version      string
		artifactType string
		wantKind     ArtifactKind
		wantErr      string
	}{
		{name: "old history without version annotation", tag: "state-foo-v1", workspace: "foo", wantKind: ArtifactKindHistory},
		{name: "old history with matching version", tag: "state-foo-v1", workspace: "foo", version: "1", wantKind: ArtifactKindHistory},
		{name: "literal workspace foo-v1 is current", tag: "state-foo-v1", workspace: "foo-v1", wantKind: ArtifactKindState},
		{name: "nested suffix history", tag: "state-foo-v1-v2", workspace: "foo-v1", version: "2", wantKind: ArtifactKindHistory},
		{name: "contradictory version", tag: "state-foo-v1", workspace: "foo", version: "2", wantErr: "workspace identity mismatch"},
		{name: "wrong artifact type", tag: "state-foo-v1", workspace: "foo", artifactType: "application/vnd.example.v1", wantErr: "workspace identity mismatch"},
		{name: "wrong workspace", tag: "state-foo-v1", workspace: "bar", wantErr: "workspace identity mismatch"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			src, repo := newCopyClient(t, "example/src", WorkspaceLayoutLegacy)
			if tc.artifactType != "" {
				addCopyTypedArtifact(t, repo, tc.tag, tc.artifactType, copyMarkerLayers(), map[string]string{
					annotationWorkspace:    tc.workspace,
					annotationStateVersion: tc.version,
				})
			} else {
				addCopyLegacyHistory(t, src, repo, tc.tag, tc.workspace, tc.version)
			}

			plan, err := src.PlanCopy(ctx)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("PlanCopy = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("PlanCopy: %v", err)
			}
			artifact := copyArtifactByTag(t, plan, tc.tag)
			if artifact.Kind != tc.wantKind || artifact.Workspace != tc.workspace {
				t.Fatalf("artifact = %+v, want kind %s workspace %q", artifact, tc.wantKind, tc.workspace)
			}
		})
	}
}

// TestCopyTo_DetectsSkippedAliasChange proves a validated marker that moves
// after planning blocks the copy before any destination write.
func TestCopyTo_DetectsSkippedAliasChange(t *testing.T) {
	ctx := context.Background()
	src, srcRepo := newCopyClient(t, "example/src", WorkspaceLayoutHashed)
	seedCopySource(t, src)
	markerTag := unlockedTagPrefix + workspaceTagFor("gone")
	addCopyUnlockMarker(t, src, srcRepo, markerTag, "gone")

	plan, err := src.PlanCopy(ctx)
	if err != nil {
		t.Fatalf("PlanCopy: %v", err)
	}

	stateDesc := resolveFake(t, srcRepo, stateTagPrefix+workspaceTagFor("default"))
	if err := srcRepo.Tag(ctx, stateDesc, markerTag); err != nil {
		t.Fatalf("move skipped marker: %v", err)
	}

	dst, dstRepo := newCopyClient(t, "example/dst", WorkspaceLayoutHashed)
	if _, err := src.CopyTo(ctx, dst, plan); err == nil || !strings.Contains(err.Error(), "source changed") {
		t.Fatalf("CopyTo with a moved skipped marker = %v, want source-changed error", err)
	}
	if len(dstRepo.tags) != 0 {
		t.Fatalf("destination was written before the marker change was refused: %v", dstRepo.tags)
	}
}

// TestCopyTo_RejectsTamperedKindAndSkipped proves exported plan fields cannot
// forge a kind or drop a skipped marker.
func TestCopyTo_RejectsTamperedKindAndSkipped(t *testing.T) {
	ctx := context.Background()
	src, srcRepo := newCopyClient(t, "example/src", WorkspaceLayoutLegacy)
	seedCopySource(t, src)
	markerTag := unlockedTagPrefix + src.workspaceTagFor("gone")
	addCopyUnlockMarker(t, src, srcRepo, markerTag, "gone")
	historyTag := stateTagPrefix + "foo" + stateVersionTagSeparator + "1"
	addCopyLegacyHistory(t, src, srcRepo, historyTag, "foo", "1")

	tests := []struct {
		name    string
		mutate  func(*CopyPlan)
		wantErr string
	}{
		{
			name: "exported kind",
			mutate: func(plan *CopyPlan) {
				for i := range plan.Artifacts {
					if plan.Artifacts[i].Tag == historyTag {
						plan.Artifacts[i].Kind = ArtifactKindState
					}
				}
			},
			wantErr: "inconsistent kind",
		},
		{
			name: "dropped skipped tag",
			mutate: func(plan *CopyPlan) {
				plan.Skipped = slices.DeleteFunc(plan.Skipped, func(tag string) bool { return tag == markerTag })
			},
			wantErr: "skipped tags do not match",
		},
		{
			name: "skipped tag copied",
			mutate: func(plan *CopyPlan) {
				plan.Artifacts = append(plan.Artifacts, CopyArtifact{
					Tag:        markerTag,
					Descriptor: plan.snapshot[markerTag],
					Kind:       ArtifactKindUnknown,
				})
			},
			wantErr: "inconsistent kind",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			plan, err := src.PlanCopy(ctx)
			if err != nil {
				t.Fatalf("PlanCopy: %v", err)
			}
			tc.mutate(plan)

			dst, dstRepo := newCopyClient(t, "example/dst", WorkspaceLayoutLegacy)
			if _, err := src.CopyTo(ctx, dst, plan); err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("CopyTo with tampered plan = %v, want %q", err, tc.wantErr)
			}
			if len(dstRepo.tags) != 0 {
				t.Fatalf("destination was written for a tampered plan: %v", dstRepo.tags)
			}
		})
	}
}

// copyRawManifest lets tests emit raw annotation JSON (including null) and an
// explicit config/subject, which ocispec.Manifest cannot represent.
type copyRawManifest struct {
	SchemaVersion int                        `json:"schemaVersion"`
	MediaType     string                     `json:"mediaType"`
	ArtifactType  string                     `json:"artifactType,omitempty"`
	Config        ocispec.Descriptor         `json:"config"`
	Layers        []ocispec.Descriptor       `json:"layers"`
	Subject       *ocispec.Descriptor        `json:"subject,omitempty"`
	Annotations   map[string]json.RawMessage `json:"annotations,omitempty"`
}

func addCopyRawArtifact(t *testing.T, repo *fakeORASRepo, tag string, m copyRawManifest) ocispec.Descriptor {
	t.Helper()
	ctx := context.Background()
	if len(m.Config.Data) > 0 {
		if err := repo.Push(ctx, m.Config, bytes.NewReader(m.Config.Data)); err != nil {
			t.Fatalf("push config: %v", err)
		}
	}
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal raw artifact: %v", err)
	}
	desc := ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageManifest,
		Digest:    digest.FromBytes(raw),
		Size:      int64(len(raw)),
	}
	if err := repo.Push(ctx, desc, bytes.NewReader(raw)); err != nil {
		t.Fatalf("push raw artifact: %v", err)
	}
	if err := repo.Tag(ctx, desc, tag); err != nil {
		t.Fatalf("tag raw artifact: %v", err)
	}
	return desc
}

func copyRawMarkerAnnotations(t *testing.T, workspace string) map[string]json.RawMessage {
	t.Helper()
	quoted, err := json.Marshal(workspace)
	if err != nil {
		t.Fatalf("marshal workspace: %v", err)
	}
	return map[string]json.RawMessage{
		annotationWorkspace: quoted,
		annotationLockID:    json.RawMessage(`""`),
		annotationLockInfo:  json.RawMessage(`""`),
		annotationLockGen:   json.RawMessage(`"{\"generation\":0}"`),
	}
}

// TestCopyUnlockMarkerMatchesPackedMarker validates the predicate against the
// exact bytes packLockManifest writes, not a synthetic approximation.
func TestCopyUnlockMarkerMatchesPackedMarker(t *testing.T) {
	ctx := context.Background()
	src, repo := newCopyClient(t, "example/src", WorkspaceLayoutHashed)
	desc, err := newWorkspaceClient(src, "default").packLockManifest(ctx, "", 0, 0, "")
	if err != nil {
		t.Fatalf("packLockManifest: %v", err)
	}
	data, err := copyManifestFromDescriptor(ctx, repo, desc)
	if err != nil {
		t.Fatalf("copyManifestFromDescriptor: %v", err)
	}
	if !src.isCopyUnlockMarker(data, workspaceTagFor("default")) {
		t.Fatalf("packed marker was not accepted: %+v", data.Manifest)
	}
}

// TestCopyPlan_RejectsUnlockMarkerPayloadAndNullAnnotations covers payload
// config/subject and explicit-null lock metadata that the decoded string map
// cannot distinguish from an empty string.
func TestCopyPlan_RejectsUnlockMarkerPayloadAndNullAnnotations(t *testing.T) {
	subject := ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageManifest,
		Digest:    digest.FromString("subject"),
		Size:      5,
	}
	unexpectedConfig := ocispec.DescriptorEmptyJSON
	unexpectedConfig.MediaType = mediaTypeStateLayer

	tests := []struct {
		name   string
		mutate func(*copyRawManifest)
	}{
		{"unexpected config descriptor", func(m *copyRawManifest) { m.Config = unexpectedConfig }},
		{"unexpected subject", func(m *copyRawManifest) { m.Subject = &subject }},
		{"null lock id", func(m *copyRawManifest) { m.Annotations[annotationLockID] = json.RawMessage("null") }},
		{"null lock info", func(m *copyRawManifest) { m.Annotations[annotationLockInfo] = json.RawMessage("null") }},
	}

	layouts := []struct {
		name   string
		layout WorkspaceLayout
	}{
		{"hashed", WorkspaceLayoutHashed},
		{"legacy", WorkspaceLayoutLegacy},
	}
	prefixes := []struct {
		name   string
		prefix string
	}{
		{"locked", lockTagPrefix},
		{"unlocked", unlockedTagPrefix},
	}

	for _, layout := range layouts {
		for _, prefix := range prefixes {
			for _, tc := range tests {
				t.Run(layout.name+"/"+prefix.name+"/"+tc.name, func(t *testing.T) {
					ctx := context.Background()
					src, repo := newCopyClient(t, "example/src", layout.layout)
					tag := prefix.prefix + src.workspaceTagFor("default")
					m := copyRawManifest{
						SchemaVersion: 2,
						MediaType:     ocispec.MediaTypeImageManifest,
						ArtifactType:  artifactTypeLock,
						Config:        ocispec.DescriptorEmptyJSON,
						Layers:        copyMarkerLayers(),
						Annotations:   copyRawMarkerAnnotations(t, "default"),
					}
					tc.mutate(&m)
					addCopyRawArtifact(t, repo, tag, m)

					want := "not a valid unlocked marker"
					if prefix.prefix == lockTagPrefix {
						want = "live lock"
					}
					if _, err := src.PlanCopy(ctx); err == nil || !strings.Contains(err.Error(), want) {
						t.Fatalf("PlanCopy for %s = %v, want %q", tc.name, err, want)
					}
				})
			}
		}
	}
}

func fetchManifestFake(t *testing.T, repo *fakeORASRepo, desc ocispec.Descriptor) ocispec.Manifest {
	t.Helper()
	data := fetchFake(t, repo, desc)
	var manifest ocispec.Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatalf("decode manifest %s: %v", desc.Digest, err)
	}
	return manifest
}

func TestCopyTo_ResumeRefusesIncompleteGraph(t *testing.T) {
	ctx := context.Background()
	src, srcRepo := newCopyClient(t, "example/src", WorkspaceLayoutHashed)
	seedCopySource(t, src)
	plan, err := src.PlanCopy(ctx)
	if err != nil {
		t.Fatalf("PlanCopy: %v", err)
	}

	dst, dstRepo := newCopyClient(t, "example/dst", WorkspaceLayoutHashed)
	if _, err := src.CopyTo(ctx, dst, plan); err != nil {
		t.Fatalf("first CopyTo: %v", err)
	}

	state := copyArtifactByTag(t, plan, stateTagPrefix+workspaceTagFor("default"))
	manifest := fetchManifestFake(t, dstRepo, state.Descriptor)
	if len(manifest.Layers) == 0 {
		t.Fatal("state manifest has no layer")
	}
	layer := manifest.Layers[0]
	if err := dstRepo.Delete(ctx, ocispec.Descriptor{Digest: layer.Digest}); err != nil {
		t.Fatalf("delete destination layer: %v", err)
	}
	if _, err := srcRepo.Fetch(ctx, layer); err != nil {
		t.Fatalf("source layer was removed: %v", err)
	}

	report, err := src.CopyTo(ctx, dst, plan)
	if err == nil || !strings.Contains(err.Error(), "incomplete or invalid graph") {
		t.Fatalf("resume with a missing destination layer = %v, want refusal", err)
	}
	if report != nil {
		t.Fatalf("report = %+v, want no report before writes", report)
	}
	if got := resolveFake(t, dstRepo, state.Tag); got.Digest != state.Descriptor.Digest {
		t.Fatalf("destination tag %q = %s, want the original pinned digest %s", state.Tag, got.Digest, state.Descriptor.Digest)
	}
}
