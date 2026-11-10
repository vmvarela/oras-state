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
	addCopyArtifact(t, srcRepo, tombstoneTag, map[string]string{annotationWorkspace: "gone"})

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
