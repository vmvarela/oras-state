package orasstate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"slices"
	"strings"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	oras "github.com/oras-project/oras-go/v3"
	"github.com/oras-project/oras-go/v3/content"
	orasProperties "github.com/oras-project/oras-go/v3/registry/remote/properties"
)

// ArtifactKind classifies a tagged artifact in a copy plan.
type ArtifactKind string

const (
	// ArtifactKindState is a current-state tag (state-*).
	ArtifactKindState ArtifactKind = "state"
	// ArtifactKindHistory is a retained history version tag (stver-*-vN).
	ArtifactKindHistory ArtifactKind = "history"
	// ArtifactKindMarker is a workspace marker tag (wsmeta-*).
	ArtifactKindMarker ArtifactKind = "marker"
	// ArtifactKindUnknown is any other tagged OCI artifact. It is copied as-is.
	ArtifactKindUnknown ArtifactKind = "unknown"
)

// copyTagKind is the internal classification, including tags a copy refuses or
// skips.
type copyTagKind uint8

const (
	copyKindState copyTagKind = iota
	copyKindHistory
	copyKindMarker
	copyKindUnknown
	copyKindTombstone
	copyKindLock
)

// CopyArtifact is one tagged artifact pinned by a CopyPlan.
type CopyArtifact struct {
	// Tag is the source tag name.
	Tag string
	// Descriptor is the source descriptor the tag resolved to when planned.
	Descriptor ocispec.Descriptor
	// Kind classifies recognized package tags; other artifacts are unknown.
	Kind ArtifactKind
	// Workspace is the original workspace name from the manifest annotation for
	// recognized tags. It is empty for unknown artifacts and for manifests that
	// carry no workspace annotation.
	Workspace string
}

// CopyPlan is a snapshot of a source repository's tags, pinned to the
// descriptor (and therefore digest) each tag resolved to when the plan was
// made. It is created by PlanCopy and consumed by CopyTo. CopyTo rejects a plan
// whose exported fields no longer match the unexported snapshot.
//
// A plan is not CAS: the source can change between PlanCopy and CopyTo. CopyTo
// re-inventories the source before writing and again after copying, and fails
// if the tag-to-digest snapshot moved.
type CopyPlan struct {
	// Source is the full source repository reference (registry/repository).
	Source string
	// Artifacts are the tags that CopyTo will copy, sorted by tag.
	Artifacts []CopyArtifact
	// Skipped lists validated unlock marker tags (locked-* aliases and
	// unlocked-* tombstones) that the plan does not copy.
	Skipped []string

	// layout, snapshot, tombstones, and kinds bind the exported plan to the
	// source repository and let CopyTo detect mutation or forgery. Snapshots
	// hold the full resolved descriptor, not just the digest, so metadata
	// mutations are rejected too. kinds is the classification pinned while
	// reading the source content, so validate does not re-derive it from the
	// tag name alone.
	layout     WorkspaceLayout
	snapshot   map[string]ocispec.Descriptor
	tombstones map[string]ocispec.Descriptor
	kinds      map[string]copyTagKind
}

// CopyReport describes what CopyTo did. A non-nil report may be returned with
// an error when a copy or verification fails after some tags were written.
type CopyReport struct {
	// Copied lists artifacts written to the destination by this call.
	Copied []CopyArtifact
	// Existing lists artifacts already present in the destination with the
	// pinned digest, so they were not rewritten.
	Existing []CopyArtifact
	// Verified lists artifacts whose destination tag resolved to the pinned
	// digest and whose graph was present after copying. It is populated only
	// after the final destination and source inventories pass.
	Verified []CopyArtifact
}

// PlanCopy inventories every tag in the source repository and pins its current
// tag-to-descriptor mapping. It refuses to plan while an active locked-* tag
// exists, skips validated unlock markers, and rejects provider tags that do not
// match the configured WorkspaceLayout. Every other tagged artifact is copied,
// including tags this package does not recognize.
//
// The plan is bound to this repository and workspace layout. It is a snapshot,
// not CAS: CopyTo checks it before and after copying.
func (c *Client) PlanCopy(ctx context.Context) (*CopyPlan, error) {
	if c == nil || c.repoClient == nil || c.repoClient.inner == nil {
		return nil, errors.New("copy source client is not configured")
	}
	ctx, cancel := c.opContext(ctx)
	defer cancel()

	tags, err := c.listCopyTags(ctx)
	if err != nil {
		return nil, err
	}
	if c.config.WorkspaceLayout == WorkspaceLayoutHashed {
		// Same repository-wide check the state engine performs: the hashed
		// layout must not contain legacy literal workspace tags.
		if _, err := reservedWorkspaceTags(ctx, c.repoClient); err != nil {
			return nil, err
		}
	}

	plan := &CopyPlan{
		Source:     c.repoClient.repository,
		layout:     c.config.WorkspaceLayout,
		snapshot:   make(map[string]ocispec.Descriptor, len(tags)),
		tombstones: make(map[string]ocispec.Descriptor),
		kinds:      make(map[string]copyTagKind, len(tags)),
	}
	for _, tag := range tags {
		if err := (orasProperties.Reference{Tag: tag}).ValidateTag(); err != nil {
			return nil, fmt.Errorf("invalid source tag %q: %w", tag, err)
		}
		desc, err := resolveCopyTag(ctx, c.repoClient.inner, tag)
		if isNotFound(err) {
			// The tag was listed but is no longer resolvable: the inventory
			// moved during planning. Pinning a partial view would silently
			// produce a stale plan, so fail closed and make the caller re-plan.
			return nil, fmt.Errorf("source tag %q disappeared while planning; re-run PlanCopy", tag)
		}
		if err != nil {
			return nil, fmt.Errorf("resolving source tag %q: %w", tag, err)
		}

		kind := classifyCopyTag(tag)
		id, reserved := workspaceIDFromTag(tag)
		if reserved && c.config.WorkspaceLayout == WorkspaceLayoutHashed && len(id) != 64 {
			// reservedWorkspaceTags already checked the earlier listing;
			// re-check the tag actually being pinned so a tag that appeared
			// between the two listings cannot slip through.
			return nil, fmt.Errorf("legacy workspace tag %q detected: stop all writers and migrate verified state to an empty repository; do not mix provider storage formats (see docs/guides/workspace-migration.md)", tag)
		}

		if kind == copyKindLock || kind == copyKindTombstone {
			// A tag name is not proof of content: GHCR unlocks by retagging
			// locked-* (and unlocked-*) to the empty marker. Only a positively
			// validated marker is skipped; anything else stays a lock.
			manifest, err := copyManifestFromDescriptor(ctx, c.repoClient.inner, desc)
			if err != nil {
				return nil, fmt.Errorf("reading unlock marker for source tag %q: %w", tag, err)
			}
			if !c.isCopyUnlockMarker(manifest, id) {
				if kind == copyKindLock {
					// Conservative: stale locks are refused too. Retiring a
					// lock is a separate, authorized operation, never part of
					// a copy.
					return nil, fmt.Errorf("source repository has a live lock tag %q; unlock or retire it before copying (copy never removes source locks)", tag)
				}
				return nil, fmt.Errorf("source tag %q is not a valid unlocked marker; refusing to copy ambiguous lock content", tag)
			}
			plan.snapshot[tag] = desc
			plan.kinds[tag] = copyKindTombstone
			plan.tombstones[tag] = desc
			plan.Skipped = append(plan.Skipped, tag)
			continue
		}

		plan.snapshot[tag] = desc
		plan.kinds[tag] = kind
		artifact := CopyArtifact{
			Tag:        tag,
			Descriptor: desc,
			Kind:       artifactKind(kind),
		}
		if kind != copyKindUnknown {
			manifest, err := copyManifestFromDescriptor(ctx, c.repoClient.inner, desc)
			if err != nil {
				return nil, fmt.Errorf("reading workspace metadata for source tag %q: %w", tag, err)
			}
			artifact.Workspace = manifest.Manifest.Annotations[annotationWorkspace]
			if reserved && c.config.WorkspaceLayout == WorkspaceLayoutLegacy {
				// Full literal identity wins, including a workspace whose name
				// ends in -vN (state-foo-v1 for workspace foo-v1).
				matched := legacyWorkspaceTagFor(artifact.Workspace) == id
				if !matched && kind == copyKindState && strings.HasPrefix(tag, stateTagPrefix) {
					// Old history: state-foo-v1 belongs to workspace foo. It is
					// history only when the pinned manifest proves that base
					// identity and an optional version that matches the suffix.
					base, version, ok := splitLegacyStateVersionTag(tag)
					if ok && legacyStateVersionManifestMatches(manifest.Manifest, base, version) {
						kind = copyKindHistory
						plan.kinds[tag] = kind
						artifact.Kind = artifactKind(kind)
						matched = true
					}
				}
				if !matched {
					if isHashedWorkspaceID(id) {
						return nil, fmt.Errorf("source tag %q uses the hashed workspace layout but this client is configured for the legacy layout", tag)
					}
					return nil, fmt.Errorf("workspace identity mismatch at source tag %q; refusing to copy invalid provider metadata", tag)
				}
			}
			if reserved && c.config.WorkspaceLayout == WorkspaceLayoutHashed && id != workspaceTagFor(artifact.Workspace) {
				return nil, fmt.Errorf("workspace identity mismatch at source tag %q; refusing to copy invalid provider metadata", tag)
			}
		}
		plan.Artifacts = append(plan.Artifacts, artifact)
	}
	slices.SortFunc(plan.Artifacts, func(a, b CopyArtifact) int {
		return strings.Compare(a.Tag, b.Tag)
	})
	slices.Sort(plan.Skipped)
	return plan, nil
}

// CopyTo copies every artifact pinned by plan from this source into dst, then
// verifies the result. It never deletes or modifies the source, and it never
// deletes destination content.
//
// Before writing anything it requires every destination tag to be absent or to
// be a planned tag with exactly the planned digest and a present graph; any
// other tag or digest is refused. Tags already present with the planned digest
// are a resume, not an overwrite. After writing it verifies every destination
// tag resolves to the pinned digest and its graph is present, re-inventories
// the destination to require exactly the planned tag set, and re-inventories
// the source, failing if the source changed.
//
// This is not CAS. A writer can still move a destination tag between the
// pre-write re-resolve and Tag, or after the final inventory; the checks narrow
// those windows but do not close them.
//
// Manifests and blobs are copied byte-for-byte by digest through ORAS graph
// copy, so aliases and unknown tagged OCI artifacts are preserved. ORAS follows
// a manifest's Subject by default, so an unknown artifact that has a subject
// also copies that subject graph; referrers/attestations are not discovered
// through the referrers API. Only tags are inspected: untagged package
// versions in dst are invisible here and are neither verified nor claimed
// absent.
func (c *Client) CopyTo(ctx context.Context, dst *Client, plan *CopyPlan) (*CopyReport, error) {
	if err := validateCopyTargets(c, dst, plan); err != nil {
		return nil, err
	}
	ctx, cancel := c.opContext(ctx)
	defer cancel()

	// The source must still match the plan before any write. This also catches
	// a lock acquired after PlanCopy.
	current, err := c.inventoryCopyTags(ctx)
	if err != nil {
		return nil, fmt.Errorf("inspecting copy source: %w", err)
	}
	if err := compareCopySnapshot(plan.snapshot, current); err != nil {
		return nil, err
	}

	dstTags, err := dst.inventoryCopyTags(ctx)
	if err != nil {
		return nil, fmt.Errorf("inspecting copy destination: %w", err)
	}
	expectedArtifacts := make(map[string]CopyArtifact, len(plan.Artifacts))
	for _, artifact := range plan.Artifacts {
		expectedArtifacts[artifact.Tag] = artifact
	}
	for tag, desc := range dstTags {
		artifact, ok := expectedArtifacts[tag]
		if !ok {
			return nil, fmt.Errorf("destination repository already contains tag %q, which is not in the copy plan; refusing to mix packages", tag)
		}
		if desc.Digest != artifact.Descriptor.Digest {
			return nil, fmt.Errorf("destination tag %q resolves to %s, plan expects %s; refusing to overwrite", tag, desc.Digest, artifact.Descriptor.Digest)
		}
		// A matching tag/digest is not enough to resume: the graph it names
		// must be present too. This is a read-only preflight before any write.
		// The pinned descriptor (not the destination's metadata) drives the
		// graph walk.
		if err := verifyCopyGraph(ctx, dst.repoClient.inner, artifact.Descriptor); err != nil {
			return nil, fmt.Errorf("destination tag %q has an incomplete or invalid graph: %w", tag, err)
		}
	}

	report := &CopyReport{}
	srcStorage := copyStorage{c.repoClient.inner}
	dstStorage := copyStorage{dst.repoClient.inner}
	for _, artifact := range plan.Artifacts {
		if _, ok := dstTags[artifact.Tag]; ok {
			report.Existing = append(report.Existing, artifact)
			continue
		}
		if err := oras.CopyGraph(ctx, srcStorage, dstStorage, artifact.Descriptor, oras.CopyGraphOptions{}); err != nil {
			return report, fmt.Errorf("copying tag %q (%s): %w", artifact.Tag, artifact.Descriptor.Digest, err)
		}
		// Re-resolve immediately before tagging so we never knowingly
		// overwrite a tag that appeared during the graph copy. This is not CAS:
		// a writer can still race this check, which is why the final
		// destination inventory below is a separate, best-effort check.
		desc, err := resolveCopyTag(ctx, dst.repoClient.inner, artifact.Tag)
		switch {
		case err == nil:
			if desc.Digest == artifact.Descriptor.Digest {
				report.Existing = append(report.Existing, artifact)
				continue
			}
			return report, fmt.Errorf("destination tag %q appeared with %s; refusing to overwrite", artifact.Tag, desc.Digest)
		case !isNotFound(err):
			return report, fmt.Errorf("checking destination tag %q before writing: %w", artifact.Tag, err)
		}
		if err := dst.repoClient.inner.Tag(ctx, artifact.Descriptor, artifact.Tag); err != nil {
			return report, fmt.Errorf("tagging destination %q with %s: %w", artifact.Tag, artifact.Descriptor.Digest, err)
		}
		report.Copied = append(report.Copied, artifact)
	}

	verified := make([]CopyArtifact, 0, len(plan.Artifacts))
	for _, artifact := range plan.Artifacts {
		desc, err := resolveCopyTag(ctx, dst.repoClient.inner, artifact.Tag)
		if err != nil {
			return report, fmt.Errorf("verifying destination tag %q: %w", artifact.Tag, err)
		}
		if desc.Digest != artifact.Descriptor.Digest {
			return report, fmt.Errorf("destination tag %q resolves to %s after copy, expected %s", artifact.Tag, desc.Digest, artifact.Descriptor.Digest)
		}
		if err := verifyCopyGraph(ctx, dst.repoClient.inner, artifact.Descriptor); err != nil {
			return report, fmt.Errorf("verifying destination graph for tag %q: %w", artifact.Tag, err)
		}
		verified = append(verified, artifact)
	}

	// Final destination inventory: require the destination tag set to be
	// exactly the planned set with the planned digests. This catches a tag that
	// appeared or moved after the initial preflight. Skipped tombstones are not
	// expected in the destination, so their presence is refused. This is still
	// not CAS: a change after this inventory is undetected.
	finalDst, err := dst.inventoryCopyTags(ctx)
	if err != nil {
		return report, fmt.Errorf("re-inspecting copy destination: %w", err)
	}
	if err := compareCopyDestination(plan.expectedDigests(), finalDst); err != nil {
		return report, err
	}

	final, err := c.inventoryCopyTags(ctx)
	if err != nil {
		return report, fmt.Errorf("re-inspecting copy source: %w", err)
	}
	if err := compareCopySnapshot(plan.snapshot, final); err != nil {
		return report, err
	}
	// Verified is only claimed once every per-tag graph check and both final
	// inventories have passed.
	report.Verified = verified
	return report, nil
}

// validateCopyTargets rejects same-repository, layout-mismatched, and
// inconsistent plans before any registry read or write.
func validateCopyTargets(src, dst *Client, plan *CopyPlan) error {
	if src == nil || src.repoClient == nil || src.repoClient.inner == nil {
		return errors.New("copy source client is not configured")
	}
	if dst == nil || dst.repoClient == nil || dst.repoClient.inner == nil {
		return errors.New("copy destination client is not configured")
	}
	if src == dst || src.repoClient.repository == dst.repoClient.repository {
		return fmt.Errorf("copy source and destination are the same repository %q", src.repoClient.repository)
	}
	if plan == nil {
		return errors.New("copy plan is nil")
	}
	if plan.Source != src.repoClient.repository {
		return errors.New("copy plan belongs to a different source repository")
	}
	if plan.layout != src.config.WorkspaceLayout {
		return errors.New("copy plan was created with a different workspace layout")
	}
	if dst.config.WorkspaceLayout != src.config.WorkspaceLayout {
		return errors.New("copy source and destination use different workspace layouts")
	}
	return plan.validate()
}

// validate checks that the plan is internally consistent and unmodified.
func (p *CopyPlan) validate() error {
	if p.Source == "" || p.snapshot == nil || p.kinds == nil {
		return errors.New("copy plan has no source snapshot")
	}
	seen := make(map[string]struct{}, len(p.snapshot))
	for _, artifact := range p.Artifacts {
		if artifact.Tag == "" || artifact.Descriptor.Digest == "" {
			return errors.New("copy plan contains an empty tag or digest")
		}
		pinned, ok := p.snapshot[artifact.Tag]
		if !ok || !descriptorsEqual(pinned, artifact.Descriptor) {
			return fmt.Errorf("copy plan is inconsistent for tag %q", artifact.Tag)
		}
		kind, ok := p.kinds[artifact.Tag]
		if !ok || kind == copyKindLock || kind == copyKindTombstone || artifact.Kind != artifactKind(kind) {
			return fmt.Errorf("copy plan has an inconsistent kind for tag %q", artifact.Tag)
		}
		if _, skipped := p.tombstones[artifact.Tag]; skipped {
			return fmt.Errorf("copy plan would copy skipped tombstone %q", artifact.Tag)
		}
		if _, dup := seen[artifact.Tag]; dup {
			return fmt.Errorf("copy plan contains duplicate tag %q", artifact.Tag)
		}
		seen[artifact.Tag] = struct{}{}
	}
	if len(p.Skipped) != len(p.tombstones) {
		return errors.New("copy plan skipped tags do not match the source snapshot")
	}
	for _, tag := range p.Skipped {
		pinned, ok := p.snapshot[tag]
		if !ok {
			return fmt.Errorf("copy plan skips unknown tag %q", tag)
		}
		if tombstone, ok := p.tombstones[tag]; !ok || !descriptorsEqual(tombstone, pinned) {
			return fmt.Errorf("copy plan skips a non-tombstone tag %q", tag)
		}
		if kind, ok := p.kinds[tag]; !ok || kind != copyKindTombstone {
			return fmt.Errorf("copy plan skips a non-tombstone tag %q", tag)
		}
		if _, dup := seen[tag]; dup {
			return fmt.Errorf("copy plan contains duplicate tag %q", tag)
		}
		seen[tag] = struct{}{}
	}
	if len(seen) != len(p.snapshot) {
		return errors.New("copy plan does not account for every source tag")
	}
	return nil
}

func (p *CopyPlan) expectedDigests() map[string]digest.Digest {
	expected := make(map[string]digest.Digest, len(p.Artifacts))
	for _, artifact := range p.Artifacts {
		expected[artifact.Tag] = artifact.Descriptor.Digest
	}
	return expected
}

// listCopyTags lists every tag in the repository, sorted and deduplicated.
// A missing repository is an empty tag list, matching the rest of the package;
// permission and transport errors are propagated.
func (c *Client) listCopyTags(ctx context.Context) ([]string, error) {
	var tags []string
	err := retry(ctx, func(ctx context.Context) error {
		tags = nil
		return c.repoClient.inner.Tags(ctx, "", func(page []string) error {
			tags = append(tags, page...)
			return nil
		})
	})
	if isNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("listing repository tags: %w", err)
	}
	slices.Sort(tags)
	return slices.Compact(tags), nil
}

// inventoryCopyTags resolves every tag once to its current descriptor. A tag
// that was listed but no longer resolves moved during inventory; fail closed
// instead of treating it as absent. A tag whose content is missing still
// resolves and is pinned (graph verification will fail if its content is
// needed).
func (c *Client) inventoryCopyTags(ctx context.Context) (map[string]ocispec.Descriptor, error) {
	tags, err := c.listCopyTags(ctx)
	if err != nil {
		return nil, err
	}
	inventory := make(map[string]ocispec.Descriptor, len(tags))
	for _, tag := range tags {
		desc, err := resolveCopyTag(ctx, c.repoClient.inner, tag)
		if isNotFound(err) {
			return nil, fmt.Errorf("tag %q disappeared while inventorying; refusing to continue", tag)
		}
		if err != nil {
			return nil, fmt.Errorf("resolving tag %q: %w", tag, err)
		}
		inventory[tag] = desc
	}
	return inventory, nil
}

// compareCopySnapshot fails if the source inventory differs from the plan in
// any way, including tombstones and tags the copy does not write. It compares
// digests: the manifest bytes are content-addressed, while descriptor metadata
// reported by a registry may legitimately vary between reads.
func compareCopySnapshot(want, got map[string]ocispec.Descriptor) error {
	wantTags := make([]string, 0, len(want))
	for tag := range want {
		wantTags = append(wantTags, tag)
	}
	slices.Sort(wantTags)
	for _, tag := range wantTags {
		desc, ok := got[tag]
		if !ok {
			return fmt.Errorf("source changed since copy plan: tag %q disappeared", tag)
		}
		if desc.Digest != want[tag].Digest {
			return fmt.Errorf("source changed since copy plan: tag %q moved from %s to %s", tag, want[tag].Digest, desc.Digest)
		}
	}
	gotTags := make([]string, 0, len(got))
	for tag := range got {
		gotTags = append(gotTags, tag)
	}
	slices.Sort(gotTags)
	for _, tag := range gotTags {
		if _, ok := want[tag]; !ok {
			return fmt.Errorf("source changed since copy plan: tag %q appeared", tag)
		}
	}
	return nil
}

// compareCopyDestination requires the destination tag set to be exactly the
// planned set with the planned digests. Skipped tombstones are not part of the
// expected set, so their presence is an error.
func compareCopyDestination(want map[string]digest.Digest, got map[string]ocispec.Descriptor) error {
	gotTags := make([]string, 0, len(got))
	for tag := range got {
		gotTags = append(gotTags, tag)
	}
	slices.Sort(gotTags)
	for _, tag := range gotTags {
		expected, ok := want[tag]
		if !ok {
			return fmt.Errorf("destination contains unexpected tag %q after copy", tag)
		}
		if got[tag].Digest != expected {
			return fmt.Errorf("destination tag %q moved to %s after copy, expected %s", tag, got[tag].Digest, expected)
		}
	}
	wantTags := make([]string, 0, len(want))
	for tag := range want {
		wantTags = append(wantTags, tag)
	}
	slices.Sort(wantTags)
	for _, tag := range wantTags {
		desc, ok := got[tag]
		if !ok {
			return fmt.Errorf("destination tag %q disappeared after copy", tag)
		}
		if desc.Digest != want[tag] {
			return fmt.Errorf("destination tag %q moved to %s after copy, expected %s", tag, desc.Digest, want[tag])
		}
	}
	return nil
}

func descriptorsEqual(a, b ocispec.Descriptor) bool {
	return reflect.DeepEqual(a, b)
}

// verifyCopyGraph walks the graph rooted at root and requires every referenced
// manifest and blob to exist in repo. It parses manifests (through
// content.Successors) but does not re-hash blob bytes: the registry CAS is
// trusted for content integrity, as elsewhere in this package.
func verifyCopyGraph(ctx context.Context, repo orasRepository, root ocispec.Descriptor) error {
	storage := copyStorage{repo}
	seen := make(map[digest.Digest]struct{})
	var walk func(ocispec.Descriptor) error
	walk = func(desc ocispec.Descriptor) error {
		if desc.Digest == "" {
			return errors.New("graph contains a descriptor with an empty digest")
		}
		if _, ok := seen[desc.Digest]; ok {
			return nil
		}
		seen[desc.Digest] = struct{}{}

		exists, err := storage.Exists(ctx, desc)
		if err != nil {
			return fmt.Errorf("checking %s: %w", desc.Digest, err)
		}
		if !exists {
			return fmt.Errorf("missing content %s", desc.Digest)
		}

		successors, err := content.Successors(ctx, storage, desc)
		if err != nil {
			return fmt.Errorf("reading successors of %s: %w", desc.Digest, err)
		}
		for _, successor := range successors {
			if err := walk(successor); err != nil {
				return err
			}
		}
		return nil
	}
	return walk(root)
}

func resolveCopyTag(ctx context.Context, repo orasRepository, tag string) (ocispec.Descriptor, error) {
	return retryWithResult(ctx, func(ctx context.Context) (ocispec.Descriptor, error) {
		return repo.Resolve(ctx, tag)
	})
}

// copyManifestData pairs the decoded manifest with its raw annotation JSON so
// marker validation can distinguish an explicit empty string from null or an
// absent key. Decoding into map[string]string loses that distinction: JSON null
// unmarshals into an empty string without error.
type copyManifestData struct {
	Manifest       ocispec.Manifest
	rawAnnotations map[string]json.RawMessage
}

// copyManifestFromDescriptor reads and decodes an already-pinned manifest. It
// never re-resolves a tag, so classification follows the snapshot descriptor.
func copyManifestFromDescriptor(ctx context.Context, repo orasRepository, desc ocispec.Descriptor) (copyManifestData, error) {
	rc, err := repo.Fetch(ctx, desc)
	if err != nil {
		return copyManifestData{}, err
	}
	defer func() { _ = rc.Close() }()
	data, err := io.ReadAll(io.LimitReader(rc, maxManifestSize))
	if err != nil {
		return copyManifestData{}, err
	}
	var manifest ocispec.Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return copyManifestData{}, fmt.Errorf("decoding manifest %s: %w", desc.Digest, err)
	}
	var raw struct {
		Annotations map[string]json.RawMessage `json:"annotations"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return copyManifestData{}, fmt.Errorf("decoding manifest annotations %s: %w", desc.Digest, err)
	}
	return copyManifestData{Manifest: manifest, rawAnnotations: raw.Annotations}, nil
}

// isCopyUnlockMarker reports whether data is exactly the empty marker
// packLockManifest(ctx, "", 0, 0, "") writes for the workspace whose provider
// tag suffix is id. Only this positive shape is skipped: permissive parsers
// accept absent, null, or empty metadata as zero values, which would turn any
// content under locked-*/unlocked-* into a tombstone.
func (c *Client) isCopyUnlockMarker(data copyManifestData, id string) bool {
	m := data.Manifest
	if m.ArtifactType != artifactTypeLock || !isEmptyJSONDescriptor(m.Config) ||
		m.Subject != nil || !isUnlockMarkerLayers(m.Layers) {
		return false
	}
	workspace, ok := m.Annotations[annotationWorkspace]
	if !ok || workspace == "" || c.workspaceTagFor(workspace) != id {
		return false
	}
	if raw, ok := data.rawAnnotations[annotationLockID]; !ok || !isExplicitEmptyJSONString(raw) {
		return false
	}
	if raw, ok := data.rawAnnotations[annotationLockInfo]; !ok || !isExplicitEmptyJSONString(raw) {
		return false
	}
	raw, ok := m.Annotations[annotationLockGen]
	if !ok {
		return false
	}
	return isZeroLockGeneration(raw)
}

// isExplicitEmptyJSONString accepts only the JSON string "". JSON null,
// missing keys, and non-empty strings are rejected.
func isExplicitEmptyJSONString(raw json.RawMessage) bool {
	trimmed := bytes.TrimSpace(raw)
	return len(trimmed) == 2 && trimmed[0] == '"' && trimmed[1] == '"'
}

// isEmptyJSONDescriptor reports whether desc names the empty JSON blob the OCI
// packer uses for marker configs and placeholder layers. Data is optional.
func isEmptyJSONDescriptor(desc ocispec.Descriptor) bool {
	return desc.MediaType == ocispec.MediaTypeEmptyJSON &&
		desc.Digest == ocispec.DescriptorEmptyJSON.Digest &&
		desc.Size == ocispec.DescriptorEmptyJSON.Size
}

// isUnlockMarkerLayers accepts only the content-free layer shape
// PackManifestVersion1_1 produces when packLockManifest passes no layers: the
// single empty JSON descriptor (or, for other packer versions, no layers).
func isUnlockMarkerLayers(layers []ocispec.Descriptor) bool {
	if len(layers) == 0 {
		return true
	}
	if len(layers) != 1 {
		return false
	}
	return isEmptyJSONDescriptor(layers[0])
}

// isZeroLockGeneration accepts only the generation object packLockManifest
// produces for an empty marker: a JSON object whose sole key is generation:0.
// Absent, null, empty, or extra keys (holder/lease) are rejected.
func isZeroLockGeneration(raw string) bool {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &fields); err != nil || len(fields) != 1 {
		return false
	}
	gen, ok := fields["generation"]
	if !ok {
		return false
	}
	var n *int64
	if err := json.Unmarshal(gen, &n); err != nil || n == nil || *n != 0 {
		return false
	}
	return true
}

// classifyCopyTag recognizes the package's own tag families. Anything else is
// preserved as an unknown artifact rather than dropped.
func classifyCopyTag(tag string) copyTagKind {
	switch {
	case strings.HasPrefix(tag, lockTagPrefix):
		return copyKindLock
	case strings.HasPrefix(tag, unlockedTagPrefix):
		return copyKindTombstone
	}
	if _, reserved := workspaceIDFromTag(tag); !reserved {
		return copyKindUnknown
	}
	switch {
	case strings.HasPrefix(tag, markerTagPrefix):
		return copyKindMarker
	case strings.HasPrefix(tag, stateTagPrefix):
		return copyKindState
	case strings.HasPrefix(tag, stateVersionTagPrefix):
		if _, _, ok := splitStateVersionTag(tag); ok {
			return copyKindHistory
		}
	}
	return copyKindUnknown
}

// isHashedWorkspaceID reports whether id has the exact shape produced by the
// hashed layout: 64 lowercase hexadecimal characters.
func isHashedWorkspaceID(id string) bool {
	if len(id) != 64 {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func artifactKind(kind copyTagKind) ArtifactKind {
	switch kind {
	case copyKindState:
		return ArtifactKindState
	case copyKindHistory:
		return ArtifactKindHistory
	case copyKindMarker:
		return ArtifactKindMarker
	default:
		return ArtifactKindUnknown
	}
}

// copyStorage adapts the package's minimal orasRepository to ORAS
// content.Storage. Existence is delegated to the repository when it supports
// it; otherwise it is answered by resolving the digest, which is the same CAS
// check at the registry.
type copyStorage struct {
	repo orasRepository
}

func (s copyStorage) Push(ctx context.Context, expected ocispec.Descriptor, content io.Reader) error {
	return s.repo.Push(ctx, expected, content)
}

func (s copyStorage) Fetch(ctx context.Context, target ocispec.Descriptor) (io.ReadCloser, error) {
	return s.repo.Fetch(ctx, target)
}

func (s copyStorage) Exists(ctx context.Context, target ocispec.Descriptor) (bool, error) {
	if exister, ok := s.repo.(interface {
		Exists(context.Context, ocispec.Descriptor) (bool, error)
	}); ok {
		return exister.Exists(ctx, target)
	}
	_, err := retryWithResult(ctx, func(ctx context.Context) (ocispec.Descriptor, error) {
		return s.repo.Resolve(ctx, target.Digest.String())
	})
	if err != nil {
		if isNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
