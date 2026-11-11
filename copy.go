package orasstate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
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

// CopyPlan is an immutable snapshot of a source repository's tags, pinned to
// the descriptor (and therefore digest) each tag resolved to when the plan was
// made. It is created by PlanCopy and consumed by CopyTo.
//
// A plan is not CAS: the source can change between PlanCopy and CopyTo. CopyTo
// re-inventories the source before writing and again after copying, and fails
// if the tag-to-digest snapshot moved.
type CopyPlan struct {
	source    string
	layout    WorkspaceLayout
	artifacts []CopyArtifact
	skipped   []string
	snapshot  map[string]digest.Digest
}

// Source returns the full source repository reference (registry/repository).
func (p *CopyPlan) Source() string {
	if p == nil {
		return ""
	}
	return p.source
}

// Artifacts returns a copy of the tags that CopyTo will copy, sorted by tag.
func (p *CopyPlan) Artifacts() []CopyArtifact {
	if p == nil {
		return nil
	}
	out := slices.Clone(p.artifacts)
	for i := range out {
		out[i].Descriptor.URLs = slices.Clone(out[i].Descriptor.URLs)
		out[i].Descriptor.Data = slices.Clone(out[i].Descriptor.Data)
		out[i].Descriptor.Annotations = maps.Clone(out[i].Descriptor.Annotations)
	}
	return out
}

// Skipped returns a copy of the validated unlock marker tags (locked-* aliases
// and unlocked-* tombstones) that the plan does not copy.
func (p *CopyPlan) Skipped() []string {
	if p == nil {
		return nil
	}
	return slices.Clone(p.skipped)
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
	// Caller-bounded: a package with many tags outlives the per-operation
	// default deadline.
	tags, err := c.listCopyTags(ctx, false)
	if err != nil {
		return nil, err
	}

	plan := &CopyPlan{
		source:   c.repoClient.repository,
		layout:   c.config.WorkspaceLayout,
		snapshot: make(map[string]digest.Digest, len(tags)),
	}
	for _, tag := range tags {
		if err := (orasProperties.Reference{Tag: tag}).ValidateTag(); err != nil {
			return nil, fmt.Errorf("invalid source tag %q: %w", tag, err)
		}
		id, reserved := workspaceIDFromTag(tag)
		if reserved && c.config.WorkspaceLayout == WorkspaceLayoutHashed && len(id) != 64 {
			return nil, fmt.Errorf("legacy workspace tag %q detected: stop all writers and migrate verified state to an empty repository; do not mix provider storage formats", tag)
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

		if strings.HasPrefix(tag, lockTagPrefix) || strings.HasPrefix(tag, unlockedTagPrefix) {
			// A tag name is not proof of content: GHCR unlocks by retagging
			// locked-* (and unlocked-*) to the empty marker. Only a positively
			// validated marker is skipped; anything else stays a lock.
			manifest, err := copyManifestFromDescriptor(ctx, c.repoClient.inner, desc)
			if err != nil {
				return nil, fmt.Errorf("reading unlock marker for source tag %q: %w", tag, err)
			}
			if !c.isCopyUnlockMarker(manifest, id) {
				if strings.HasPrefix(tag, lockTagPrefix) {
					// Conservative: stale locks are refused too. Retiring a
					// lock is a separate, authorized operation, never part of
					// a copy.
					return nil, fmt.Errorf("source repository has a live lock tag %q; unlock or retire it before copying (copy never removes source locks)", tag)
				}
				return nil, fmt.Errorf("source tag %q is not a valid unlocked marker; refusing to copy ambiguous lock content", tag)
			}
			plan.snapshot[tag] = desc.Digest
			plan.skipped = append(plan.skipped, tag)
			continue
		}

		kind := classifyArtifactKind(tag)
		artifact := CopyArtifact{
			Tag:        tag,
			Descriptor: desc,
			Kind:       kind,
		}
		if kind != ArtifactKindUnknown {
			manifest, err := copyManifestFromDescriptor(ctx, c.repoClient.inner, desc)
			if err != nil {
				return nil, fmt.Errorf("reading workspace metadata for source tag %q: %w", tag, err)
			}
			artifact.Workspace = manifest.Manifest.Annotations[annotationWorkspace]
			if reserved && c.config.WorkspaceLayout == WorkspaceLayoutLegacy {
				// Full literal identity wins, including a workspace whose name
				// ends in -vN (state-foo-v1 for workspace foo-v1).
				matched := legacyWorkspaceTagFor(artifact.Workspace) == id
				if !matched && kind == ArtifactKindState && strings.HasPrefix(tag, stateTagPrefix) {
					// Old history: state-foo-v1 belongs to workspace foo. It is
					// history only when the pinned manifest proves that base
					// identity and an optional version that matches the suffix.
					base, version, ok := splitLegacyStateVersionTag(tag)
					if ok && legacyStateVersionManifestMatches(manifest.Manifest, base, version) {
						artifact.Kind = ArtifactKindHistory
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
		plan.snapshot[tag] = desc.Digest
		plan.artifacts = append(plan.artifacts, artifact)
	}
	return plan, nil
}

// CopyTo copies every artifact pinned by plan from this source into dst, then
// verifies the result. It never deletes or modifies the source, and it never
// deletes destination content.
//
// Before writing anything it requires every destination tag to be absent or to
// be a planned tag with exactly the planned digest and a present graph; any
// other tag or digest is refused. Tags already present with the planned digest
// are a resume, not an overwrite. After writing it re-inventories the
// destination to require the exact planned tag-to-digest set, verifies every
// planned graph is present, and re-inventories the source, failing if the
// source changed.
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

	// The source must still match the plan before any write, which also catches
	// a lock acquired after PlanCopy. Bulk copy uses the caller's context: the
	// single-operation default deadline would abort large migrations.
	current, err := c.inventoryCopyTags(ctx, false)
	if err != nil {
		return nil, fmt.Errorf("inspecting copy source: %w", err)
	}
	if err := compareCopySnapshot(plan.snapshot, current); err != nil {
		return nil, err
	}

	dstTags, err := dst.inventoryCopyTags(ctx, true)
	if err != nil {
		return nil, fmt.Errorf("inspecting copy destination: %w", err)
	}
	expectedArtifacts := make(map[string]CopyArtifact, len(plan.artifacts))
	for _, artifact := range plan.artifacts {
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
	for _, artifact := range plan.artifacts {
		if _, ok := dstTags[artifact.Tag]; ok {
			report.Existing = append(report.Existing, artifact)
			continue
		}
		if err := oras.CopyGraph(ctx, c.repoClient.inner, dst.repoClient.inner, artifact.Descriptor, oras.CopyGraphOptions{}); err != nil {
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
		if err := tagPrePushNotFoundRetry(ctx, dst.repoClient.inner, artifact.Descriptor, artifact.Tag); err != nil {
			return report, fmt.Errorf("tagging destination %q with %s: %w", artifact.Tag, artifact.Descriptor.Digest, err)
		}
		report.Copied = append(report.Copied, artifact)
	}

	// Final destination inventory: require the destination tag set to be
	// exactly the planned set with the planned digests, then walk every planned
	// graph. Skipped tombstones are not expected in the destination, so their
	// presence is refused. This is still not CAS: a change after this inventory
	// is undetected.
	finalDst, err := dst.inventoryCopyTags(ctx, false)
	if err != nil {
		return report, fmt.Errorf("re-inspecting copy destination: %w", err)
	}
	if err := compareCopyDestination(plan.expectedDigests(), finalDst); err != nil {
		return report, err
	}
	for _, artifact := range plan.artifacts {
		if err := verifyCopyGraph(ctx, dst.repoClient.inner, artifact.Descriptor); err != nil {
			return report, fmt.Errorf("verifying destination graph for tag %q: %w", artifact.Tag, err)
		}
	}

	final, err := c.inventoryCopyTags(ctx, false)
	if err != nil {
		return report, fmt.Errorf("re-inspecting copy source: %w", err)
	}
	if err := compareCopySnapshot(plan.snapshot, final); err != nil {
		return report, err
	}
	// Verified is only claimed once every per-tag graph check and both final
	// inventories have passed.
	report.Verified = plan.Artifacts()
	return report, nil
}

// validateCopyTargets rejects same-repository, layout-mismatched, and
// uninitialized plans before any registry read or write.
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
	if plan.source == "" || plan.snapshot == nil {
		return errors.New("copy plan has no source snapshot")
	}
	if plan.source != src.repoClient.repository {
		return errors.New("copy plan belongs to a different source repository")
	}
	if plan.layout != src.config.WorkspaceLayout {
		return errors.New("copy plan was created with a different workspace layout")
	}
	if dst.config.WorkspaceLayout != src.config.WorkspaceLayout {
		return errors.New("copy source and destination use different workspace layouts")
	}
	return nil
}

func (p *CopyPlan) expectedDigests() map[string]digest.Digest {
	expected := make(map[string]digest.Digest, len(p.artifacts))
	for _, artifact := range p.artifacts {
		expected[artifact.Tag] = artifact.Descriptor.Digest
	}
	return expected
}

// listCopyTags lists every tag in the repository, sorted and deduplicated.
// A missing repository is an empty tag list only when allowMissing is set: the
// destination may not exist yet, but a missing source must not silently plan a
// zero-artifact copy. Permission and transport errors are propagated.
func (c *Client) listCopyTags(ctx context.Context, allowMissing bool) ([]string, error) {
	tags, err := listRepoTags(ctx, c.repoClient.inner)
	if isNotFound(err) {
		if allowMissing {
			return nil, nil
		}
		return nil, fmt.Errorf("repository %q not found (check the configured repository): %w", c.repoClient.repository, err)
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
func (c *Client) inventoryCopyTags(ctx context.Context, allowMissing bool) (map[string]ocispec.Descriptor, error) {
	tags, err := c.listCopyTags(ctx, allowMissing)
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
func compareCopySnapshot(want map[string]digest.Digest, got map[string]ocispec.Descriptor) error {
	for _, tag := range slices.Sorted(maps.Keys(want)) {
		desc, ok := got[tag]
		if !ok {
			return fmt.Errorf("source changed since copy plan: tag %q disappeared", tag)
		}
		if desc.Digest != want[tag] {
			return fmt.Errorf("source changed since copy plan: tag %q moved from %s to %s", tag, want[tag], desc.Digest)
		}
	}
	for _, tag := range slices.Sorted(maps.Keys(got)) {
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
	for _, tag := range slices.Sorted(maps.Keys(got)) {
		expected, ok := want[tag]
		if !ok {
			return fmt.Errorf("destination contains unexpected tag %q after copy", tag)
		}
		if got[tag].Digest != expected {
			return fmt.Errorf("destination tag %q moved to %s after copy, expected %s", tag, got[tag].Digest, expected)
		}
	}
	for _, tag := range slices.Sorted(maps.Keys(want)) {
		if _, ok := got[tag]; !ok {
			return fmt.Errorf("destination tag %q disappeared after copy", tag)
		}
	}
	return nil
}

// verifyCopyGraph walks the graph rooted at root and requires every referenced
// manifest and blob to exist in repo. Manifests are verified and parsed through
// content.Successors; layer blobs are checked via Exists without downloading
// every historical state payload.
func verifyCopyGraph(ctx context.Context, repo orasRepository, root ocispec.Descriptor) error {
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

		exists, err := repo.Exists(ctx, desc)
		if err != nil {
			return fmt.Errorf("checking %s: %w", desc.Digest, err)
		}
		if !exists {
			return fmt.Errorf("missing content %s", desc.Digest)
		}

		successors, err := content.Successors(ctx, repo, desc)
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
	data, err := fetchManifestBytes(ctx, repo, desc)
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

// classifyArtifactKind recognizes the package's own copied tag families.
// Anything else is preserved as an unknown artifact rather than dropped.
func classifyArtifactKind(tag string) ArtifactKind {
	if _, reserved := workspaceIDFromTag(tag); !reserved {
		return ArtifactKindUnknown
	}
	switch {
	case strings.HasPrefix(tag, markerTagPrefix):
		return ArtifactKindMarker
	case strings.HasPrefix(tag, stateTagPrefix):
		return ArtifactKindState
	case strings.HasPrefix(tag, stateVersionTagPrefix):
		if _, _, ok := splitStateVersionTag(tag); ok {
			return ArtifactKindHistory
		}
	}
	return ArtifactKindUnknown
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
