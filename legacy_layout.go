package orasstate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"golang.org/x/sync/errgroup"
	orasRegistry "oras.land/oras-go/v2/registry"
)

const legacyVersionWorkspaceValidationLimitPerBase = 16

// legacyWorkspaceTagFor reproduces the exact workspace tag mapping used
// by the legacy backend layout. It is intentionally not the default layout.
func legacyWorkspaceTagFor(workspace string) string {
	ref := orasRegistry.Reference{Reference: workspace}
	if err := ref.ValidateReferenceAsTag(); err == nil {
		return workspace
	}
	h := sha256.Sum256([]byte(workspace))
	return "ws-" + hex.EncodeToString(h[:8])
}

// checkLegacyWorkspace validates only the tags owned by this workspace.
// Unlike the strict hashed layout, it deliberately does not reject a repository
// merely because literal legacy tags exist.
func (wc *workspaceClient) checkLegacyWorkspace(ctx context.Context) error {
	for _, tag := range []string{wc.stateTag, wc.lockTag, wc.unlockedTag, wc.versionTagFor(maxStateVersion)} {
		if err := (orasRegistry.Reference{Reference: tag}).ValidateReferenceAsTag(); err != nil {
			return fmt.Errorf("invalid workspace tag: %w", err)
		}
	}

	// Existing legacy manifests already carry org.terraform.workspace. Verify
	// our own mutable tags when present so compatibility does not weaken
	// identity checking for state that already exists.
	for _, tag := range []string{wc.stateTag, wc.lockTag, wc.unlockedTag} {
		m, _, err := wc.fetchManifestWithDesc(ctx, tag)
		if isNotFound(err) {
			continue
		}
		if err != nil {
			return err
		}
		if name := m.Annotations[annotationWorkspace]; name != "" && name != wc.stateID {
			return fmt.Errorf("workspace identity mismatch at tag %q", tag)
		}
	}
	return nil
}

func listLegacyWorkspaces(ctx context.Context, repo *orasRepositoryClient) ([]string, error) {
	var tags []string
	if err := repo.inner.Tags(ctx, "", func(page []string) error {
		tags = append(tags, page...)
		return nil
	}); err != nil {
		if isNotFound(err) {
			return []string{"default"}, nil
		}
		return nil, err
	}

	tagSet := make(map[string]struct{}, len(tags))
	for _, tag := range tags {
		tagSet[tag] = struct{}{}
	}

	type legacyVersionCandidate struct {
		tag     string
		base    string
		version int
	}

	var relevantTags []string
	ambiguousByBase := make(map[string][]legacyVersionCandidate)
	for _, tag := range tags {
		if !strings.HasPrefix(tag, stateTagPrefix) {
			continue
		}
		if base, version, ok := splitLegacyStateVersionTag(tag); ok {
			if _, exists := tagSet[base]; exists {
				ambiguousByBase[base] = append(ambiguousByBase[base], legacyVersionCandidate{
					tag: tag, base: base, version: version,
				})
				continue
			}
		}
		relevantTags = append(relevantTags, tag)
	}

	var validateLegacyTags []legacyVersionCandidate
	for _, candidates := range ambiguousByBase {
		sort.Slice(candidates, func(i, j int) bool {
			if candidates[i].version == candidates[j].version {
				return candidates[i].tag < candidates[j].tag
			}
			return candidates[i].version < candidates[j].version
		})
		if len(candidates) <= legacyVersionWorkspaceValidationLimitPerBase {
			validateLegacyTags = append(validateLegacyTags, candidates...)
			continue
		}
		low := legacyVersionWorkspaceValidationLimitPerBase / 2
		high := legacyVersionWorkspaceValidationLimitPerBase - low
		validateLegacyTags = append(validateLegacyTags, candidates[:low]...)
		validateLegacyTags = append(validateLegacyTags, candidates[len(candidates)-high:]...)
	}

	var mu sync.Mutex
	var out []string
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(10)

	for _, tag := range relevantTags {
		tag := tag
		group.Go(func() error {
			name, err := legacyWorkspaceNameFromTag(groupCtx, repo, tag)
			if err != nil {
				return err
			}
			if name != "" {
				mu.Lock()
				out = append(out, name)
				mu.Unlock()
			}
			return nil
		})
	}
	for _, candidate := range validateLegacyTags {
		candidate := candidate
		group.Go(func() error {
			isVersion, err := legacyStateVersionTagMatchesStateTag(groupCtx, repo, candidate.tag, candidate.base, candidate.version)
			if err != nil {
				return err
			}
			if isVersion {
				return nil
			}
			name, err := legacyWorkspaceNameFromTag(groupCtx, repo, candidate.tag)
			if err != nil {
				return err
			}
			if name != "" {
				mu.Lock()
				out = append(out, name)
				mu.Unlock()
			}
			return nil
		})
	}

	if err := group.Wait(); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		out = append(out, "default")
	}
	sort.Strings(out)
	dedup := out[:1]
	for i := 1; i < len(out); i++ {
		if out[i] != out[i-1] {
			dedup = append(dedup, out[i])
		}
	}
	return dedup, nil
}

func splitLegacyStateVersionTag(tag string) (base string, version int, ok bool) {
	idx := strings.LastIndex(tag, stateVersionTagSeparator)
	if idx < 0 {
		return "", 0, false
	}
	base = tag[:idx]
	if base == "" {
		return "", 0, false
	}
	raw := tag[idx+len(stateVersionTagSeparator):]
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 || n > maxStateVersion {
		return "", 0, false
	}
	return base, n, true
}

func legacyStateVersionTagMatchesStateTag(ctx context.Context, repo *orasRepositoryClient, tag, expectedStateTag string, expectedVersion int) (bool, error) {
	m, err := fetchRawManifest(ctx, repo, tag)
	if err != nil {
		if isNotFound(err) {
			return false, nil
		}
		return false, err
	}
	if m.ArtifactType != "" && m.ArtifactType != artifactTypeState {
		return false, nil
	}
	workspace := m.Annotations[annotationWorkspace]
	if workspace == "" || stateTagPrefix+legacyWorkspaceTagFor(workspace) != expectedStateTag {
		return false, nil
	}
	if raw := m.Annotations[annotationStateVersion]; raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n != expectedVersion {
			return false, nil
		}
	}
	return true, nil
}

func legacyWorkspaceNameFromTag(ctx context.Context, repo *orasRepositoryClient, stateTag string) (string, error) {
	wsTag := strings.TrimPrefix(stateTag, stateTagPrefix)
	if !strings.HasPrefix(wsTag, "ws-") {
		return wsTag, nil
	}
	m, err := fetchRawManifest(ctx, repo, stateTag)
	if err != nil {
		return "", err
	}
	if name := m.Annotations[annotationWorkspace]; name != "" {
		return name, nil
	}
	return wsTag, nil
}

func fetchRawManifest(ctx context.Context, repo *orasRepositoryClient, reference string) (ocispec.Manifest, error) {
	desc, err := repo.inner.Resolve(ctx, reference)
	if err != nil {
		return ocispec.Manifest{}, err
	}
	rc, err := repo.inner.Fetch(ctx, desc)
	if err != nil {
		return ocispec.Manifest{}, err
	}
	defer func() { _ = rc.Close() }()

	data, err := io.ReadAll(io.LimitReader(rc, maxManifestSize))
	if err != nil {
		return ocispec.Manifest{}, err
	}
	var manifest ocispec.Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return ocispec.Manifest{}, fmt.Errorf("decoding manifest %q: %w", reference, err)
	}
	if manifest.Annotations == nil {
		manifest.Annotations = map[string]string{}
	}
	return manifest, nil
}
