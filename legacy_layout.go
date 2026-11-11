package orasstate

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	orasProperties "github.com/oras-project/oras-go/v3/registry/remote/properties"
	"golang.org/x/sync/errgroup"
)

const legacyVersionWorkspaceValidationLimitPerBase = 16

// legacyWorkspaceTagFor reproduces the exact workspace tag mapping used
// by the legacy backend layout. It is intentionally not the default layout.
func legacyWorkspaceTagFor(workspace string) string {
	ref := orasProperties.Reference{Tag: workspace}
	if err := ref.ValidateTag(); err == nil {
		return workspace
	}
	h := sha256.Sum256([]byte(workspace))
	return "ws-" + hex.EncodeToString(h[:8])
}

// checkLegacyWorkspace validates only the tags owned by this workspace.
// Unlike the strict hashed layout, it deliberately does not reject a repository
// merely because literal legacy tags exist. The workspace marker is not part of
// the legacy layout: it is neither created nor read here.
func (wc *workspaceClient) checkLegacyWorkspace(ctx context.Context) (workspaceObservation, error) {
	for _, tag := range []string{wc.stateTag, wc.lockTag, wc.unlockedTag, wc.versionTagFor(maxStateVersion)} {
		if err := (orasProperties.Reference{Tag: tag}).ValidateTag(); err != nil {
			return workspaceObservation{}, fmt.Errorf("invalid workspace tag: %w", err)
		}
	}

	// Existing legacy manifests already carry org.terraform.workspace. Verify
	// our own mutable tags when present so compatibility does not weaken
	// identity checking for state that already exists.
	var obs workspaceObservation
	for _, tag := range []string{wc.stateTag, wc.lockTag, wc.unlockedTag} {
		m, _, err := wc.fetchManifestWithDesc(ctx, tag)
		if isNotFound(err) {
			continue
		}
		if err != nil {
			return obs, err
		}
		if tag == wc.stateTag {
			obs.statePresent = true
		}
		if name := m.Annotations[annotationWorkspace]; name != "" && name != wc.stateID {
			return obs, fmt.Errorf("workspace identity mismatch at tag %q", tag)
		}
	}
	return obs, nil
}

func listLegacyWorkspaces(ctx context.Context, repo *orasRepositoryClient) ([]string, error) {
	tags, err := listRepoTags(ctx, repo.inner)
	if err != nil {
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
		slices.SortFunc(candidates, func(a, b legacyVersionCandidate) int {
			return cmp.Or(cmp.Compare(a.version, b.version), cmp.Compare(a.tag, b.tag))
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
		return []string{"default"}, nil
	}
	slices.Sort(out)
	return slices.Compact(out), nil
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
	m, _, err := resolveAndFetchManifest(ctx, repo.inner, tag)
	if err != nil {
		if isNotFound(err) && !isMissingContent(err) {
			return false, nil
		}
		return false, err
	}
	return legacyStateVersionManifestMatches(m, expectedStateTag, expectedVersion), nil
}

// legacyStateVersionManifestMatches is the pure form of the legacy history
// identity check, for callers that already hold the pinned manifest. It keeps
// the exact rules the wrapper always applied.
func legacyStateVersionManifestMatches(m ocispec.Manifest, expectedStateTag string, expectedVersion int) bool {
	if m.ArtifactType != "" && m.ArtifactType != artifactTypeState {
		return false
	}
	workspace := m.Annotations[annotationWorkspace]
	if workspace == "" || stateTagPrefix+legacyWorkspaceTagFor(workspace) != expectedStateTag {
		return false
	}
	if raw := m.Annotations[annotationStateVersion]; raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n != expectedVersion {
			return false
		}
	}
	return true
}

func legacyWorkspaceNameFromTag(ctx context.Context, repo *orasRepositoryClient, stateTag string) (string, error) {
	wsTag := strings.TrimPrefix(stateTag, stateTagPrefix)
	if !strings.HasPrefix(wsTag, "ws-") {
		return wsTag, nil
	}
	m, _, err := resolveAndFetchManifest(ctx, repo.inner, stateTag)
	if err != nil {
		return "", err
	}
	if name := m.Annotations[annotationWorkspace]; name != "" {
		return name, nil
	}
	return wsTag, nil
}
