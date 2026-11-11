package orasstate

import (
	"context"
	"fmt"
	"strings"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	orasProperties "github.com/oras-project/oras-go/v3/registry/remote/properties"
)

// workspaceIDFromTag distinguishes provider tags from unrelated OCI artifacts.
// The full tag and the original-name annotation must both validate: a legacy
// literal name can look exactly like a SHA-256 identifier.
func workspaceIDFromTag(tag string) (string, bool) {
	for _, prefix := range []string{stateTagPrefix, lockTagPrefix, unlockedTagPrefix, markerTagPrefix} {
		if strings.HasPrefix(tag, prefix) {
			return strings.TrimPrefix(tag, prefix), true
		}
	}
	if strings.HasPrefix(tag, stateVersionTagPrefix) {
		base, _, ok := splitStateVersionTag(tag)
		if !ok {
			return "", true
		}
		return strings.TrimPrefix(base, stateVersionTagPrefix), true
	}
	return "", false
}

// reservedWorkspaceTags lists the repository and rejects legacy and mixed
// layouts by tag name alone, even when only locks/history remain: every
// reserved tag must carry a 64-character identifier. It reads no manifests.
// It is not cached: objects can change between RPCs.
func reservedWorkspaceTags(ctx context.Context, repo *orasRepositoryClient) ([]string, error) {
	tags, err := listRepoTags(ctx, repo.inner)
	if isNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("checking workspace storage format: %w", err)
	}
	var reservedTags []string
	for _, tag := range tags {
		id, reserved := workspaceIDFromTag(tag)
		if !reserved {
			continue
		}
		if len(id) != 64 {
			return nil, fmt.Errorf("legacy workspace tag %q detected: stop all writers and migrate verified state to an empty repository; do not mix provider storage formats", tag)
		}
		if err := (orasProperties.Reference{Tag: tag}).ValidateTag(); err != nil {
			return nil, err
		}
		reservedTags = append(reservedTags, tag)
	}
	return reservedTags, nil
}

func validateWorkspaceManifest(m ocispec.Manifest, tag, workspace string) error {
	name, ok := m.Annotations[annotationWorkspace]
	if !ok || name != workspace {
		return fmt.Errorf("workspace identity mismatch at tag %q: refusing access; stop writers and restore verified metadata", tag)
	}
	return nil
}

// validateWorkspaceMarker checks the exact marker identity used as evidence or
// as a deletion target: the original-name annotation must match, the artifact
// type must be the marker's own, and the manifest must carry no layers.
func validateWorkspaceMarker(m ocispec.Manifest, tag, workspace string) error {
	if err := validateWorkspaceManifest(m, tag, workspace); err != nil {
		return err
	}
	if m.ArtifactType != artifactTypeWorkspaceMarker {
		return fmt.Errorf("unexpected workspace marker artifactType %q for %q", m.ArtifactType, tag)
	}
	if len(m.Layers) != 0 {
		return fmt.Errorf("workspace marker %q has %d layers; expected none", tag, len(m.Layers))
	}
	return nil
}

// workspaceObservation is the per-operation view of the repository listing.
// It is neither CAS nor authoritative absence: tags can move after the listing.
type workspaceObservation struct {
	statePresent  bool
	markerPresent bool
	tagsListed    bool
	reservedTags  []string
}

// checkWorkspace is a preflight, NOT CAS. It neither fences old binaries nor
// closes the existing verify-to-write window. Never cache its registry result:
// tag names are checked repo-wide, and annotations only for own mutable tags.
//
// It returns the listing-derived observation so callers can reuse current-tag
// and marker presence without a second listing. Marker manifests are NOT read
// here: the marker is validated only where it is used as evidence or deleted,
// so Unlock/VerifyLock/history reads stay usable on marker-only state.
func (wc *workspaceClient) checkWorkspace(ctx context.Context) (workspaceObservation, error) {
	if wc.client.config.WorkspaceLayout == WorkspaceLayoutLegacy {
		return wc.checkLegacyWorkspace(ctx)
	}

	for _, tag := range []string{wc.stateTag, wc.lockTag, wc.unlockedTag, wc.markerTag, wc.versionTagFor(maxStateVersion)} {
		if err := (orasProperties.Reference{Tag: tag}).ValidateTag(); err != nil {
			return workspaceObservation{}, fmt.Errorf("invalid workspace tag: %w", err)
		}
	}
	tags, err := reservedWorkspaceTags(ctx, wc.client.repoClient)
	if err != nil {
		return workspaceObservation{}, err
	}
	obs := workspaceObservation{
		tagsListed:   true,
		reservedTags: tags,
	}
	for _, tag := range tags {
		switch tag {
		case wc.stateTag:
			obs.statePresent = true
		case wc.markerTag:
			obs.markerPresent = true
			continue
		case wc.lockTag, wc.unlockedTag:
		default:
			continue
		}
		name, err := workspaceNameFromTag(ctx, wc.client.repoClient, tag)
		if isNotFound(err) {
			// Concurrent deletion: no object remains to inspect.
			continue
		}
		if err != nil {
			return obs, err
		}
		if name != wc.stateID {
			return obs, fmt.Errorf("workspace identity mismatch at tag %q; stop writers and restore verified metadata", tag)
		}
	}
	return obs, nil
}
