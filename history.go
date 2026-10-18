package orasstate

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

// ErrStateVersionNotFound is returned by GetVersion when the requested
// historical state version does not exist. It is never treated as the current
// state and callers can match it with errors.Is.
var ErrStateVersionNotFound = errors.New("state version not found")

// StateVersion describes one retained historical state version.
//
// Versions are logical engine versions, not OCI tag names. UpdatedAt is zero
// only for older compatible manifests that did not record an update timestamp.
// Digest identifies the verified OCI manifest and is informational.
type StateVersion struct {
	Version   int       `json:"version"`
	UpdatedAt time.Time `json:"updated_at,omitempty"`
	Digest    string    `json:"digest"`
}

// ListVersions returns retained historical versions for stateID in ascending
// version order. OCI tag names are deliberately not exposed.
//
// Every returned manifest is verified to belong to stateID. A malformed
// version annotation fails the operation rather than returning ambiguous
// history. Concurrently deleted versions are skipped.
func (c *Client) ListVersions(ctx context.Context, stateID string) ([]StateVersion, error) {
	ctx, cancel := c.opContext(ctx)
	defer cancel()

	wc := newWorkspaceClient(c, stateID)
	if err := wc.checkWorkspace(ctx); err != nil {
		return nil, err
	}

	versions, err := wc.verifiedVersions(ctx)
	if err != nil {
		if isNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	slices.Sort(versions)

	out := make([]StateVersion, 0, len(versions))
	for _, version := range versions {
		reference := wc.versionTagFor(version)
		fm, desc, err := wc.fetchManifestWithDesc(ctx, reference)
		if err != nil {
			if isNotFound(err) {
				continue
			}
			return nil, err
		}
		if err := validateHistoricalManifest(fm, reference, version); err != nil {
			return nil, err
		}

		var updatedAt time.Time
		if raw := fm.Annotations[annotationUpdatedAt]; raw != "" {
			updatedAt, err = time.Parse(time.RFC3339Nano, raw)
			if err != nil {
				return nil, fmt.Errorf("invalid updated_at annotation for state version %d: %w", version, err)
			}
		}

		out = append(out, StateVersion{
			Version:   version,
			UpdatedAt: updatedAt,
			Digest:    desc.Digest.String(),
		})
	}
	return out, nil
}

// GetVersion returns the logical, uncompressed state bytes for one retained
// historical version. It applies the same workspace-ownership, compression,
// media-type, and MaxStateSize checks as Get.
//
// A missing version returns an error wrapping ErrStateVersionNotFound; it never
// falls back to the current state.
func (c *Client) GetVersion(ctx context.Context, stateID string, version int) ([]byte, error) {
	if version <= 0 || version > maxStateVersion {
		return nil, fmt.Errorf("invalid state version %d: must be between 1 and %d", version, maxStateVersion)
	}

	ctx, cancel := c.opContext(ctx)
	defer cancel()

	wc := newWorkspaceClient(c, stateID)
	if err := wc.checkWorkspace(ctx); err != nil {
		return nil, err
	}

	reference := wc.versionTagFor(version)
	fm, _, err := wc.fetchManifestWithDesc(ctx, reference)
	if err != nil {
		if isNotFound(err) {
			if isMissingContent(err) {
				// The version tag resolved but its content is missing:
				// incomplete existing state, distinct from an absent version.
				return nil, fmt.Errorf("state version %d at %q is incomplete: the tag resolved but its content is missing: %w", version, reference, err)
			}
			return nil, fmt.Errorf("%w: %d", ErrStateVersionNotFound, version)
		}
		return nil, err
	}
	if err := validateHistoricalManifest(fm, reference, version); err != nil {
		return nil, err
	}
	return wc.readStateManifest(ctx, reference, fm)
}

func validateHistoricalManifest(fm ocispec.Manifest, reference string, version int) error {
	if fm.ArtifactType != "" && fm.ArtifactType != artifactTypeState {
		return fmt.Errorf("unexpected state manifest artifactType %q for %q", fm.ArtifactType, reference)
	}
	raw := fm.Annotations[annotationStateVersion]
	if raw == "" {
		// Older compatible manifests may predate the version annotation. The
		// verified version tag and workspace annotation remain authoritative.
		return nil
	}
	annotated, err := strconv.Atoi(raw)
	if err != nil || annotated != version {
		return fmt.Errorf("state version metadata mismatch for %q: tag version %d, annotation %q", reference, version, raw)
	}
	return nil
}
