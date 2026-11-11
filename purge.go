package orasstate

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"
)

// defaultPurgeGracePeriod is how long an untagged version is left alone. It
// covers a writer that is still publishing (tag pushed after the manifest).
const defaultPurgeGracePeriod = 10 * time.Minute

// PurgeOptions controls Purge.
type PurgeOptions struct {
	// Apply performs the deletions. When false (the default) Purge only
	// reports what it would delete.
	Apply bool
	// GracePeriod ignores untagged versions younger than this. Zero means
	// defaultPurgeGracePeriod; negative disables the window.
	GracePeriod time.Duration
	// KeepPerWorkspace is how many history versions to keep per workspace.
	// Zero falls back to Config.MaxVersions; when that is also zero no
	// history is purged.
	KeepPerWorkspace int
}

// PurgeCandidate is a package version Purge selected for deletion.
type PurgeCandidate struct {
	VersionID int64
	Digest    string
	Tags      []string
	Reason    string
}

// PurgeReport describes what Purge found and did.
type PurgeReport struct {
	Candidates []PurgeCandidate
	Deleted    []PurgeCandidate
	Skipped    []string
	KeptTagged int
}

// Purge deletes unused package versions from a ghcr.io repository.
//
// Only ghcr.io needs this: elsewhere the registry supports manifest deletion
// and Put enforces MaxVersions. GHCR cannot delete manifests through the OCI
// API and never prunes history, so unused versions accumulate.
//
// Purge never removes the last tagged version of the package: GHCR rejects
// that, and a package without tagged versions cannot be recovered in place.
//
// Writers must be stopped for the duration. Purge refuses to run while a live
// lock is published, but that check is cooperation, not a fence: an OCI tag
// has no compare-and-swap, so a writer that starts after the check can still
// race the deletions.
func (c *Client) Purge(ctx context.Context, opts PurgeOptions) (PurgeReport, error) {
	var report PurgeReport
	ctx, cancel := c.opContext(ctx)
	defer cancel()

	if !c.ghcrRegistry() {
		return report, fmt.Errorf("purge is only implemented for ghcr.io: other registries support manifest deletion and enforce MaxVersions")
	}
	_, owner, packageName, err := parseGHCRRepository(c.repoClient.repository)
	if err != nil {
		return report, err
	}
	token, err := c.repoClient.accessToken(ctx)
	if err != nil {
		return PurgeReport{}, fmt.Errorf("resolve ghcr api token: %w", err)
	}
	if token == "" {
		return report, fmt.Errorf("purging %s needs a token with delete:packages and package admin", c.repoClient.repository)
	}

	if err := c.rejectLiveLocks(ctx); err != nil {
		return report, err
	}

	client := c.repoClient.httpClient
	if client == nil {
		client = &http.Client{}
	}
	versions, endpoint, err := listAllGitHubPackageVersions(ctx, client, githubAPIBaseURL, owner, packageName, token)
	if err != nil {
		return report, err
	}

	grace := opts.GracePeriod
	if grace == 0 {
		grace = defaultPurgeGracePeriod
	}
	keep := opts.KeepPerWorkspace
	if keep == 0 {
		keep = c.config.MaxVersions
	}

	candidates := purgeCandidates(versions, keep, grace, time.Now())
	tagged := 0
	for _, v := range versions {
		if len(v.Metadata.Container.Tags) > 0 {
			tagged++
		}
	}
	candidates, kept := protectLastTagged(candidates, tagged)
	report.Candidates = candidates
	report.KeptTagged = kept
	report.Skipped = []string{}
	if !opts.Apply {
		return report, nil
	}

	// One fresh tag listing right before deleting: a tag that moved since the
	// version listing means the digest is in use again.
	tags, err := listRepoTags(ctx, c.repoClient.inner)
	if err != nil {
		return report, err
	}
	live := make(map[string]int, len(tags))
	for _, tag := range tags {
		desc, err := resolveCopyTag(ctx, c.repoClient.inner, tag)
		if err != nil {
			if isNotFound(err) {
				continue
			}
			return report, err
		}
		live[desc.Digest.String()]++
	}

	for _, cand := range candidates {
		// A candidate's own tags are expected to resolve to its digest. Only a
		// reference held by some other tag means the digest is still in use.
		if live[cand.Digest] > len(cand.Tags) {
			report.Skipped = append(report.Skipped, fmt.Sprintf("%s: digest is referenced by a live tag", cand.Digest))
			continue
		}
		deleteURL := endpoint + "/versions/" + strconv.FormatInt(cand.VersionID, 10)
		if err := deleteGitHubPackageVersionWithRetry(ctx, client, deleteURL, token); err != nil {
			if isHTTPStatus(err, http.StatusBadRequest) {
				return report, fmt.Errorf("ghcr keeps at least one tagged version per package, so %s cannot be deleted: %w", cand.Digest, err)
			}
			return report, err
		}
		report.Deleted = append(report.Deleted, cand)
	}

	if err := c.verifySurvivingManifests(ctx); err != nil {
		return report, err
	}
	return report, nil
}

// rejectLiveLocks refuses to purge while any workspace holds a lock. A stale
// lock also refuses: retiring it is the caller's decision, not a guess.
func (c *Client) rejectLiveLocks(ctx context.Context) error {
	tags, err := listRepoTags(ctx, c.repoClient.inner)
	if err != nil {
		return err
	}
	for _, tag := range tags {
		var id string
		switch {
		case strings.HasPrefix(tag, lockTagPrefix):
			id = strings.TrimPrefix(tag, lockTagPrefix)
		case strings.HasPrefix(tag, unlockedTagPrefix):
			id = strings.TrimPrefix(tag, unlockedTagPrefix)
		default:
			continue
		}
		desc, err := resolveCopyTag(ctx, c.repoClient.inner, tag)
		if err != nil {
			if isNotFound(err) {
				continue
			}
			return err
		}
		data, err := copyManifestFromDescriptor(ctx, c.repoClient.inner, desc)
		if err != nil {
			return err
		}
		if !c.isCopyUnlockMarker(data, id) {
			return fmt.Errorf("repository has a live lock tag %q; unlock or retire it before purging", tag)
		}
	}
	return nil
}

// purgeCandidates selects untagged versions past the grace period plus history
// versions beyond keep. A version is only selected when every one of its tags
// is part of the excess.
func purgeCandidates(versions []githubPackageVersion, keep int, grace time.Duration, now time.Time) []PurgeCandidate {
	old := func(v githubPackageVersion) bool {
		return grace < 0 || v.UpdatedAt.IsZero() || now.Sub(v.UpdatedAt) > grace
	}

	var excess map[string]bool // tag -> true when it is beyond keep
	if keep > 0 {
		byBase := map[string][]int{}
		for _, v := range versions {
			for _, tag := range v.Metadata.Container.Tags {
				base, number, ok := splitStateVersionTag(tag)
				if !ok {
					continue
				}
				byBase[base] = append(byBase[base], number)
			}
		}
		excess = map[string]bool{}
		for base, numbers := range byBase {
			slices.Sort(numbers)
			for _, n := range numbers[:max(0, len(numbers)-keep)] {
				excess[base+stateVersionTagSeparator+strconv.Itoa(n)] = true
			}
		}
	}

	var candidates []PurgeCandidate
	for _, v := range versions {
		tags := v.Metadata.Container.Tags
		if len(tags) == 0 {
			if !old(v) {
				continue
			}
			candidates = append(candidates, PurgeCandidate{
				VersionID: v.ID,
				Digest:    v.Name,
				Reason:    "untagged version",
			})
			continue
		}
		if excess == nil {
			continue
		}
		all := true
		for _, tag := range tags {
			if !excess[tag] {
				all = false
				break
			}
		}
		if !all {
			continue
		}
		kept := slices.Clone(tags)
		slices.Sort(kept)
		candidates = append(candidates, PurgeCandidate{
			VersionID: v.ID,
			Digest:    v.Name,
			Tags:      kept,
			Reason:    fmt.Sprintf("history beyond %d versions", keep),
		})
	}
	return candidates
}

// protectLastTagged drops tagged candidates until at least one tagged version
// survives, preferring to keep a version that holds a state tag.
func protectLastTagged(candidates []PurgeCandidate, tagged int) ([]PurgeCandidate, int) {
	var taggedCands []int
	for i, cand := range candidates {
		if len(cand.Tags) > 0 {
			taggedCands = append(taggedCands, i)
		}
	}
	removable := tagged - 1
	if removable < 0 {
		removable = 0
	}
	if len(taggedCands) <= removable {
		return candidates, tagged - len(taggedCands)
	}

	// Delete non-state versions first so the survivor is the most useful one.
	slices.SortStableFunc(taggedCands, func(a, b int) int {
		as, bs := hasStateTag(candidates[a].Tags), hasStateTag(candidates[b].Tags)
		switch {
		case as && !bs:
			return 1
		case !as && bs:
			return -1
		}
		return 0
	})

	drop := make([]int, 0, len(taggedCands)-removable)
	for _, idx := range taggedCands[removable:] {
		drop = append(drop, idx)
	}
	for i := len(drop) - 1; i >= 0; i-- {
		candidates = slices.Delete(candidates, drop[i], drop[i]+1)
	}
	return candidates, tagged - removable
}

func hasStateTag(tags []string) bool {
	return slices.ContainsFunc(tags, func(tag string) bool {
		return strings.HasPrefix(tag, stateTagPrefix)
	})
}

// verifySurvivingManifests re-reads every state, marker and history manifest
// after the deletions, so a purge that broke the repository fails loudly.
func (c *Client) verifySurvivingManifests(ctx context.Context) error {
	tags, err := listRepoTags(ctx, c.repoClient.inner)
	if err != nil {
		return err
	}
	for _, tag := range tags {
		if !strings.HasPrefix(tag, stateTagPrefix) &&
			!strings.HasPrefix(tag, markerTagPrefix) &&
			!strings.HasPrefix(tag, stateVersionTagPrefix) {
			continue
		}
		desc, err := resolveCopyTag(ctx, c.repoClient.inner, tag)
		if err != nil {
			return fmt.Errorf("tag %q is unreadable after purge: %w", tag, err)
		}
		if _, err := copyManifestFromDescriptor(ctx, c.repoClient.inner, desc); err != nil {
			return fmt.Errorf("tag %q is unreadable after purge: %w", tag, err)
		}
	}
	return nil
}

// listAllGitHubPackageVersions reads every version of a package, returning the
// endpoint that answered (organisation first, user second).
func listAllGitHubPackageVersions(ctx context.Context, client *http.Client, baseURL, owner, packageName, token string) ([]githubPackageVersion, string, error) {
	bases := []string{
		fmt.Sprintf("%s/orgs/%s/packages/container/%s", baseURL, owner, packageName),
		fmt.Sprintf("%s/users/%s/packages/container/%s", baseURL, owner, packageName),
	}
	var lastErr error
	for _, base := range bases {
		var all []githubPackageVersion
		for page := 1; ; page++ {
			batch, err := listGitHubPackageVersions(ctx, client, base, page, token)
			if err != nil {
				lastErr = err
				all = nil
				break
			}
			all = append(all, batch...)
			if len(batch) < githubVersionsPerPage {
				return all, base, nil
			}
		}
		if lastErr != nil && isHTTPStatus(lastErr, http.StatusNotFound) {
			continue
		}
		if lastErr != nil {
			return nil, "", lastErr
		}
	}
	if lastErr != nil {
		return nil, "", lastErr
	}
	return nil, "", fmt.Errorf("package %s/%s not found", owner, packageName)
}
