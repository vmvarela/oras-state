package orasstate

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/oras-project/oras-go/v3"
)

// seedManifest pushes a minimal manifest and tags it, standing in for the
// state/marker/history artifacts a real repository holds.
func seedManifest(t *testing.T, repo *fakeORASRepo, tag string) ocispec.Descriptor {
	t.Helper()
	ctx := context.Background()
	// The tag goes into the manifest so each seeded tag has its own digest,
	// like real state, marker and history artifacts do.
	desc, err := oras.PackManifest(ctx, repo, oras.PackManifestVersion1_1, "application/vnd.test.state", oras.PackManifestOptions{
		ManifestAnnotations: map[string]string{"test.tag": tag},
	})
	if err != nil {
		t.Fatalf("pack manifest: %v", err)
	}
	if err := repo.Tag(ctx, desc, tag); err != nil {
		t.Fatalf("tag %q: %v", tag, err)
	}
	return desc
}

// purgeVersionsBody renders a GitHub packages version list with timestamps.
func purgeVersionsBody(versions []githubPackageVersion) string {
	b, err := json.Marshal(versions)
	if err != nil {
		panic(err)
	}
	return string(b)
}

func purgeVersion(id int64, digest string, tags []string, age time.Duration) githubPackageVersion {
	v := githubPackageVersion{ID: id, Name: digest, UpdatedAt: time.Now().Add(-age)}
	v.Metadata.Container.Tags = tags
	return v
}

// newPurgeClient builds a client over a fake OCI repo at a ghcr.io reference
// whose GitHub API calls are answered by handler.
func newPurgeClient(t *testing.T, repo *fakeORASRepo, maxVersions int, handler roundTripFunc) *Client {
	t.Helper()
	c := newTestClient(&orasRepositoryClient{
		inner:        repo,
		repository:   "ghcr.io/myorg/myrepo",
		credentialFn: resolveCredentials("ghcr.io", Config{Token: "tok"}),
		httpClient:   &http.Client{Transport: handler},
	})
	c.config.MaxVersions = maxVersions
	return c
}

func TestPurge_RejectsNonGHCRRepository(t *testing.T) {
	var calls int
	c := newTestClient(&orasRepositoryClient{
		inner:        newFakeORASRepo(),
		repository:   "zot.local/myorg/myrepo",
		credentialFn: resolveCredentials("zot.local", Config{Token: "tok"}),
		httpClient: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			calls++
			return fakeGitHubResponse(http.StatusOK, "[]"), nil
		})},
	})

	if _, err := c.Purge(context.Background(), PurgeOptions{}); err == nil {
		t.Fatal("expected an error for a non-ghcr.io repository")
	}
	if calls != 0 {
		t.Errorf("GitHub API calls = %d, want 0", calls)
	}
}

func TestPurge_RefusesWhenLockIsLive(t *testing.T) {
	repo := newFakeORASRepo()
	id := testWorkspaceTag(t, "default")
	seedManifest(t, repo, "locked-"+id)

	c := newPurgeClient(t, repo, 0, func(r *http.Request) (*http.Response, error) {
		t.Errorf("unexpected GitHub API call: %s", r.URL.Path)
		return fakeGitHubResponse(http.StatusOK, "[]"), nil
	})

	_, err := c.Purge(context.Background(), PurgeOptions{})
	if err == nil || !strings.Contains(err.Error(), "live lock tag") {
		t.Fatalf("err = %v, want a live-lock refusal", err)
	}
}

func TestPurge_DryRunReportsCandidatesWithoutDeleting(t *testing.T) {
	repo := newFakeORASRepo()
	id := testWorkspaceTag(t, "default")
	state := seedManifest(t, repo, "state-"+id)
	seedManifest(t, repo, "wsmeta-"+id)

	versions := []githubPackageVersion{
		purgeVersion(100, "sha256:"+strings.Repeat("01", 32), nil, time.Hour),
		purgeVersion(101, state.Digest.String(), []string{"state-" + id}, time.Hour),
	}
	for n := 1; n <= 5; n++ {
		tag := fmt.Sprintf("stver-%s-v%d", id, n)
		d := seedManifest(t, repo, tag)
		versions = append(versions, purgeVersion(int64(200+n), d.Digest.String(), []string{tag}, time.Hour))
	}

	var deletes int
	c := newPurgeClient(t, repo, 2, func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodDelete {
			deletes++
			return fakeGitHubResponse(http.StatusNoContent, ""), nil
		}
		return fakeGitHubResponse(http.StatusOK, purgeVersionsBody(versions)), nil
	})

	report, err := c.Purge(context.Background(), PurgeOptions{})
	if err != nil {
		t.Fatalf("purge: %v", err)
	}

	if deletes != 0 {
		t.Errorf("deletes = %d, want 0 in dry-run", deletes)
	}

	got := map[int64]string{}
	for _, cand := range report.Candidates {
		got[cand.VersionID] = cand.Reason
	}
	if _, ok := got[100]; !ok {
		t.Error("untagged version 100 is missing from the candidates")
	}
	for n := 1; n <= 3; n++ { // keep = 2, so v1..v3 are excess
		if _, ok := got[int64(200+n)]; !ok {
			t.Errorf("history version v%d is missing from the candidates", n)
		}
	}
	for n := 4; n <= 5; n++ {
		if _, ok := got[int64(200+n)]; ok {
			t.Errorf("history version v%d must be kept", n)
		}
	}
	if len(report.Deleted) != 0 {
		t.Errorf("deleted = %d, want 0", len(report.Deleted))
	}
}

func TestPurge_ApplyDeletesUntaggedAndKeepsLastTagged(t *testing.T) {
	repo := newFakeORASRepo()
	id := testWorkspaceTag(t, "default")
	state := seedManifest(t, repo, "state-"+id)
	seedManifest(t, repo, "wsmeta-"+id)

	versions := []githubPackageVersion{
		purgeVersion(7, "sha256:"+strings.Repeat("07", 32), nil, time.Hour),
		purgeVersion(8, state.Digest.String(), []string{"state-" + id}, time.Hour),
	}

	var deleted []string
	c := newPurgeClient(t, repo, 0, func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodDelete {
			deleted = append(deleted, r.URL.Path)
			if strings.HasSuffix(r.URL.Path, "/versions/8") {
				// GHCR refuses to delete the last tagged version.
				return fakeGitHubResponse(http.StatusBadRequest, `{"message":"cannot delete the last tagged version"}`), nil
			}
			return fakeGitHubResponse(http.StatusNoContent, ""), nil
		}
		return fakeGitHubResponse(http.StatusOK, purgeVersionsBody(versions)), nil
	})

	report, err := c.Purge(context.Background(), PurgeOptions{Apply: true})
	if err != nil {
		t.Fatalf("purge: %v", err)
	}

	if len(deleted) != 1 || !strings.HasSuffix(deleted[0], "/versions/7") {
		t.Fatalf("deleted paths = %v, want only the untagged version 7", deleted)
	}
	if len(report.Deleted) != 1 || report.Deleted[0].VersionID != 7 {
		t.Errorf("report.Deleted = %+v, want version 7", report.Deleted)
	}
	if report.KeptTagged != 1 {
		t.Errorf("KeptTagged = %d, want 1 (the tagged version stays)", report.KeptTagged)
	}
}

func TestPurge_GracePeriodSkipsFreshUntaggedVersions(t *testing.T) {
	repo := newFakeORASRepo()
	id := testWorkspaceTag(t, "default")
	state := seedManifest(t, repo, "state-"+id)

	versions := []githubPackageVersion{
		purgeVersion(9, "sha256:"+strings.Repeat("09", 32), nil, 10*time.Second),
		purgeVersion(10, state.Digest.String(), []string{"state-" + id}, time.Hour),
	}

	c := newPurgeClient(t, repo, 0, func(r *http.Request) (*http.Response, error) {
		return fakeGitHubResponse(http.StatusOK, purgeVersionsBody(versions)), nil
	})

	report, err := c.Purge(context.Background(), PurgeOptions{GracePeriod: time.Minute})
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	for _, cand := range report.Candidates {
		if cand.VersionID == 9 {
			t.Error("a version younger than the grace period must not be a candidate")
		}
	}
}

// testWorkspaceTag mirrors the hashed workspace id used by the tag scheme.
func testWorkspaceTag(t *testing.T, workspace string) string {
	t.Helper()
	c := &Client{}
	return c.workspaceTagFor(workspace)
}

func TestPurge_ApplyDeletesExcessHistoryWithLiveTags(t *testing.T) {
	repo := newFakeORASRepo()
	id := testWorkspaceTag(t, "default")
	seedManifest(t, repo, "state-"+id)
	seedManifest(t, repo, "wsmeta-"+id)

	var versions []githubPackageVersion
	for i := 1; i <= 4; i++ {
		tag := fmt.Sprintf("stver-%s-v%d", id, i)
		desc := seedManifest(t, repo, tag)
		versions = append(versions, purgeVersion(int64(i), desc.Digest.String(), []string{tag}, time.Hour))
	}

	var deleted []string
	c := newPurgeClient(t, repo, 2, func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodDelete {
			deleted = append(deleted, r.URL.Path)
			return fakeGitHubResponse(http.StatusNoContent, ""), nil
		}
		return fakeGitHubResponse(http.StatusOK, purgeVersionsBody(versions)), nil
	})

	report, err := c.Purge(context.Background(), PurgeOptions{Apply: true})
	if err != nil {
		t.Fatalf("purge: %v", err)
	}

	t.Logf("DEBUG candidates=%+v skipped=%v kept=%d", report.Candidates, report.Skipped, report.KeptTagged)

	// A candidate's own history tag is still live; that must not stop the delete.
	if len(deleted) != 2 || !strings.HasSuffix(deleted[0], "/versions/1") || !strings.HasSuffix(deleted[1], "/versions/2") {
		t.Fatalf("deleted paths = %v, want versions 1 and 2", deleted)
	}
	if len(report.Deleted) != 2 {
		t.Errorf("report.Deleted = %+v, want 2 versions", report.Deleted)
	}
	if len(report.Skipped) != 0 {
		t.Errorf("Skipped = %v, want none", report.Skipped)
	}
}
