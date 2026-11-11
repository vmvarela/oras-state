// GHCR tag deletion support.
//
// GitHub Container Registry (ghcr.io) does not support the standard OCI
// manifest deletion endpoint (DELETE /v2/.../manifests/<digest>); it returns
// HTTP 405. As a fallback, this file implements tag deletion via the GitHub
// Packages REST API (https://docs.github.com/en/rest/packages).
package orasstate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"
)

var errNotGHCR = errors.New("not a ghcr.io repository")

const (
	ghcrHost              = "ghcr.io"
	githubAPIBaseURL      = "https://api.github.com"
	githubAPIVersion      = "2022-11-28"
	githubVersionsPerPage = 100

	// ghcrDeleteMaxAttempts caps retries of the package-version DELETE when
	// GitHub returns 404. A version freshly published to ghcr.io is sometimes
	// listed but not yet deletable (eventual propagation on the Packages
	// backend), so a short bounded retry recovers without masking real
	// permission failures (401/403 are returned as-is, never retried).
	ghcrDeleteMaxAttempts = 3
)

// ghcrDeleteRetryDelay is the backoff between retried DELETE attempts. A var
// so tests can shrink it and stay fast and deterministic.
var ghcrDeleteRetryDelay = 500 * time.Millisecond

type githubPackageVersion struct {
	ID       int64  `json:"id"`
	Name     string `json:"name"` // full digest of the manifest, e.g. "sha256:..."
	Metadata struct {
		Container struct {
			Tags []string `json:"tags"`
		} `json:"container"`
	} `json:"metadata"`
}

type ghcrEndpointPages struct {
	notFound  bool
	versions  []githubPackageVersion
	nextPage  int
	exhausted bool
}

// ghcrVersionCache reuses already-fetched GitHub Packages version pages across
// a single multi-tag operation (DeleteWorkspace) so N deletions page the
// package history once instead of N times. A nil cache disables reuse.
type ghcrVersionCache struct {
	endpoints map[string]*ghcrEndpointPages
}

func (c *ghcrVersionCache) endpoint(baseURL string) *ghcrEndpointPages {
	if c.endpoints == nil {
		c.endpoints = make(map[string]*ghcrEndpointPages, 2)
	}
	ep, ok := c.endpoints[baseURL]
	if !ok {
		ep = &ghcrEndpointPages{nextPage: 1}
		c.endpoints[baseURL] = ep
	}
	return ep
}

func tryDeleteGHCRTag(ctx context.Context, repo *orasRepositoryClient, tag, expectedDigest string) error {
	return tryDeleteGHCRTagCached(ctx, repo, tag, expectedDigest, nil)
}

func tryDeleteGHCRTagCached(ctx context.Context, repo *orasRepositoryClient, tag, expectedDigest string, cache *ghcrVersionCache) error {
	if repo == nil {
		return fmt.Errorf("nil repository client")
	}

	host, owner, packageName, err := parseGHCRRepository(repo.repository)
	if err != nil {
		return err
	}
	if host != ghcrHost {
		return errNotGHCR
	}

	token, err := repo.accessToken(ctx)
	if err != nil {
		return err
	}
	if token == "" {
		return fmt.Errorf("no credentials available for %s (need a token with delete:packages)", host)
	}

	client := repo.httpClient
	if client == nil {
		client = &http.Client{}
	}

	return deleteGitHubPackageVersionByTagCached(ctx, client, githubAPIBaseURL, owner, packageName, tag, token, expectedDigest, cache)
}

func parseGHCRRepository(repository string) (host, owner, packageName string, err error) {
	trimmed := strings.TrimSpace(repository)
	parts := strings.Split(trimmed, "/")
	if len(parts) < 3 {
		return "", "", "", fmt.Errorf("invalid repository %q: expected <host>/<owner>/<name>", repository)
	}

	host = parts[0]
	owner = parts[1]
	packageName = strings.Join(parts[2:], "/")

	if host == "" || owner == "" || packageName == "" {
		return "", "", "", fmt.Errorf("invalid repository %q: empty segment", repository)
	}
	return host, owner, packageName, nil
}

func deleteGitHubPackageVersionByTag(ctx context.Context, client *http.Client, baseURL, owner, packageName, tag, token, expectedDigest string) error {
	return deleteGitHubPackageVersionByTagCached(ctx, client, baseURL, owner, packageName, tag, token, expectedDigest, nil)
}

func deleteGitHubPackageVersionByTagCached(ctx context.Context, client *http.Client, baseURL, owner, packageName, tag, token, expectedDigest string, cache *ghcrVersionCache) error {
	baseURL = strings.TrimRight(baseURL, "/")
	pkgEscaped := url.PathEscape(packageName)
	ownerEscaped := url.PathEscape(owner)

	orgBase := fmt.Sprintf("%s/orgs/%s/packages/container/%s", baseURL, ownerEscaped, pkgEscaped)
	if err := deleteFromGitHubPackagesEndpointCached(ctx, client, orgBase, tag, token, expectedDigest, cache); err == nil {
		return nil
	} else if !isHTTPStatus(err, http.StatusNotFound) {
		return err
	}

	userBase := fmt.Sprintf("%s/users/%s/packages/container/%s", baseURL, ownerEscaped, pkgEscaped)
	return deleteFromGitHubPackagesEndpointCached(ctx, client, userBase, tag, token, expectedDigest, cache)
}

func deleteFromGitHubPackagesEndpointCached(ctx context.Context, client *http.Client, baseURL, tag, token, expectedDigest string, cache *ghcrVersionCache) error {
	versionID, err := findGitHubVersionIDCached(ctx, client, baseURL, tag, token, expectedDigest, cache)
	if err != nil {
		return err
	}
	if versionID == 0 {
		return httpStatusErr{code: http.StatusNotFound, op: "no package version matching tag and digest"}
	}

	deleteURL := fmt.Sprintf("%s/versions/%d", baseURL, versionID)
	if err := deleteGitHubPackageVersionWithRetry(ctx, client, deleteURL, token); err != nil {
		return err
	}
	if cache != nil {
		ep := cache.endpoint(baseURL)
		ep.versions = slices.DeleteFunc(ep.versions, func(v githubPackageVersion) bool {
			return v.ID == versionID
		})
	}
	return nil
}

// deleteGitHubPackageVersionWithRetry issues the package-version DELETE and,
// only when GitHub answers 404, retries it up to ghcrDeleteMaxAttempts with a
// short backoff — the freshly-published-version propagation window described
// in the ghcrDeleteMaxAttempts comment. Any other status is returned as-is,
// and a 404 that persists across all attempts returns the original error so
// permission problems stay visible. Cancellation aborts the backoff
// immediately with the context error.
func deleteGitHubPackageVersionWithRetry(ctx context.Context, client *http.Client, deleteURL, token string) error {
	var lastErr error
	for attempt := 1; attempt <= ghcrDeleteMaxAttempts; attempt++ {
		err := githubRequest(ctx, client, http.MethodDelete, deleteURL, token, "delete package version", http.StatusNoContent, nil)
		if err == nil || !isHTTPStatus(err, http.StatusNotFound) {
			return err
		}
		lastErr = err
		if attempt == ghcrDeleteMaxAttempts {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(ghcrDeleteRetryDelay):
		}
	}
	return lastErr
}

// findGitHubVersionIDByTag pages through the package's versions looking for a
// version whose tags contain tag AND whose top-level "name" digest equals
// expectedDigest. A tag-only match is not enough: the DELETE by version ID
// removes the digest and ALL its tags, so a digest mismatch would destroy
// another manifest sharing the tag. Paging stops at an empty or short page
// (fewer than githubVersionsPerPage entries means the API returned
// everything); context cancellation surfaces as an error.
func findGitHubVersionIDByTag(ctx context.Context, client *http.Client, baseURL, tag, token, expectedDigest string) (int64, error) {
	return findGitHubVersionIDCached(ctx, client, baseURL, tag, token, expectedDigest, nil)
}

func findGitHubVersionIDCached(ctx context.Context, client *http.Client, baseURL, tag, token, expectedDigest string, cache *ghcrVersionCache) (int64, error) {
	if cache == nil {
		return scanEndpointPages(ctx, client, baseURL, tag, token, expectedDigest, &ghcrEndpointPages{nextPage: 1}, false)
	}
	ep := cache.endpoint(baseURL)
	if ep.notFound {
		return 0, httpStatusErr{code: http.StatusNotFound, op: "list package versions"}
	}
	hadCached := len(ep.versions) > 0 || ep.exhausted
	if id, err := scanEndpointPages(ctx, client, baseURL, tag, token, expectedDigest, ep, true); err != nil || id != 0 || !hadCached {
		return id, err
	}
	// A tag published mid-operation (such as an unlock anchor) was not in the
	// earlier pages: refresh once before reporting 404.
	*ep = ghcrEndpointPages{nextPage: 1}
	return scanEndpointPages(ctx, client, baseURL, tag, token, expectedDigest, ep, true)
}

func scanEndpointPages(ctx context.Context, client *http.Client, baseURL, tag, token, expectedDigest string, ep *ghcrEndpointPages, retain bool) (int64, error) {
	if id := findVersionIDWithTag(ep.versions, tag, expectedDigest); id != 0 {
		return id, nil
	}
	for !ep.exhausted {
		versions, err := listGitHubPackageVersions(ctx, client, baseURL, ep.nextPage, token)
		if err != nil {
			if ep.nextPage == 1 && isHTTPStatus(err, http.StatusNotFound) {
				ep.notFound = true
			}
			return 0, err
		}
		ep.nextPage++
		if retain {
			ep.versions = append(ep.versions, versions...)
		}
		if len(versions) == 0 || len(versions) < githubVersionsPerPage {
			ep.exhausted = true
		}
		if id := findVersionIDWithTag(versions, tag, expectedDigest); id != 0 {
			return id, nil
		}
	}
	return 0, nil
}

func listGitHubPackageVersions(ctx context.Context, client *http.Client, baseURL string, page int, token string) ([]githubPackageVersion, error) {
	versionsURL, err := url.Parse(baseURL + "/versions")
	if err != nil {
		return nil, err
	}
	query := versionsURL.Query()
	query.Set("per_page", strconv.Itoa(githubVersionsPerPage))
	query.Set("page", strconv.Itoa(page))
	versionsURL.RawQuery = query.Encode()

	var versions []githubPackageVersion
	if err := githubRequest(ctx, client, http.MethodGet, versionsURL.String(), token, "list package versions", http.StatusOK, &versions); err != nil {
		return nil, err
	}
	return versions, nil
}

// findVersionIDWithTag returns the ID of the first version matching BOTH the
// tag and the expected digest ("name" field). Anything else — tag hit on a
// different digest, digest hit without the tag — is not a match.
func findVersionIDWithTag(versions []githubPackageVersion, tag, expectedDigest string) int64 {
	for _, v := range versions {
		if v.Name == expectedDigest && slices.Contains(v.Metadata.Container.Tags, tag) {
			return v.ID
		}
	}
	return 0
}

func githubRequest(ctx context.Context, client *http.Client, method, urlStr, token, operation string, expectedStatus int, decode any) error {
	req, err := http.NewRequestWithContext(ctx, method, urlStr, nil)
	if err != nil {
		return err
	}

	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", githubAPIVersion)
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != expectedStatus {
		return httpStatusErr{code: resp.StatusCode, op: operation, detail: readGitHubErrorDetail(resp.Body)}
	}
	if decode == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(decode)
}

func readGitHubErrorDetail(r io.Reader) string {
	head, _ := io.ReadAll(io.LimitReader(r, 1024))
	// Drain any remainder so the underlying TCP connection can be reused (HTTP/1.1 keep-alive).
	_, _ = io.Copy(io.Discard, r)
	var payload struct {
		Message string `json:"message"`
	}
	if json.Unmarshal(head, &payload) == nil && payload.Message != "" {
		return payload.Message
	}
	return strings.TrimSpace(string(head))
}

type httpStatusErr struct {
	code   int
	op     string
	detail string
}

func (e httpStatusErr) Error() string {
	if e.detail != "" {
		return fmt.Sprintf("github api error (%s): status %d: %s", e.op, e.code, e.detail)
	}
	return fmt.Sprintf("github api error (%s): status %d", e.op, e.code)
}

func isHTTPStatus(err error, code int) bool {
	var e httpStatusErr
	if errors.As(err, &e) {
		return e.code == code
	}
	return false
}

func isGHCRDeleteBadRequest(err error) bool {
	var e httpStatusErr
	return errors.As(err, &e) && e.code == http.StatusBadRequest && e.op == "delete package version"
}
