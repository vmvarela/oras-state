package orasstate

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

// Harbor integration tests run against a real Harbor registry. They are
// gated behind TF_ORAS_HARBOR_TEST and configured entirely through
// environment variables so local (http) and remote (https) instances are
// both supported.
const (
	harborTestEnvVar      = "TF_ORAS_HARBOR_TEST"
	harborAddrEnvVar      = "TF_ORAS_HARBOR_ADDR"
	harborRepoEnvVar      = "TF_ORAS_HARBOR_REPO"
	harborUserEnvVar      = "TF_ORAS_HARBOR_USERNAME"
	harborPassEnvVar      = "TF_ORAS_HARBOR_PASSWORD"
	harborPlainHTTPEnvVar = "TF_ORAS_HARBOR_PLAIN_HTTP"

	// defaultHarborRepo is the full project/repo path used when
	// TF_ORAS_HARBOR_REPO is unset.
	defaultHarborRepo = "library/oras-state-ci"
)

// requireHarborTest skips the test unless TF_ORAS_HARBOR_TEST is set, and
// returns the configured registry address (host:port).
func requireHarborTest(t *testing.T) string {
	t.Helper()
	if os.Getenv(harborTestEnvVar) == "" {
		t.Skipf("set %s to run Harbor integration tests", harborTestEnvVar)
	}
	addr := os.Getenv(harborAddrEnvVar)
	if addr == "" {
		t.Fatalf("set %s to the Harbor registry address (host:port)", harborAddrEnvVar)
	}
	return addr
}

// harborConfig assembles the base Harbor client configuration from the
// TF_ORAS_HARBOR_* environment variables. PlainHTTP defaults to true for
// local instances; set TF_ORAS_HARBOR_PLAIN_HTTP=0 for https endpoints.
func harborConfig() Config {
	return Config{
		PlainHTTP: os.Getenv(harborPlainHTTPEnvVar) != "0",
		Username:  os.Getenv(harborUserEnvVar),
		Password:  os.Getenv(harborPassEnvVar),
	}
}

// harborRepo returns the full project/repo path, defaulting to
// defaultHarborRepo.
func harborRepo() string {
	if repo := os.Getenv(harborRepoEnvVar); repo != "" {
		return repo
	}
	return defaultHarborRepo
}

// harborWorkspace returns a unique workspace per test run so concurrent CI
// runs never contend on the same tags, and cleanup never races.
func harborWorkspace() string {
	return fmt.Sprintf("ci-%d", time.Now().UnixNano())
}

// newHarborClient creates a *Client configured for the Harbor registry from
// the environment, merged with the provided config overrides.
func newHarborClient(t *testing.T, addr string, cfg Config) *Client {
	t.Helper()
	base := harborConfig()
	base.Compression = cfg.Compression
	if cfg.MaxVersions != 0 {
		base.MaxVersions = cfg.MaxVersions
	}
	c, err := NewClient(addr, harborRepo(), base)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return c
}

// TestHarborIntegration_StateRoundTrip exercises the state CRUD lifecycle
// against a real Harbor registry: empty Get, Put, Get back, Delete, Get
// after delete. Unlike ghcr.io, Harbor supports manifest DELETE, so the real
// delete path is asserted without tolerance for 405.
func TestHarborIntegration_StateRoundTrip(t *testing.T) {
	addr := requireHarborTest(t)
	ctx := context.Background()
	c := newHarborClient(t, addr, Config{})
	ws := harborWorkspace()

	data, err := c.Get(ctx, ws)
	if err != nil {
		t.Fatalf("Get on empty: %v", err)
	}
	if data != nil {
		t.Fatal("expected nil on empty workspace")
	}

	stateData := []byte(`{"version":4,"serial":1,"harbor-integration":true}`)
	if err := c.Put(ctx, ws, stateData); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, err := c.Get(ctx, ws)
	if err != nil {
		t.Fatalf("Get after Put: %v", err)
	}
	if !bytes.Equal(got, stateData) {
		t.Fatalf("Get returned %q, want %q", string(got), string(stateData))
	}

	// Mutable tag overwrite: a second Put replaces the current tag and the
	// new payload is observed byte-intact (last-writer-wins).
	stateData2 := []byte(`{"version":4,"serial":2,"harbor-integration":true}`)
	if err := c.Put(ctx, ws, stateData2); err != nil {
		t.Fatalf("second Put: %v", err)
	}
	got, err = c.Get(ctx, ws)
	if err != nil {
		t.Fatalf("Get after second Put: %v", err)
	}
	if !bytes.Equal(got, stateData2) {
		t.Fatalf("Get returned %q, want %q", string(got), string(stateData2))
	}

	if err := c.Delete(ctx, ws); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	data, err = c.Get(ctx, ws)
	if err != nil {
		t.Fatalf("Get after Delete: %v", err)
	}
	if data != nil {
		t.Fatal("expected nil after delete")
	}
}

// TestHarborIntegration_LockUnlock verifies generation-based optimistic
// locking against a real Harbor registry.
func TestHarborIntegration_LockUnlock(t *testing.T) {
	addr := requireHarborTest(t)
	ctx := context.Background()
	c := newHarborClient(t, addr, Config{})
	ws := harborWorkspace()

	id1, err := c.Lock(ctx, ws, LockInfo{
		ID:        "harbor-lock-1",
		Operation: "harbor-integration",
	})
	if err != nil {
		t.Fatalf("first lock: %v", err)
	}

	// Concurrent locker with a different ID must fail.
	_, err = c.Lock(ctx, ws, LockInfo{
		ID:        "harbor-lock-2",
		Operation: "harbor-integration-concurrent",
	})
	if err == nil {
		t.Fatal("expected concurrent lock to fail")
	}

	if err := c.Unlock(ctx, ws, id1); err != nil {
		t.Fatalf("Unlock: %v", err)
	}

	// Lock after unlock must succeed; release to leave the namespace clean.
	id3, err := c.Lock(ctx, ws, LockInfo{
		ID:        "harbor-lock-3",
		Operation: "after-unlock",
	})
	if err != nil {
		t.Fatalf("lock after unlock: %v", err)
	}
	if err := c.Unlock(ctx, ws, id3); err != nil {
		t.Fatalf("final unlock: %v", err)
	}
}

// TestHarborIntegration_HistoryAndCompression verifies compressed history
// publication and client-side version retention against a real Harbor
// registry: unlike ghcr.io, Harbor supports manifest DELETE, so with
// MaxVersions=2 only the two most recent versions are retained and readable.
func TestHarborIntegration_HistoryAndCompression(t *testing.T) {
	addr := requireHarborTest(t)
	ctx := context.Background()
	ws := harborWorkspace()

	c := newHarborClient(t, addr, Config{
		Compression: true,
		MaxVersions: 2,
	})

	states := make([][]byte, 3)
	for i := range states {
		states[i] = []byte(fmt.Sprintf(`{"version":4,"serial":%d,"run":%q}`, i+1, ws))
		if err := c.Put(ctx, ws, states[i]); err != nil {
			t.Fatalf("Put %d: %v", i+1, err)
		}
	}
	// Retention runs asynchronously after each Put.
	c.WaitForRetention()

	versions, err := c.ListVersions(ctx, ws)
	if err != nil {
		t.Fatalf("ListVersions: %v", err)
	}
	if len(versions) != 2 {
		t.Fatalf("ListVersions returned %d versions, want 2 (MaxVersions=2)", len(versions))
	}
	for i, metadata := range versions {
		wantVersion := i + 2
		if metadata.Version != wantVersion {
			t.Fatalf("versions[%d] = %d, want ascending version %d", i, metadata.Version, wantVersion)
		}
		got, err := c.GetVersion(ctx, ws, wantVersion)
		if err != nil {
			t.Fatalf("GetVersion(%d): %v", wantVersion, err)
		}
		// Version N holds the payload of Put N, so the retained versions
		// (2 and 3 after pruning version 1) map to states[1] and states[2].
		if want := states[wantVersion-1]; !bytes.Equal(got, want) {
			t.Fatalf("GetVersion(%d) = %q, want %q", wantVersion, got, want)
		}
	}
}
