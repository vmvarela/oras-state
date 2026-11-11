package orasstate

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

func TestClient_HistoryListAndGetVersion(t *testing.T) {
	ctx := context.Background()
	repo := &orasRepositoryClient{inner: newFakeORASRepo()}
	client := newTestClient(repo)
	client.config.MaxVersions = 5

	states := [][]byte{
		[]byte(`{"serial":1}`),
		[]byte(`{"serial":2}`),
		[]byte(`{"serial":3}`),
	}
	for _, state := range states {
		if err := client.Put(ctx, "default", state); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	client.WaitForRetention()

	versions, err := client.ListVersions(ctx, "default")
	if err != nil {
		t.Fatalf("ListVersions: %v", err)
	}
	if len(versions) != len(states) {
		t.Fatalf("ListVersions returned %d versions, want %d", len(versions), len(states))
	}
	for i, version := range versions {
		want := i + 1
		if version.Version != want {
			t.Errorf("version[%d].Version = %d, want %d", i, version.Version, want)
		}
		if version.UpdatedAt.IsZero() {
			t.Errorf("version[%d].UpdatedAt is zero", i)
		}
		if version.Digest == "" {
			t.Errorf("version[%d].Digest is empty", i)
		}

		got, err := client.GetVersion(ctx, "default", want)
		if err != nil {
			t.Fatalf("GetVersion(%d): %v", want, err)
		}
		if !bytes.Equal(got, states[i]) {
			t.Errorf("GetVersion(%d) = %q, want %q", want, got, states[i])
		}
	}
}

func TestClient_GetVersionMissingDoesNotFallBackToCurrent(t *testing.T) {
	ctx := context.Background()
	repo := &orasRepositoryClient{inner: newFakeORASRepo()}
	client := newTestClient(repo)
	client.config.MaxVersions = 2

	if err := client.Put(ctx, "default", []byte("current")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, err := client.GetVersion(ctx, "default", 2)
	if !errors.Is(err, ErrStateVersionNotFound) {
		t.Fatalf("GetVersion missing error = %v, want ErrStateVersionNotFound", err)
	}
	if got != nil {
		t.Fatalf("GetVersion missing returned %q, want nil", got)
	}
}

func TestClient_GetVersionUsesCompressionAndSizeLimit(t *testing.T) {
	ctx := context.Background()
	repo := &orasRepositoryClient{inner: newFakeORASRepo()}
	client := newTestClient(repo)
	client.config.MaxVersions = 2
	client.config.Compression = true
	client.config.MaxStateSize = 128

	state := bytes.Repeat([]byte("history"), 8)
	if err := client.Put(ctx, "default", state); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, err := client.GetVersion(ctx, "default", 1)
	if err != nil {
		t.Fatalf("GetVersion compressed: %v", err)
	}
	if !bytes.Equal(got, state) {
		t.Fatalf("GetVersion compressed = %q, want %q", got, state)
	}

	client.config.MaxStateSize = 8
	if _, err := client.GetVersion(ctx, "default", 1); err == nil {
		t.Fatal("GetVersion should enforce MaxStateSize")
	}
}

func TestClient_HistorySupportsLegacyWorkspaceLayout(t *testing.T) {
	ctx := context.Background()
	repo := &orasRepositoryClient{inner: newFakeORASRepo()}
	client := newTestClient(repo)
	client.config.WorkspaceLayout = WorkspaceLayoutLegacy
	client.config.MaxVersions = 2

	const workspace = "team/prod"
	if err := client.Put(ctx, workspace, []byte("legacy-layout-state")); err != nil {
		t.Fatalf("Put legacy layout: %v", err)
	}

	versions, err := client.ListVersions(ctx, workspace)
	if err != nil {
		t.Fatalf("ListVersions legacy layout: %v", err)
	}
	if len(versions) != 1 || versions[0].Version != 1 {
		t.Fatalf("ListVersions legacy layout = %#v, want version 1", versions)
	}

	got, err := client.GetVersion(ctx, workspace, 1)
	if err != nil {
		t.Fatalf("GetVersion legacy layout: %v", err)
	}
	if string(got) != "legacy-layout-state" {
		t.Fatalf("GetVersion legacy layout = %q", got)
	}
}

func TestClient_GetVersionRejectsInvalidVersion(t *testing.T) {
	client := newTestClient(&orasRepositoryClient{inner: newFakeORASRepo()})
	for _, version := range []int{0, -1, maxStateVersion + 1} {
		if _, err := client.GetVersion(context.Background(), "default", version); err == nil {
			t.Errorf("GetVersion(%d) should fail", version)
		}
	}
}

func TestClient_PutContinuesVersionNumberingAfterDelete(t *testing.T) {
	ctx := context.Background()
	repo := &orasRepositoryClient{inner: newFakeORASRepo()}
	client := newTestClient(repo)
	client.config.MaxVersions = 2

	for _, s := range []string{"s1", "s2", "s3"} {
		if err := client.Put(ctx, "default", []byte(s)); err != nil {
			t.Fatalf("Put(%s): %v", s, err)
		}
		client.Close()
	}

	if err := client.Delete(ctx, "default"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := client.Put(ctx, "default", []byte("s4")); err != nil {
		t.Fatalf("Put(s4): %v", err)
	}
	client.Close()

	versions, err := client.ListVersions(ctx, "default")
	if err != nil {
		t.Fatalf("ListVersions: %v", err)
	}
	if len(versions) != 2 || versions[0].Version != 3 || versions[1].Version != 4 {
		t.Fatalf("ListVersions after Delete+Put = %#v, want [3, 4]", versions)
	}
	got4, err := client.GetVersion(ctx, "default", 4)
	if err != nil || string(got4) != "s4" {
		t.Fatalf("GetVersion(4) = %q, %v; want %q", got4, err, "s4")
	}
}

func TestClient_ListVersionsSingleTagListAndSingleFetchPerVersion(t *testing.T) {
	ctx := context.Background()
	repo := &countingRepo{delegatingRepo: delegatingRepo{inner: newFakeORASRepo()}}
	client := newTestClient(&orasRepositoryClient{inner: repo})
	client.config.MaxVersions = 5

	for _, s := range []string{"v1", "v2", "v3"} {
		if err := client.Put(ctx, "default", []byte(s)); err != nil {
			t.Fatalf("Put(%s): %v", s, err)
		}
		client.Close()
	}

	repo.tags.Store(0)
	repo.resolve.Store(0)
	repo.fetch.Store(0)

	versions, err := client.ListVersions(ctx, "default")
	if err != nil {
		t.Fatalf("ListVersions: %v", err)
	}
	if len(versions) != 3 {
		t.Fatalf("len(versions) = %d, want 3", len(versions))
	}
	// 1 tag listing (reused from preflight) and 1 preflight state fetch + 3 version fetches = 4.
	if tags, fetch := repo.tags.Load(), repo.fetch.Load(); tags != 1 || fetch != 4 {
		t.Fatalf("ListVersions calls: Tags=%d Fetch=%d; want 1, 4", tags, fetch)
	}
}
