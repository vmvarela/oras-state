package orasstate

import (
	"context"
	"strings"
	"testing"
)

func TestWorkspaceLayout_DefaultRemainsHashed(t *testing.T) {
	client := &Client{config: Config{}}
	ws := newWorkspaceClient(client, "default")

	if !strings.HasPrefix(ws.stateTag, stateTagPrefix) {
		t.Fatalf("state tag %q does not have expected prefix", ws.stateTag)
	}
	id := strings.TrimPrefix(ws.stateTag, stateTagPrefix)
	if len(id) != 64 {
		t.Fatalf("default layout must use full SHA-256 workspace IDs, got %q", id)
	}
}

func TestWorkspaceLayout_LegacyGhotenPreservesExistingTags(t *testing.T) {
	client := &Client{config: Config{WorkspaceLayout: WorkspaceLayoutLegacyGhoten}}

	t.Run("literal OCI tag", func(t *testing.T) {
		ws := newWorkspaceClient(client, "default")
		if ws.stateTag != "state-default" {
			t.Fatalf("state tag = %q, want state-default", ws.stateTag)
		}
		if ws.lockTag != "locked-default" {
			t.Fatalf("lock tag = %q, want locked-default", ws.lockTag)
		}
		if ws.versionTagBase != "stver-default" {
			t.Fatalf("version tag base = %q, want stver-default", ws.versionTagBase)
		}
	})

	t.Run("invalid OCI tag", func(t *testing.T) {
		ws := newWorkspaceClient(client, "my workspace")
		id := strings.TrimPrefix(ws.stateTag, stateTagPrefix)
		if !strings.HasPrefix(id, "ws-") || len(id) != len("ws-")+16 {
			t.Fatalf("legacy hash fallback = %q, want ws- plus 16 hex chars", id)
		}
	})
}


func TestLegacyGhotenDeleteWorkspaceRemovesStateHistoryAndLock(t *testing.T) {
	ctx := context.Background()
	c := newTestClient(&orasRepositoryClient{inner: newFakeORASRepo()})
	c.config.WorkspaceLayout = WorkspaceLayoutLegacyGhoten
	c.config.MaxVersions = 2

	if err := c.Put(ctx, "dev", []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := c.Put(ctx, "dev", []byte("two")); err != nil {
		t.Fatal(err)
	}
	c.WaitForRetention()

	if _, err := c.Lock(ctx, "dev", LockInfo{ID: "old"}); err != nil {
		t.Fatal(err)
	}
	if err := c.DeleteWorkspace(ctx, "dev"); err != nil {
		t.Fatal(err)
	}

	data, err := c.Get(ctx, "dev")
	if err != nil {
		t.Fatal(err)
	}
	if data != nil {
		t.Fatalf("state remains after DeleteWorkspace: %q", data)
	}

	// The old lock must also be gone; acquiring a new one proves the workspace
	// was deleted as a unit rather than only deleting the state tag.
	if _, err := c.Lock(ctx, "dev", LockInfo{ID: "new"}); err != nil {
		t.Fatalf("lock remains after DeleteWorkspace: %v", err)
	}
	if err := c.Unlock(ctx, "dev", "new"); err != nil {
		t.Fatal(err)
	}

	workspaces, err := c.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, workspace := range workspaces {
		if workspace == "dev" {
			t.Fatalf("deleted workspace still listed: %v", workspaces)
		}
	}
}
