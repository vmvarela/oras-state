package orasstate

import (
	"bytes"
	"context"
	"fmt"
	"testing"
)

func TestZotIntegration_HistoryListAndGetVersion(t *testing.T) {
	requireZotTest(t)
	ctx := context.Background()
	port := freeLocalPort(t)
	addr := "localhost:" + port
	startZot(t, port)
	waitForZot(t, addr)

	client := newZotClient(t, addr, "provider-test/history", Config{
		MaxVersions: 3,
		Compression: true,
	})

	states := make([][]byte, 3)
	for i := range states {
		states[i] = []byte(fmt.Sprintf(`{"serial":%d,"payload":"history"}`, i+1))
		if err := client.Put(ctx, "default", states[i]); err != nil {
			t.Fatalf("Put %d: %v", i+1, err)
		}
	}
	client.WaitForRetention()

	versions, err := client.ListVersions(ctx, "default")
	if err != nil {
		t.Fatalf("ListVersions: %v", err)
	}
	if len(versions) != 3 {
		t.Fatalf("ListVersions returned %d versions, want 3", len(versions))
	}

	for i, metadata := range versions {
		wantVersion := i + 1
		if metadata.Version != wantVersion {
			t.Fatalf("version[%d] = %d, want %d", i, metadata.Version, wantVersion)
		}
		if metadata.Digest == "" || metadata.UpdatedAt.IsZero() {
			t.Fatalf("incomplete metadata for version %d: %#v", wantVersion, metadata)
		}
		got, err := client.GetVersion(ctx, "default", wantVersion)
		if err != nil {
			t.Fatalf("GetVersion(%d): %v", wantVersion, err)
		}
		if !bytes.Equal(got, states[i]) {
			t.Fatalf("GetVersion(%d) = %q, want %q", wantVersion, got, states[i])
		}
	}
}
