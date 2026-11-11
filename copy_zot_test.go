package orasstate

import (
	"context"
	"testing"
)

// TestZotIntegration_Copy exercises the real ORAS graph copy path between two
// repositories on one local zot registry, after a real Lock+Unlock sequence.
func TestZotIntegration_Copy(t *testing.T) {
	requireZotTest(t)
	ctx := context.Background()
	port := freeLocalPort(t)
	addr := "localhost:" + port
	startZot(t, port)
	waitForZot(t, addr)

	src := newZotClient(t, addr, "copy-test/src", Config{MaxVersions: 2})
	if err := src.Put(ctx, "default", []byte(`{"serial":1}`)); err != nil {
		t.Fatalf("source Put: %v", err)
	}
	src.WaitForRetention()

	lockID, err := src.Lock(ctx, "default", LockInfo{ID: "copy-zot-holder"})
	if err != nil {
		t.Fatalf("Lock: %v", err)
	}
	if err := src.Unlock(ctx, "default", lockID); err != nil {
		t.Fatalf("Unlock: %v", err)
	}

	plan, err := src.PlanCopy(ctx)
	if err != nil {
		t.Fatalf("PlanCopy: %v", err)
	}
	artifacts := plan.Artifacts()
	if len(artifacts) == 0 {
		t.Fatal("plan has no artifacts")
	}

	dst := newZotClient(t, addr, "copy-test/dst", Config{MaxVersions: 2})
	report, err := src.CopyTo(ctx, dst, plan)
	if err != nil {
		t.Fatalf("CopyTo: %v", err)
	}
	if len(report.Copied) != len(artifacts) {
		t.Fatalf("copied = %v, want %v", copyArtifactTags(report.Copied), copyPlanTags(plan))
	}

	for _, artifact := range artifacts {
		tag := artifact.Tag
		srcDesc, err := src.repoClient.inner.Resolve(ctx, tag)
		if err != nil {
			t.Fatalf("resolve source %q: %v", tag, err)
		}
		dstDesc, err := dst.repoClient.inner.Resolve(ctx, tag)
		if err != nil {
			t.Fatalf("resolve destination %q: %v", tag, err)
		}
		if srcDesc.Digest != dstDesc.Digest {
			t.Errorf("tag %q digest = %s, want %s", tag, dstDesc.Digest, srcDesc.Digest)
		}
	}
}

// TestZotIntegration_CopyLegacyHistory copies the old legacy history shape
// state-foo-v1 (workspace annotation foo) through a real registry.
func TestZotIntegration_CopyLegacyHistory(t *testing.T) {
	requireZotTest(t)
	ctx := context.Background()
	port := freeLocalPort(t)
	addr := "localhost:" + port
	startZot(t, port)
	waitForZot(t, addr)

	src, err := NewClient(addr, "copy-test/legacy-src", Config{PlainHTTP: true, WorkspaceLayout: WorkspaceLayoutLegacy})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	historyTag := stateTagPrefix + "foo" + stateVersionTagSeparator + "1"
	desc, err := newWorkspaceClient(src, "foo").packStateManifestAnnotated(ctx, nil, map[string]string{
		annotationWorkspace:    "foo",
		annotationStateVersion: "1",
	})
	if err != nil {
		t.Fatalf("pack legacy history: %v", err)
	}
	if err := src.repoClient.inner.Tag(ctx, desc, historyTag); err != nil {
		t.Fatalf("tag legacy history: %v", err)
	}

	plan, err := src.PlanCopy(ctx)
	if err != nil {
		t.Fatalf("PlanCopy: %v", err)
	}
	artifact := copyArtifactByTag(t, plan, historyTag)
	if artifact.Kind != ArtifactKindHistory || artifact.Workspace != "foo" {
		t.Fatalf("artifact = %+v, want history for workspace foo", artifact)
	}

	dst, err := NewClient(addr, "copy-test/legacy-dst", Config{PlainHTTP: true, WorkspaceLayout: WorkspaceLayoutLegacy})
	if err != nil {
		t.Fatalf("NewClient dst: %v", err)
	}
	if _, err := src.CopyTo(ctx, dst, plan); err != nil {
		t.Fatalf("CopyTo: %v", err)
	}
	got, err := dst.repoClient.inner.Resolve(ctx, historyTag)
	if err != nil {
		t.Fatalf("resolve destination %q: %v", historyTag, err)
	}
	if got.Digest != desc.Digest {
		t.Fatalf("destination digest = %s, want %s", got.Digest, desc.Digest)
	}
}
