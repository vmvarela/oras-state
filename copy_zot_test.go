package orasstate

import (
	"context"
	"testing"
)

// TestZotIntegration_Copy exercises the real ORAS graph copy path between two
// repositories on one local zot registry.
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

	plan, err := src.PlanCopy(ctx)
	if err != nil {
		t.Fatalf("PlanCopy: %v", err)
	}
	if len(plan.Artifacts) == 0 {
		t.Fatal("plan has no artifacts")
	}

	dst := newZotClient(t, addr, "copy-test/dst", Config{MaxVersions: 2})
	report, err := src.CopyTo(ctx, dst, plan)
	if err != nil {
		t.Fatalf("CopyTo: %v", err)
	}
	if len(report.Copied) != len(plan.Artifacts) {
		t.Fatalf("copied = %v, want %v", copyArtifactTags(report.Copied), copyPlanTags(plan))
	}

	for _, artifact := range plan.Artifacts {
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
