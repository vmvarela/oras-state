# OCI registry compatibility

`oras-state` stores Terraform/OpenTofu state as OCI artifacts, so registry behavior is part of the state engine's contract. This document records which registries are validated, how each test suite runs, and the registry-specific differences that affect the engine.

Compatibility testing lives in this repository because it belongs to the shared OCI state engine, not to a single adapter (`terraform-provider-oras`). Engine semantics — layout, locking model, retention, concurrency — are described in the [README](../README.md); this document covers only registry behavior.

## Tested matrix

| Registry | State read/write | Locking | Version history | Client-side retention | Compression | Large state |
|---|---|---|---|---|---|---|
| Zot | ✅ CI + local | ✅ CI + local | ✅ CI + local | ✅ CI + local | ✅ CI + local | ✅ 2 MiB local |
| GHCR (`ghcr.io`) | ✅ CI | ✅ CI | ✅ CI | ⚠️ skipped by design | ✅ CI | not tested |
| Harbor | ✅ opt-in | ✅ opt-in | ✅ opt-in | ✅ opt-in | ✅ opt-in | not tested |

- **CI** = exercised on every push and pull request by the `zot-integration` and `ghcr-integration` jobs in [`.github/workflows/test.yml`](../.github/workflows/test.yml). The GHCR job runs on non-fork pull requests and pushes to the default branch.
- **opt-in** = exercised manually with the Harbor suite (see [Running the suites](#running-the-suites)); evaluation results are recorded in this document.
- **⚠️ GHCR retention** = the engine skips automatic pruning on `ghcr.io`, so `MaxVersions` does not bound history there; the GHCR suite asserts this documented behavior.

Workspace/state identity mapping is exercised live on Zot (`TestZotWorkspaceIsolation`, including legacy-layout migration). GHCR and Harbor suites use unique per-run workspaces.

## Registry-specific behavior

### Zot

The mandatory CI target. Tests run PlainHTTP without authentication, using either Docker (`ghcr.io/project-zot/zot-linux-amd64:v2.1.0`) or a native binary via `TF_ORAS_ZOT_BINARY`. Full state lifecycle, locking, retention, compression, history, workspace isolation and concurrent-writer behavior are covered.

### GitHub Container Registry (ghcr.io)

- Manifest `DELETE` is not supported (HTTP 405). The engine falls back to the GitHub Packages API for pruning.
- Because that fallback is best-effort, automatic pruning is skipped on `ghcr.io`: history keeps growing regardless of `MaxVersions`. Stop old writers before relying on retention semantics.
- Deleting the last tagged version of a package can be rejected by GHCR; the integration tests keep an anchor fixture for this reason.
- Authentication uses a token with `packages:write` scope (the CI job uses the workflow `GITHUB_TOKEN`).

### Harbor

- Manifest `DELETE` is supported; the opt-in suite asserts the real delete path.
- Tags are overwritable by default; immutability is opt-in per project. Keep immutability off for state repositories, or the engine's overwrite model breaks.
- Server-side retention rules are independent of the client and can delete artifacts the engine still references. Leave them disabled for state projects and rely on the engine's `MaxVersions`.
- Deleting artifacts does not free storage until Harbor's garbage collection runs.
- `/v2/_catalog` requires an administrator account; use project robot accounts (basic auth) for automation — push permission implies pull.
- Evaluated 2026-10-18 against a local Harbor v2.15.4 over HTTP (admin account): state round-trip including delete, lock/unlock, and compressed history with client-side retention all pass (`TestHarborIntegration_*`).

## Mutable tags and concurrency

OCI tags are last-writer-wins with no compare-and-swap. The engine:

- treats a moved mutable tag as a fail-closed error (`ErrMutableTagMoved`) when a transient failure leaves the outcome ambiguous, and
- retries a bounded number of times before publishing; the retry may overwrite a rival writer's tag (documented limitation, see the README).

Live validation: Zot exercises concurrent writers (`TestZotIntegration_MutableTagConcurrency`) and sequential last-writer-wins; the GHCR and Harbor suites exercise sequential overwrites and lock contention. Failure paths (lost responses, rival moves, ambiguous outcomes) are covered deterministically by the unit seam tests (`mutabletag_test.go`, `tagprepush_test.go`, `recovery_test.go`).

## TLS and custom CAs

Transport selection (`CAFile`, `TLSSkipVerify`) and HTTP client construction are unit-tested; no live registry suite exercises custom CAs yet. Registries exposed over TLS with a private CA are expected to work through `Config.CAFile`.

## Running the suites

```sh
# Unit suite (all platforms, no registry)
go test -race -count=1 -timeout 10m ./...

# Zot — Docker, or a native binary
TF_ORAS_ZOT_TEST=1 go test -race -count=1 -timeout 300s -run Zot ./...
TF_ORAS_ZOT_TEST=1 TF_ORAS_ZOT_BINARY=/path/to/zot go test -race -count=1 -timeout 300s -run Zot ./...

# GHCR (token needs the packages:write scope)
TF_ORAS_GHCR_TEST=1 TF_ORAS_GHCR_TOKEN=... go test -race -count=1 -timeout 300s -run TestGHCRIntegration ./...

# Harbor (any reachable instance; plain HTTP by default, set TF_ORAS_HARBOR_PLAIN_HTTP=0 for https)
TF_ORAS_HARBOR_TEST=1 \
  TF_ORAS_HARBOR_ADDR=harbor.example.com \
  TF_ORAS_HARBOR_REPO=myproject/oras-state \
  TF_ORAS_HARBOR_USERNAME='robot$myproject+ci' TF_ORAS_HARBOR_PASSWORD=... \
  go test -race -count=1 -timeout 300s -run Harbor ./...
```

## Known gaps

- No live TLS/custom-CA suite (unit coverage only).
- Large state is validated live on Zot (2 MiB); GHCR and Harbor are not exercised with large states.
- Harbor validation is manual/opt-in, not part of CI.
