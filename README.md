# oras-state

A small Go library for storing state in OCI registries using the ORAS protocol.

`oras-state` contains the shared OCI state engine used by:

- [vmvarela/terraform-provider-oras](https://github.com/vmvarela/terraform-provider-oras), which adapts it to Terraform's experimental `StateStore` API.
- [vmvarela/ghoten](https://github.com/vmvarela/ghoten), which adapts it to Ghoten's native `backend "oras"`.

The library itself is intentionally independent from Terraform Core, OpenTofu Core, Ghoten, and the Terraform Plugin Framework.

## Responsibilities

`oras-state` owns the registry-facing state semantics:

- read and write state as OCI artifacts
- workspace/state identity and tag layout
- locking and lock verification
- bounded state size
- optional gzip compression
- optional state-version retention
- TLS and custom CA handling
- caller-supplied credential resolution
- GHCR compatibility behavior
- fail-closed handling for ambiguous mutable-tag publication

Adapters remain responsible for product-specific configuration, diagnostics, CLI behavior, and credential policy.

## Installation

```bash
go get github.com/vmvarela/oras-state
```

The module currently targets Go 1.26.

## Basic API

Create a client for one OCI repository:

```go
client, err := orasstate.NewClient(
    "ghcr.io",
    "myorg/infra-state",
    orasstate.Config{
        CredentialFunc: resolver,
        Compression:    true,
        LockTTL:        15 * time.Minute,
        MaxVersions:    10,
    },
)
if err != nil {
    return err
}
```

State operations are keyed by a caller-defined state ID:

```go
data, err := client.Get(ctx, "default")
if err != nil {
    return err
}

if err := client.Put(ctx, "default", newState); err != nil {
    return err
}

lockID, err := client.Lock(ctx, "default", orasstate.LockInfo{
    ID:        "operation-id",
    Operation: "apply",
})
if err != nil {
    return err
}

if err := client.VerifyLock(ctx, "default", lockID); err != nil {
    return err
}

defer client.Unlock(ctx, "default", lockID)
```

Callers may also provide their own HTTP client or use `BuildHTTPClient` for TLS verification, custom CAs, or explicit skip-verification behavior.

`LockTTL` sets a fixed lease duration written at lock acquisition time: leases are **not** renewed automatically while an operation runs. Set `LockTTL` longer than the longest expected run so `VerifyLock` does not expire mid-apply (or leave `LockTTL` at `0` so locks never expire automatically). Both `Lock` and `Unlock` require a non-empty lock ID.

In the default hashed layout, `Get` returns `nil` when both the current-state tag and its workspace marker are absent. If the current-state tag is absent but a valid marker exists, it returns an error instead of reporting the state as absent. This detects missing state for workspaces that have been marked; it does not cover older workspaces until their next successful `Put`. If a tag resolves but its manifest or layer content is missing, `Get` also returns an error. `WorkspaceLayoutLegacy` keeps its historical behavior.

## Read-only state history

A positive `MaxVersions` enables and publishes state-version history — including on ghcr.io, where pruning is skipped — so consumers can inspect retained history without knowing the OCI tag layout:

```go
versions, err := client.ListVersions(ctx, "default")
if err != nil {
    return err
}

for _, version := range versions {
    fmt.Printf("%d %s %s\n", version.Version, version.UpdatedAt, version.Digest)
}

if len(versions) > 0 {
    previous, err := client.GetVersion(ctx, "default", versions[0].Version)
    if err != nil && !errors.Is(err, orasstate.ErrStateVersionNotFound) {
        return err
    }
    if previous != nil {
        fmt.Printf("previous version: %d bytes\n", len(previous))
    }
}
```

`ListVersions` returns versions in ascending numeric order. `GetVersion` returns the logical uncompressed state bytes and enforces the same workspace ownership, media-type, compression, and `MaxStateSize` checks as `Get`. A missing version returns an error wrapping `ErrStateVersionNotFound` — history never falls back to current state. If `GetVersion` resolves a version tag but its manifest or layer content is missing, it returns a distinct error (not `ErrStateVersionNotFound`): incomplete history is never reported as absent.

`MaxVersions <= 0` stops publishing new version history (already-published history can remain readable); a positive value publishes history and targets that many retained versions. On registries other than ghcr.io, pruning is best-effort: unmarked, unknown, or ambiguous legacy entries may be preserved. On ghcr.io, history is still published and readable, but automatic pruning is skipped — `MaxVersions` does **not** bound history there, and it may grow without limit.

See the [GHCR retention investigation](docs/ghcr-retention-investigation.md) for deletion semantics, required permissions, and the evidence needed before enabling automatic pruning.

Before relying on the new retention behavior, stop old writers and wait for their asynchronous retention workers to finish: a new instance does not fence or protect against old processes still reading, writing, or pruning.

The API deliberately exposes logical versions and verified manifest digests, not OCI tag names. It is read-only: restore, per-version deletion, and garbage collection are outside this contract.

## Copying state to another repository

`PlanCopy` and `CopyTo` migrate a repository to another one. The copy works on OCI graphs, so state bytes are never decoded or rewritten:

```go
plan, err := source.PlanCopy(ctx)
if err != nil {
    return err
}

report, err := source.CopyTo(ctx, destination, plan)
if err != nil {
    return err
}
fmt.Printf("copied %d, already present %d, verified %d\n",
    len(report.Copied), len(report.Existing), len(report.Verified))
```

`PlanCopy` pins every source tag and its current digest into an immutable `CopyPlan` (inspectable via `plan.Source()`, `plan.Artifacts()`, and `plan.Skipped()`), so `CopyTo` copies exactly that snapshot. `CopyTo` copies each tagged graph, re-resolves every destination tag, walks its graph again, and re-checks both tag inventories before reporting success.

What the copy does and does not do:

- It copies tagged OCI graphs only. Untagged package versions and referrers are not discovered or copied.
- It skips validated `locked-*` and `unlocked-*` markers: locks are process state, not state content. A live lock refuses the plan and the copy.
- It refuses a destination that contains a tag outside the plan, or a planned tag pointing at a different digest.
- It never deletes from the source. It refuses to overwrite a destination tag it detects, but that check is best-effort, not compare-and-swap.
- It is not compare-and-swap. Stop all writers while planning and copying; a write can still race the final inventory.
- It is resumable: a re-run skips tags that already point at the planned digest with a complete graph, and copies the rest.

The usual migration is: drain writers, copy and verify, repoint consumers at the new package, then retire the source package separately. Nothing is published to the source, and no OCI tag is deleted. The whole copy runs under the caller's context; pass a deadline if you want one.

## Concurrency model

OCI mutable tags are last-writer-wins and do not provide compare-and-swap semantics.
See the [conditional-write investigation](docs/oci-cas-investigation.md) for the OCI, ORAS, and registry evidence behind this limit.

`oras-state` therefore does **not** claim strict distributed CAS locking. It uses generation-aware lock metadata, ownership verification, bounded observation after ambiguous writes, and fail-closed behavior where blindly retrying a mutable tag could overwrite another writer.

In particular, `ErrMutableTagMoved` means a mutable tag moved or became ambiguous and the caller should stop rather than retry the publication blindly.

A `PartialPublicationError` means the main state tag was published at the time of the write, but a later publication step, such as the history tag, failed. Callers must not treat that as a wholly failed state write, and must not assume the state tag remains current when the error is observed: a concurrent writer may already have superseded it.

## Workspace layout

The default layout hashes every logical state/workspace name using full SHA-256 before constructing OCI tags. This avoids collisions and keeps arbitrary logical names independent from registry tag syntax.

On the default layout, the first `Put` publishes a small, internal `wsmeta-` workspace marker before publishing state. If marker publication fails, `Put` fails without publishing state. If initialization then fails, the marker remains; `Get` fails closed and `Put`/`Lock` refuse to treat the workspace as new. Recover the state or explicitly reset the workspace with `Delete`/`DeleteWorkspace`. The marker is prospective recovery evidence, not a transaction or lock: it does not fence concurrent writers, and it cannot identify an older workspace whose state disappeared before that workspace was marked. `Lock`, `Unlock`, and history reads do not create or require the marker.

`WorkspaceLayoutLegacy` exists only for compatibility with repositories created by the historical layout. New consumers should use the default hashed layout.

Do not mix old and new layouts in the same repository.

## Development

```bash
go test ./...
go test -race ./...
```

CI automatically exercises Zot integration behavior. GHCR integration is opt-in and requires manually enabling `run_ghcr` in the Test workflow. See [docs/compatibility.md](docs/compatibility.md) for the tested registry matrix, per-registry differences, and how to run each integration suite.

## Design rule

This repository should stay product-neutral.

If a change is specific to Terraform's plugin framework, it belongs in `terraform-provider-oras`. If it is specific to OpenTofu/Ghoten backend integration, it belongs in Ghoten. Shared OCI state semantics belong here.

## License

MPL-2.0. See [LICENSE](LICENSE).
