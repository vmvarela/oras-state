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

## Read-only state history

When version retention is enabled with `MaxVersions`, consumers can inspect retained history without knowing the OCI tag layout:

```go
versions, err := client.ListVersions(ctx, "default")
if err != nil {
    return err
}

for _, version := range versions {
    fmt.Printf("%d %s %s\n", version.Version, version.UpdatedAt, version.Digest)
}

previous, err := client.GetVersion(ctx, "default", versions[0].Version)
if errors.Is(err, orasstate.ErrStateVersionNotFound) {
    // The retained version no longer exists.
}
```

`ListVersions` returns versions in ascending numeric order. `GetVersion` returns the logical uncompressed state bytes and enforces the same workspace ownership, media-type, compression, and `MaxStateSize` checks as `Get`. Missing history never falls back to current state.

The API deliberately exposes logical versions and verified manifest digests, not OCI tag names. It is read-only: restore, per-version deletion, and garbage collection are outside this contract.

## Concurrency model

OCI mutable tags are last-writer-wins and do not provide compare-and-swap semantics.

`oras-state` therefore does **not** claim strict distributed CAS locking. It uses generation-aware lock metadata, ownership verification, bounded observation after ambiguous writes, and fail-closed behavior where blindly retrying a mutable tag could overwrite another writer.

In particular, `ErrMutableTagMoved` means a mutable tag moved or became ambiguous and the caller should stop rather than retry the publication blindly.

A `PartialPublicationError` means the main state tag was published successfully but a later publication step, such as a version tag, failed. Callers must not treat that as a wholly failed state write.

## Workspace layout

The default layout hashes every logical state/workspace name using full SHA-256 before constructing OCI tags. This avoids collisions and keeps arbitrary logical names independent from registry tag syntax.

`WorkspaceLayoutLegacy` exists only for compatibility with repositories created by the historical layout. New consumers should use the default hashed layout.

Do not mix old and new layouts in the same repository.

## Development

```bash
go test ./...
go test -race ./...
```

The CI suite also exercises registry integration behavior, including Zot and GHCR-specific paths.

## Design rule

This repository should stay product-neutral.

If a change is specific to Terraform's plugin framework, it belongs in `terraform-provider-oras`. If it is specific to OpenTofu/Ghoten backend integration, it belongs in Ghoten. Shared OCI state semantics belong here.

## License

MPL-2.0. See [LICENSE](LICENSE).
