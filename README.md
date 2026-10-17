# oras-state

A small Go library for storing state as OCI artifacts using [ORAS](https://oras.land/).

`oras-state` is intentionally independent from Terraform, OpenTofu, Ghoten, and any provider/plugin framework. Consumers provide logical state IDs and credentials; the library owns OCI storage semantics.

## Responsibilities

- OCI state read/write/delete
- state locking
- workspace/state identity
- version retention
- mutable-tag safety
- GHCR compatibility
- Zot/OCI registry interoperability
- optional gzip compression
- caller-supplied credential resolution

## Non-goals

- Terraform StateStore APIs
- OpenTofu backend interfaces
- Terraform/OpenTofu CLI credential configuration
- state encryption

Those belong to consumers.

## Module

```text
github.com/vmvarela/oras-state
```

## Example

```go
package main

import (
    "context"
    "log"

    orasstate "github.com/vmvarela/oras-state"
)

func main() {
    ctx := context.Background()

    store, err := orasstate.NewClient("ghcr.io", "acme/state", orasstate.Config{})
    if err != nil {
        log.Fatal(err)
    }
    defer store.Close()

    if err := store.Put(ctx, "production", []byte("{}")); err != nil {
        log.Fatal(err)
    }
}
```

## Design rule

> oras-state knows how to store state in OCI. It does not know which Terraform/OpenTofu integration is using it.

## License

MPL-2.0.
