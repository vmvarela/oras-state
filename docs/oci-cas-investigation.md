# Conditional writes and OCI compare-and-swap

`oras-state` publishes mutable OCI tags for current state and locks. OCI
Distribution standardizes how a manifest is written to a tag, but does not give
clients a portable compare-and-swap (CAS) operation for changing that tag. The
engine should keep its current last-writer-wins contract and must not promise
strong distributed locking.

This investigation addresses [#11](https://github.com/vmvarela/oras-state/issues/11).
It reviewed OCI Distribution v1.1.1, ORAS Go v3.0.0, and the registry evidence
listed below on 2026-11-09. No engine behavior changes.

## What the specification guarantees

OCI Distribution v1.1.1 defines manifest publication as `PUT
/v2/<repository>/manifests/<tag>` and requires a successful write to return
`201 Created`. A tag is a mutable pointer: one manifest digest can have zero or
many tags. The general manifest-write endpoint does not define `If-Match`,
`If-None-Match`, or an ETag precondition for updating a tag.

The spec mentions conditional HTTP requests only as an optional defense when
clients update the fallback referrers index. That is a narrow `MAY`, not a
portable contract for ordinary manifest tags. OCI's conformance suite does not
test conditional manifest writes. The open
[ETag and precondition proposal](https://github.com/opencontainers/distribution-spec/issues/250)
also reflects that this behavior has not been standardized.

HTTP itself defines `If-Match` semantics for servers that implement it: the
origin server must evaluate the condition and not apply the method when it does
not match. But OCI clients cannot infer that an OCI registry implements this
optional HTTP behavior. Sending `If-Match` and receiving a normal successful
manifest `PUT` is not proof that a registry evaluated it.

## What ORAS Go exposes

In [ORAS Go v3.0.0](https://github.com/oras-project/oras-go/releases/tag/v3.0.0),
`Repository.Tag` delegates to the manifest store's `Tag` operation, which
fetches the manifest by digest and then writes it under the reference. The
public API has no expected-current-digest or conditional-write parameter.
`Resolve` returns a manifest descriptor, not the response ETag needed to replay
an opaque HTTP validator.

ORAS lets callers supply a custom HTTP `Client`, so a wrapper can add headers.
That is only a transport hook: it neither exposes a validator through `Tag` nor
makes the registry honor the header. It cannot create a portable CAS guarantee.

## Registry-specific evidence

| Registry | Evidence | Conclusion and limit |
| --- | --- | --- |
| GHCR | GitHub's container-registry documentation does not promise conditional manifest writes. Read-only requests to a public image returned `ETag` equal to `Docker-Content-Digest`; `If-Match` on GET returned 200 for both matching and false values. | Conditional `PUT` behavior remains **unverified**; no write was attempted. The GET results do not establish PUT behavior. |
| Zot 2.1.23 | Source review of [`UpdateManifest`](https://github.com/project-zot/zot/blob/v2.1.23/pkg/api/routes.go#L781) shows no precondition check in the manifest `PUT` handler. In a local test, matching and false `If-Match`, and `If-None-Match: *` on an existing tag, all returned 201 and overwrote the tag. | Observed for this version and single-node local setup. Test commands completed with `set -e`, but per-command exit codes and full logs were not saved; the GET used a malformed `Accept` header, so no conclusion depends on that GET. |
| Distribution 3.1.1 | Local test: false `If-Match` and `If-None-Match: *` on an existing tag both returned 201 and overwrote it; GET returned `ETag` equal to `Docker-Content-Digest`. | Observed for this local version only. The corrected test block completed with `set -e`, but full logs and per-command exit codes were not saved. An earlier malformed-URL run is excluded. |
| Harbor 2.14.6 | [`putManifest`](https://github.com/goharbor/harbor/blob/v2.14.6/src/server/registry/manifest.go#L173-L186) proxies writes to its backend. Optional [tag immutability middleware](https://github.com/goharbor/harbor/blob/v2.14.6/src/server/middleware/immutable/pushmf.go#L53-L77) rejects pushes to an already-immutable tag with 412; it does not compare an expected digest. | Immutability is an all-or-nothing policy, not CAS. Harbor was inspected in source only, not run live; the backend may vary by deployment. |

These results distinguish the protocol guarantee from implementation evidence:
GHCR has no documented promise and was not tested on write; Zot and Distribution
were tested at specific versions with limited retained logs; Harbor was
inspected in source only.

## Why the apparent alternatives are not CAS

- **Write by digest:** makes the manifest content-addressed and repeatable, but
  does not atomically change the mutable tag that points to it.
- **`If-Match`:** could express “replace only the version I read” only if the
  registry supplies a usable validator and enforces the condition. OCI does not
  require either for tag writes, and a server that ignores the header can make
  an attempted CAS look like an ordinary successful write.
- **`If-None-Match: *`:** could express create-only if enforced, but OCI does not
  define it for manifest tags and implementations can ignore it.
- **Immutable tags:** prevent later changes under a policy; they cannot express
  “update if the current digest is X,” and would conflict with normal state
  overwrites.

## Recommendation

Keep mutable tags documented as last-writer-wins and retain the engine's
generation metadata, read-back checks, `ErrMutableTagMoved`, and fail-closed
handling of ambiguous writes. Do not add a default `If-Match` path: if a registry
ignores the condition, it could weaken the current behavior without the caller
knowing.

A future registry-specific opt-in would need to identify the registry
capability, obtain a validator, and prove it is enforced—for example, a
deliberately stale precondition must fail without changing the tag. It must not
be described as portable CAS. No such implementation is justified by the
current evidence; the issue's outcome is **OCI does not provide stronger
portable semantics, so keep the current contract**.

## References and limits

- [OCI Distribution Specification v1.1.1: pushing manifests](https://github.com/opencontainers/distribution-spec/blob/v1.1.1/spec.md#pushing-manifests)
- [OCI Distribution Specification v1.1.1: referrers-tag fallback](https://github.com/opencontainers/distribution-spec/blob/v1.1.1/spec.md#referrers-tag-schema)
- [OCI conformance tests v1.1.1](https://github.com/opencontainers/distribution-spec/tree/v1.1.1/conformance)
- [ORAS Go v3.0.0 `Repository.Tag`](https://github.com/oras-project/oras-go/blob/v3.0.0/registry/remote/repository.go#L620-L624) and [manifest store `Tag`](https://github.com/oras-project/oras-go/blob/v3.0.0/registry/remote/repository.go#L1938-L1955)
- [Zot v2.1.23 release](https://github.com/project-zot/zot/releases/tag/v2.1.23)
- [Distribution v3.1.1 image](https://hub.docker.com/layers/library/registry/3.1.1/images/sha256-0f6d3aa6f8c01d91b146b9b196758c99e849bff91b13a73d60b6af5119d385cd)
- [RFC 9110: `If-Match`](https://www.rfc-editor.org/rfc/rfc9110.html#name-if-match)
- [GHCR container-registry documentation](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry)

GHCR writes and Harbor runtime behavior were not tested. Zot and Distribution
results are limited to the tested versions and local setups; their full logs
were not retained. This investigation does not cover other registries or
multi-replica behavior.
