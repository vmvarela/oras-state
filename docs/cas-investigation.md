# CAS and conditional-write investigation (issue #11)

Outcome: **4 — OCI does not define a portable conditional-update mechanism; the current last-writer-wins/fail-closed model remains the correct contract.** No implementation is added.

## Summary

OCI mutable tags are pointers that can be replaced, and the Distribution Specification defines no mandatory precondition for replacing them. Without a registry-specific conditional-write extension, the effective behavior is last-writer-wins. ORAS does not expose a first-class conditional tag API, although a caller can supply a custom HTTP client/transport. `oras-state` therefore continues to treat mutable tags as last-writer-wins and relies on generation-aware metadata, read-back verification, bounded observation after ambiguous writes, and fail-closed handling (`ErrMutableTagMoved`, `PartialPublicationError`).

## Specification (OCI Distribution Spec v1.1.1)

- Manifest publication is `PUT /v2/<name>/manifests/<tag-or-digest>` (`end-7a`). The spec defines success as `201 Created` and requires the registry to store the manifest byte-for-byte. It does **not** require or define `If-Match`, `If-None-Match`, or ETag handling for this endpoint.
- Tags are described as mutable pointers: "A manifest digest may have zero, one, or many tags referencing it." The push contract defines no version or ETag precondition for moving a tag.
- The spec mentions conditional requests only as optional client behavior in the referrers section: "The client MAY use conditional HTTP requests to prevent overwriting a referrers list that has changed since it was first pulled" and "Clients MAY use a conditional HTTP push for registries that support ETag conditions to avoid conflicts with other clients." Those examples concern referrers-list maintenance, but the spec does **not** restrict conditional requests to referrers and does **not** define portable manifest/tag preconditions.
- The spec defers otherwise-unspecified HTTP behavior to RFC 9110, but registries are not required to implement conditional requests for manifest pushes.

Conclusion: the specification does **not guarantee** CAS or conditional writes for mutable tags. It also does not guarantee last-writer-wins as a semantic: that is the engine's baseline behavior absent a conditional-write extension.

## ORAS capabilities (oras-go v3)

- `Repository.Tag` / `manifestStore.Tag` perform a plain manifest `PUT` under the tag. There is no `If-Match`/ETag parameter, no precondition option, and no CAS helper in the public API used by this library.
- `Resolve` returns a descriptor and digest but no ETag.
- A conditional-write attempt would require custom HTTP-client/transport integration rather than a first-class ORAS call. That is possible, but the registry would still have to implement the precondition, and no portable contract exists.

## Registry behavior

### Zot (tested)

Tested with Zot v2.1.0 (`ghcr.io/project-zot/zot-linux-amd64:v2.1.0`) over plain HTTP, repository `testrepo`, tag `test`, using two distinct replacement manifests.

Observed sequence:

1. `PUT` manifest to `test` → `201 Created`, `Docker-Content-Digest: sha256:1743...`, no `ETag`.
2. `GET /v2/testrepo/manifests/test` → `200 OK`, `Docker-Content-Digest: sha256:1743...`, no `ETag`.
3. `PUT` a second manifest to `test` with `If-Match: "sha256:1743..."` → `201 Created`; the tag was overwritten with the new digest.
4. `PUT` the first manifest to `test` with `If-Match: "sha256:0000..."` (wrong digest) → `201 Created`; the tag was overwritten again.

Observation: Zot ignores `If-Match` and provides no ETag. Its effective mutable-tag behavior is last-writer-wins.

Reproduction shape:

```sh
curl -i -X PUT "http://localhost:5000/v2/testrepo/manifests/test" \
  -H "Content-Type: application/vnd.oci.image.manifest.v1+json" \
  --data-binary @manifest1.json

curl -i "http://localhost:5000/v2/testrepo/manifests/test"

curl -i -X PUT "http://localhost:5000/v2/testrepo/manifests/test" \
  -H "Content-Type: application/vnd.oci.image.manifest.v1+json" \
  -H 'If-Match: "sha256:1743..."' \
  --data-binary @manifest2.json
```

### GHCR (documented / conditional support unverified)

- No `packages:write` token was available in this investigation, so no write experiment was performed.
- The existing GHCR compatibility suite documents sequential overwrites and lock contention, which establishes unconditional overwrite behavior, not whether GHCR supports conditional writes.
- GHCR rejects manifest `DELETE` (HTTP 405) and requires the Packages API fallback; automatic pruning is skipped.
- GHCR has no documented conditional manifest-push contract. Its Packages REST API manages package versions and deletion; it does not expose an atomic "move tag if current digest is X" operation. Conditional support remains **unverified**, not disproved.

### Harbor (documented / conditional support unverified)

- No Harbor instance was available in this investigation.
- Compatibility docs state tags are overwritable by default and immutability is opt-in per project; server-side retention rules are independent of the client.
- Harbor's default mutability establishes unconditional overwrite behavior, not whether it supports conditional writes. Conditional support remains **unverified**, not disproved. Immutability rules would break the engine's overwrite model if enabled.

## Digest-addressed writes and create-if-absent

- Pushing by digest (`PUT /v2/<name>/manifests/<digest>`) protects content identity: the manifest is addressed by its digest and cannot be confused with different bytes. It does **not** make a mutable-tag transition atomic; tagging the digest is still an unconditional tag update unless the registry supports a precondition.
- `If-None-Match: *` (create-if-absent) is distinct from `If-Match` (compare-and-swap). It does not require a returned ETag and could in principle implement a create-only lock tag. It was **not tested** in this investigation and is not required by the spec for manifest pushes.

## Spec guarantees vs. observed behavior

| Claim | Source | Status |
|---|---|---|
| No mandatory CAS/`If-Match` for manifest push | OCI Distribution Spec v1.1.1, `end-7a` | Specification guarantee (absence of a requirement) |
| Conditional requests only appear as optional referrers behavior | OCI Distribution Spec v1.1.1, referrers section | Specification text; not a restriction to referrers |
| Effective mutable-tag behavior is last-writer-wins | Engine baseline + compatibility evidence | Observed/implementation behavior, not a spec guarantee |
| Zot ignores `If-Match` and returns no ETag | Live test (Zot v2.1.0) | Observed implementation behavior |
| ORAS has no first-class conditional tag API | oras-go v3 source (`manifestStore.Tag`) | Observed library behavior |
| GHCR/Harbor conditional-write support | Documentation review only | Unverified |

## Recommendation

1. Keep the current contract: mutable tags are last-writer-wins; no strict distributed CAS.
2. Keep `ErrMutableTagMoved`, `PartialPublicationError`, generation-aware lock metadata, read-back verification, and bounded observation/fail-closed behavior.
3. Do not add an implementation based on `If-Match`/ETag: it would be registry-specific, untestable portably, and could silently weaken fail-closed behavior on registries that ignore the precondition.
4. If a future need arises, any stronger mechanism must be an explicitly documented, opt-in, registry-specific extension with live validation on each supported registry; it cannot be a default portable guarantee.
5. README/API concurrency claims are already accurate and require no change.

## Limits of this investigation

- Zot tested as v2.1.0 over plain HTTP only; newer versions and TLS paths were not exercised.
- GHCR and Harbor were not live-tested for conditional writes; their support is unverified, not disproved.
- `If-None-Match: *` create-if-absent behavior was not tested.
- No other registries (e.g., Artifactory, ECR, ACR) were evaluated.
- This document records findings only; it does not change code or tests.
