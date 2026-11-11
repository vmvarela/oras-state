# Tag deletion (`Untag`) investigation

Status: **closed — tag deletion is not usable.** Investigated 2026-11-11 against
GHCR, Zot 2.1.0 and Harbor 2.15.4, using `oras-go/v3 v3.0.0`.

## Question

`oras-state` unlocks by deleting the lock manifest, and on GHCR — which rejects
manifest deletion — by retagging `locked-*` to an `unlocked-*` marker. That
leaves the previous lock manifest as an untagged package version, i.e. garbage.

Could we delete the lock *tag* instead? `oras-go/v3` exposes
`Repository.Untag(ctx, reference)`, which issues
`DELETE /v2/<name>/manifests/<tag>` (OCI distribution spec v1.1.1, "Deleting
tags"). The spec makes it optional and, critically, **does not say what happens
to the manifest or to sibling tags that point at the same digest**: it only
guarantees that `GET .../manifests/<tag>` returns 404 afterwards.

## Method

Synthetic manifests, no real state. Per registry: push `m1` tagged `t-a` and
`t-b`, `m2` tagged `t-c`, and `m3` by digest with no tag; then delete and untag,
re-reading tags and digest bytes after every step and verifying the digest of
the served bytes.

- GHCR: disposable package with a token holding `delete:packages`.
- Zot: `ghcr.io/project-zot/zot-linux-arm64:v2.1.0`, plain HTTP.
- Harbor: `goharbor/registry-photon:v2.15.4` with `storage.delete.enabled: true`
  (the registry component, without Harbor's token front end).

## Results

| Registry | `Delete(digest)` | `Untag(tag)` | Effect |
| --- | --- | --- | --- |
| Zot 2.1.0 | OK | **OK** | Tag only: manifest and sibling aliases survive and still verify |
| Harbor 2.15.4 | OK (202) | **400 `DIGEST_INVALID`** | No effect; the endpoint requires a digest |
| GHCR | **405 `UNSUPPORTED`** | **405 `UNSUPPORTED`** | No effect. One run removed the tag anyway; a repeat did nothing at all |

GHCR details that matter for any cleanup design:

- A manifest pushed by digest appears as a package version with `tags: []`.
- Deleting a package version through the GitHub Packages API
  (`DELETE /versions/{id}`) returned 204 for an untagged version and for a
  tagged version that was not the last tagged one — it removes the version and
  all of its tags.
- Deleting the **last tagged version** returned 400 with the message
  "Publicly visible package versions with more than 5000 downloads cannot be
  deleted". That text is misleading — the real rule is that a package must keep
  at least one tagged version. So a package can never be emptied; only deleted
  as a whole.
- The Packages API reported duplicate aliases (`t-a`, `t-a`, `t-b`) after a
  retag, so its tag metadata is not a source of truth. Tag lists and digests
  must be revalidated over the OCI API.

## Conclusions

1. **Do not use `Untag`.** It is unsupported or a no-op on two of the three
   registries, and on GHCR its behaviour is non-deterministic, so no retry or
   fallback can depend on its status code. It is also not needed: on Zot, where
   it works, it leaves the manifest behind, which is exactly the garbage.
2. **The unlock path stays as it is.** `clearLock` deletes the manifest and
   falls back to `retagToUnlocked` on 405. There is no smaller correct
   alternative, so no refactor of `Load`/`Lock`/`Unlock` is justified.
3. **Garbage removal on GHCR has to be an explicit, manual operation** over the
   Packages API: delete versions that no live tag references, and excess
   history versions, while always keeping at least one tagged version. It needs
   `delete:packages` plus package admin, so it cannot run inside a normal
   `plan`/`apply`.
4. **A misleading 400 must be mapped** to a clear "GHCR keeps the last tagged
   version" error instead of surfacing GitHub's download-count text.

## Not covered

Harbor with its token front end (the results come from its registry component),
multi-node Harbor, and GitHub Enterprise registries.
