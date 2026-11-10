# GHCR history retention: no-go for automatic deletion

`MaxVersions > 0` publishes readable history on GHCR but does not bound it. Keep
automatic pruning disabled: deleting a GitHub package version is broader than
removing a history tag, and a live experiment showed a deletion destroying state
that a concurrent writer had just attached to the same version.

This is the investigation for [#28](https://github.com/vmvarela/oras-state/issues/28),
reviewed on 2026-11-08 against `35ee4110bd6c35062f2671f2374ff960a8bc96b7`.
It changes no engine behavior. Follow-up [#33](https://github.com/vmvarela/oras-state/issues/33)
remains blocked for product pruning.

## What deletion means

There are three different objects to distinguish:

| Object | Identity | Effect of deletion |
|---|---|---|
| OCI tag | A name pointing to a manifest | Tag-only deletion would remove one reference; the existing GHCR fallback is not tag-only deletion. |
| GHCR package version | Numeric Packages API ID; the engine matches its `name` to a manifest digest | Removes the version, including all its tag aliases. A matching history tag is not sufficient authorization to remove it. |
| State layer | Blob digest referenced by manifests | Can be shared by current state, history and other workspaces; deleting a manifest is not permission to delete its blobs. |

The [Packages REST API][packages] exposes `metadata.container.tags` as an array
and deletes by package version ID. It exposes no tag selector or documented
conditional-delete precondition for the version's tag set. The [OCI Distribution
specification][oci-delete] explicitly makes every tag pointing at a deleted
manifest unavailable. The existing [GHCR fallback](../ghcr.go) uses whole-version
deletion after the registry's unsupported manifest DELETE (HTTP 405).

For example, if an old history tag and the current-state tag both alias digest
`D`, deleting the package version for `D` loses both references. Pinning `D`
prevents accidentally deleting a *different* digest after a tag moves; it does
not protect an additional alias to `D`.

Current writers avoid that particular current/history alias: `put` in
[client.go](../client.go) prepares distinct current (`C`) and history (`H`)
manifests. A history-only annotation changes `H`'s digest. **Both still reference
the same state layer.** Repeated identical payloads and other workspaces can
share that layer too. Legacy or externally retagged manifests need not follow
the new writer protocol.

Neither the cited GitHub deletion contract nor OCI's manifest-deletion section
specifies GHCR's shared-layer garbage-collection guarantees or reclamation
timing. Distinct manifest digests therefore do not, by themselves, demonstrate
shared-layer safety. A live experiment (below) showed surviving manifests still
reading their shared blobs after a deletion; it does not establish when, or
whether, GHCR ever reclaims the bytes.

## Permissions: writing is not deletion

For a personal access token (classic), [GitHub documents][container-auth]:

- State publication: `write:packages` and write access to the package; reading
  private state/metadata requires `read:packages` (included by the write scope).
- Package-version deletion: `read:packages` **and** `delete:packages`, plus the
  applicable administrative access. GHCR granular packages require package
  admin; inherited access comes from repository admin. The organization REST
  endpoint additionally documents organization-admin permission.
- Restoration: `read:packages` and `write:packages`, with administrative access;
  it is subject to the [30-day and namespace-availability limits][delete-restore].
  Restoration is not a safety mechanism for an automatic pruner.

In Actions, the existing workflow uses `GITHUB_TOKEN` with `packages: write`.
GitHub separately requires package `admin` permission for REST deletion/restoration
and describes this capability as public preview. A successful push alone does
not establish deletion access. `packages: write` is workflow syntax;
`write:packages` is the classic PAT scope.

Permission failures must remain visible and must not make a successful state
publication look rolled back. The existing fallback retries only DELETE 404s
within a bound and preserves 401/403 failures; this is not a deletion-capability
probe. Public versions with more than 5,000 downloads also have deletion limits.

## Minimum safeguards for a future engine design

Reuse [client.go](../client.go)'s ownership/history validation and
[ghcr.go](../ghcr.go)'s authenticated, paginated Packages API transport. Adapters
must not grow a separate registry engine. The following are prerequisites, not
an implemented or approved algorithm:

1. **Verify identity and ownership.** Resolve candidates once, fetch their exact
   manifest bytes by digest, and require the recognized state artifact type,
   workspace annotation and consistent history/version markers. Preserve
   unmarked legacy artifacts, unknown formats and conflicting metadata.
2. **Protect the whole package version.** Match package owner/name, version ID
   and digest; enumerate its complete tag set, not just tags for this workspace.
   Every alias must be an eligible retired history tag. Preserve any alias to
   current state, retained history, locks/unlocked markers, another workspace or
   an unknown tag. Empty, incomplete or ambiguous metadata is not eligibility.
   Preserve manifests referenced by other artifacts as well; a tag inventory
   alone does not establish the absence of manifest/index references.
3. **Define the concurrency boundary.** Listing and rechecking aliases are not
   atomic with DELETE. A protected alias can appear between the last check and
   deletion. Require a demonstrated server-enforced condition or an enforceable
   exclusive-writer/retagger boundary; existing advisory locks and an age delay
   do not provide that boundary. Stop old writers and drain their retention
   workers before any protocol transition.
4. **Establish shared-content safety.** Demonstrate that deletion leaves current
   state, kept history and other workspaces readable byte-for-byte when their
   distinct manifests share layers/config blobs. Never issue blob deletion as
   part of logical history retention.
5. **Fail without expanding the target.** Preserve candidates on uncertainty,
   incomplete pagination, permission errors or API/registry disagreement. Pin
   the selected version ID/digest across retries; never substitute a newly
   resolved mutable tag. Recheck protected state after the operation to detect
   regressions, without mistaking that check for prevention.

Today `findVersionIDWithTag` returns an ID after matching one tag and digest;
it does not reject other aliases. `groupVersionsByDigest` groups this workspace's
version tags, not a package-wide protection set. Neither helper supplies the
missing concurrent-deletion boundary. Simply removing the GHCR guard would
therefore not implement these safeguards.

## Bounded history is not bounded storage

`ListVersions` counts logical history tags. Even a future safe pruner could only
target `MaxVersions` on eligible history; preserved ambiguous artifacts and
failed cleanup can exceed that target. Slicing the reader's result to the last
N entries would hide history, not prune it.

Overwriting current-state and lock tags can leave untagged package versions.
Failed or partial publications can leave prepared manifests too. An empty tag
list is not proof of garbage: content may be in flight or referenced by another
manifest. Sweeping untagged state/lock versions is separate garbage collection
and outside #28. Logical deletion also says nothing about when GHCR reclaims
physical bytes; do not promise a storage quota or savings from `MaxVersions`.

## Live deletion experiment (2026-11-08)

An explicitly authorized disposable package,
`oras-state-retention-probe-2937c7107511dcec6dfbbdbe` under the `vmvarela`
account, was published with synthetic payloads and then deleted. The package
itself was removed at the end of the run (`DELETE` 204, followed by an
authenticated `GET` 404); the throwaway test runner was not committed.

Package version IDs and complete tag sets were snapshotted through the Packages
API before and after each operation. Every survivor read resolved the tag **and**
fetched the digest it pointed at, then compared the decoded state byte for byte.

| Check | Result |
|---|---|
| Deleting one version removes every alias | A version carrying three tags (one history tag and two synthetic unknown tags) lost all three tags and its manifest digest after a single `DELETE` 204. |
| Shared payload blobs survive | Eight survivor manifests across both compression modes — current state, retained history, and another workspace's current and history — stayed readable byte-for-byte by tag and by digest, and their config and payload blobs re-fetched with matching digest and size, including 30 seconds after the deletion. |
| A protected alias added after enumeration is destroyed | A version holding only a history tag was enumerated; the current-state tag was then pointed at that same version ID, confirmed by a second snapshot. Deleting the ID returned 204 and left both tags and the digest at 404; `Get` then returned `nil` state instead of an error. |
| Tag moves leave untagged versions | Repeated overwrites left versions whose tag list was empty, which the API still reports. |

The shared-blob result removes one of the two blockers: GHCR did not reclaim
blobs still referenced by surviving manifests, in either the plain or the gzip
layout. The alias race went the other way — it is no longer hypothetical. Under
the current design a pruner can enumerate a version, an ordinary writer can then
point protected state at it, and the delete removes that state while other
workspaces keep reading normally.

Limits: this is one snapshot on one package with synthetic data. It does not
predict future garbage-collection behaviour, reclamation timing, or behaviour
under retention pressure, and the concurrency scenario was reproduced
deliberately rather than observed in production.

## Evidence and the gate to reconsider

| Claim | Evidence | Limit |
|---|---|---|
| Current/history manifests differ but share the state layer | `TestPut_CurrentAndHistoryManifestsDistinct` in [recovery_test.go](../recovery_test.go), compressed and uncompressed | Local fake registry, not GHCR garbage collection |
| GHCR writes publish history without automatic delete or cleanup retagging | `TestGHCR_AutomaticRetentionDisabledHistoryReadable` in [recovery_test.go](../recovery_test.go) | Confirms the guard, not that pruning is safe |
| API fallback pins a matching digest and handles pagination/statuses | [ghcr_http_test.go](../ghcr_http_test.go) | HTTP fixtures do not establish real alias or layer deletion semantics |
| Live suite preserves all three histories with `MaxVersions: 2` | `TestGHCRIntegration_HistoryAndCompression` in [ghcr_integration_test.go](../ghcr_integration_test.go) | Existing opt-in coverage; not a pruning test |
| Deletion is version-wide and needs stronger access | [REST contract][packages], [package deletion guide][delete-restore] and local fallback | No documented atomic tag-set condition or shared-layer GC contract |
| Deletion removes every alias of a version | Live experiment, 2026-11-08 | One disposable package, synthetic data |
| Surviving manifests keep shared plain/gzip blobs readable | Live experiment, 2026-11-08 | Snapshot only; says nothing about reclamation timing |
| A protected alias attached after enumeration is destroyed | Live experiment, 2026-11-08 | Reproduced deliberately, not sampled |

The 2026-11-08 experiment established version-wide deletion, shared-blob
survival in that snapshot, and the destructive late-alias race. The
2026-11-10 follow-up in this document verified the Packages API tag metadata and
showed that alias moves do not update `updated_at`; it also deleted only reviewed
synthetic versions that remained untagged. Together these observations do not
provide an atomic deletion condition or an exclusive-writer boundary.

Any future proposal that changes the deletion mechanism still needs evidence for
its specific safety claims: package-version IDs and complete tag sets, manifest
and payload digests, multi-tag behavior, shared layers, unknown/legacy aliases,
and permission/pagination failures. Read survivors by tag and digest after
propagation. Do not repeat the destructive late-alias DELETE merely to reconfirm
the already-observed data loss. A passing snapshot experiment alone cannot prove
future GC behavior or eliminate the alias race.

Reconsider product pruning only with evidence for shared-layer preservation
**and** a server-enforced precondition or genuinely enforceable package-wide
writer/retagger boundary; neither C's workspace witness, rechecks nor an age
cutoff supplies that boundary. Issue #33 remains **no-go** until then: retain
the automatic-pruning guard and the [README warning](../README.md#read-only-state-history).

## Follow-up disposable-package experiment (2026-11-10)

This authorized experiment checked the real Packages API metadata and the
eligibility/deletion mechanics using only synthetic versions. It did not repeat
the destructive late-alias deletion. The private package
`ghcr.io/vmvarela/gc-experiment` and its tagged anchor were left in place.

| Version ID | Digest | `created_at` / initial `updated_at` | Outcome |
|---|---|---|---|
| `1535333136` | `sha256:780abb9de17c4c4bdcc63b33e4ac3e5d7b5effe7f0eea66212873a1f0b35e3a5` | anchor | Preserved; final package version. |
| `1535336780` | `sha256:9db8f9e8a2b246e173632e97336c88caa77b299ac8926ae09fedb45efa322c02` | `2026-11-10T11:24:16Z` | Synthetic; deleted after fresh complete inventory showed present tag metadata and `tags=[]`. |
| `1535337032` | `sha256:678436a991950765b14d1925edd051b86421fb495f7da8deedab1c91f778c2bc` | `2026-11-10T11:24:19Z` | Synthetic; deleted after fresh complete inventory showed present tag metadata and `tags=[]`. |
| `1535337265` | `sha256:9b846d49eea24610db10034c4aa2b5fa8a3262993831b91eef16336201469e49` | `2026-11-10T11:24:22Z` | Synthetic; initially tagged, then untagged and alias-race-tested non-destructively; deleted only after alias moved to anchor and a fresh inventory showed `tags=[]`. |

The `metadata.container.tags` field was present as an array in the inspected
snapshots. Adding and moving aliases did **not** change `updated_at`, so an age
cutoff cannot detect a recent retag if the alias is subsequently moved away.
The non-destructive race check added `late-alias` after enumeration, observed
the candidate become tagged, then moved that alias to the anchor; the candidate
became untagged again and the alias resolved to the anchor digest. The
2026-11-08 destructive result above remains the evidence for what a late tagged
DELETE does; it was not repeated.

Each DELETE targeted one reviewed synthetic package-version ID. The candidate
was re-enumerated immediately beforehand and checked against the generated
ID/digest allowlist, complete tag metadata, empty tag list and experimental age
cutoff. All three IDs disappeared; after 30 seconds the anchor was still the
only package version, its payload hash remained
`a7b986b73b7460c3bc2157401791cc816ade499e8961e25da05fdba2ecc4725a`, and its
tags (`anchor`, `candidate-2`, `candidate-3`, `candidate-4`, `late-alias`) all
resolved to its digest. The package remains private. No tagged version or
package was deleted.

This confirms the observed response fields and that individually deleting
reviewed untagged synthetic versions leaves the anchor readable in this run.
It does **not** make production pruning safe: tag-set checks still race with
DELETE, `updated_at` does not change on retag, and an untagged manifest may have
other references. The product prune API/CLI remains blocked; the workspace
marker described in the code change is only a prospective recovery witness, not
a deletion fence.

[packages]: https://docs.github.com/en/rest/packages/packages#delete-package-version-for-an-organization
[container-auth]: https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry#authenticating-to-the-container-registry
[delete-restore]: https://docs.github.com/en/packages/learn-github-packages/deleting-and-restoring-a-package
[oci-delete]: https://github.com/opencontainers/distribution-spec/blob/v1.1.1/spec.md#deleting-manifests
