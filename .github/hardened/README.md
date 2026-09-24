# Hardened OpenCode CLI prerelease

This directory freezes the reviewed patch used by the isolated hardened release
workflow. The artifact is upstream base commit
`3104c1428ec91f809e5ab86631300de41eb6952e` (upstream `v1.18.30`) plus the exact
`packages/` projection from the pinned PR3 source commit
`58d5ae38b582156627e13543cffc58ec4150d4b9`. The patch is byte-for-byte the
canonical `packages/` diff between those commits. The source commit tree is
`48a34d8a65dd3cefb2834712dc56d800275e5b3b`, its `packages/` subtree is
`61ca5eda3f50665ef6080a575da37e53a6c9daed`, and applying the patch produces the
frozen artifact tree `d61e3dfc0b0f1c1f7a1e9f396fe27a339264e588`. PR3's `.github/`
runner changes are deliberately excluded from the release source artifact. Bun
`1.4.0` is separately pinned by the workflow and is not taken from the projected
PR3 tree.

The workflow intentionally produces a draft prerelease that is never production
eligible. It does not replace or modify the upstream publish workflow. Release
mutation occurs only after identity, reproducibility, native execution, manifest,
and provenance gates pass. Before any release-job mutation, the workflow queries
the live `hardened-release` GitHub environment and fails closed unless it has at
least one required reviewer, prevents self-review when that setting is returned
by the API, disables administrator bypass, and has exactly one custom deployment
branch policy: the `hardened-release` branch. The dispatched workflow ref must be
exactly `refs/heads/hardened-release`. The workflow does not configure these
external protections; dispatches remain blocked or fail closed until repository
administrators configure them.

The workflow atomically creates its lightweight tag at the immutable source
commit through the Git refs API. Any existing tag causes the API create to fail;
the workflow never adopts, moves, or deletes an existing tag. It resolves the
new tag back to the exact source commit before invoking
`gh release create --verify-tag`, and it never updates an existing release.

If release creation fails after the workflow created the tag, do not blindly
rerun: the retained tag deliberately makes every rerun fail closed. First verify
that no release exists for the tag and that the lightweight tag still resolves
to the documented source commit. A repository administrator may then delete
only that verified, workflow-owned tag as an explicit recovery action before a
fresh dispatch. If a release exists or the ref differs, preserve both and
investigate; this workflow must not mutate either collision.

The full five-platform build and native verification matrix is CI-only. Local
validation covers the immutable patch, script tests, YAML parsing, and static
workflow policy; it is not evidence that foreign-platform binaries execute.
