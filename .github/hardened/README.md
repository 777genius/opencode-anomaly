# Hardened OpenCode CLI prerelease

This directory freezes the reviewed patch used by the isolated hardened release
workflow. The patch is applied to base commit
`47b6b6f5f4f9b42d2bce7af1c4e5bf6efaf22ba7`. It is byte-for-byte the `packages/`
diff to accepted PR3 source commit
`044bb4a47b6c2e4cecfb7d790f6be5f5efe3a0b1`; the commit tree is
`4873a6eda63b8a78d6cd4badb70c3a3a5532bfc6`, and applying the patch produces the
frozen artifact tree `6b0442241c0fe40c6711352952766602b1904f97`. PR3's `.github/`
runner changes are deliberately excluded from the release source artifact.

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
