# Hardened OpenCode CLI prerelease

This directory freezes the reviewed patch used by the isolated hardened release
workflow. The patch is applied to base commit
`47b6b6f5f4f9b42d2bce7af1c4e5bf6efaf22ba7`; its resulting tree must equal source
commit `3186244c3103eb02d95a255b593847b14488b070`.

The workflow intentionally produces a draft prerelease that is never production
eligible. It does not replace or modify the upstream publish workflow. Release
mutation occurs only after identity, reproducibility, native execution, manifest,
and provenance gates pass. A protected `hardened-release` GitHub environment must
require reviewer approval.

The full five-platform build and native verification matrix is CI-only. Local
validation covers the immutable patch, script tests, YAML parsing, and static
workflow policy; it is not evidence that foreign-platform binaries execute.
