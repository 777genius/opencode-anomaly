# Hardened OpenCode CLI prerelease

This directory freezes the reviewed patch used by the isolated hardened release
workflow. The patch is applied to base commit
`826d9ad46a22bef0294998e08daa3c4904fea28f`; its resulting tree must equal source
commit `94540b2cbd116bb5bcd6c9ac8f4734f3df637a2b`.

The workflow intentionally produces a draft prerelease that is never production
eligible. It does not replace or modify the upstream publish workflow. Release
mutation occurs only after identity, reproducibility, native execution, manifest,
and provenance gates pass. A protected `hardened-release` GitHub environment must
require reviewer approval.

The full five-platform build and native verification matrix is CI-only. Local
validation covers the immutable patch, script tests, YAML parsing, and static
workflow policy; it is not evidence that foreign-platform binaries execute.
