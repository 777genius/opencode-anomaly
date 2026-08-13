# Hardened OpenCode CLI prerelease

This directory freezes the reviewed patch used by the isolated hardened release
workflow. The patch is applied to base commit
`49c69c5ed3ccf706b61b3febb43c8aaff7f8325e`; its resulting tree must equal source
commit `1554487639c28df9eb294c93257ed52114aa24c5`.

The workflow intentionally produces a draft prerelease that is never production
eligible. It does not replace or modify the upstream publish workflow. Release
mutation occurs only after identity, reproducibility, native execution, manifest,
and provenance gates pass. A protected `hardened-release` GitHub environment must
require reviewer approval.

The full five-platform build and native verification matrix is CI-only. Local
validation covers the immutable patch, script tests, YAML parsing, and static
workflow policy; it is not evidence that foreign-platform binaries execute.
