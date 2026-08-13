# Hosted approval v2

OpenCode exposes hosted approval only when standard server Basic authentication
is configured. The capability, observe, and conditional reply endpoints use the
same Authorization and workspace routing middleware as `/permission`. Without a
server password all three return 404; bad credentials return 401.

The protocol identifier is `agent-teams-hosted-approval-v2`. One process-wide
coordinator owns a random `runtime_instance_<32 lowercase hex>` and a random
`config_generation_<32 lowercase hex>`. The generation changes after successful
config writes and after an ordinary `always` reply changes approval policy.
Credentials are outside this generation and must be bound separately by clients.

Pending requests are observed per session at
`GET /experimental/agent-teams/hosted-approval/session/:sessionID/permissions`.
Each insertion receives a random `request_incarnation_<32 lowercase hex>`.
Responses are all-or-nothing, limited to 256 requests and 1 MiB JSON.

Conditional replies use
`POST /experimental/agent-teams/hosted-approval/session/:sessionID/permission/:requestID/reply`.
Bodies are limited to 16 KiB, fatal UTF-8 decoded, checked for duplicate JSON
keys, and exact-schema decoded. Runtime/config mismatches return 412 with no
effect. Missing, replayed, wrong-session, wrong-incarnation, or digest mismatches
return 409 with no effect. Successful replies settle exactly one request; reject
never cascades and `always` is not supported.

Permission digests are lowercase SHA-256 over UTF-8 canonical JSON. Object keys
are recursively sorted by JavaScript UTF-16 order, arrays retain order, and
JSON primitives use `JSON.stringify`. Golden vectors:

- `{"requestID":"permission_1","sessionID":"session_1","tool":"bash"}` -> `bf6bdf3651bc31505a7430699c5b3e55b6c51c787cd1ab27cc2017736fa679b2`
- `{"always":[],"id":"per_1","metadata":{"cwd":"/tmp","risk":1},"patterns":["ls","pwd"],"permission":"bash","sessionID":"ses_1"}` -> `04c8063915eb1c84563b998516a670e59885c0fc453318e7ca8545a94b8accca`
