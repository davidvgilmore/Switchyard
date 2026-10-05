# ARC local policy adapter

This optional crate implements Switchyard's `Algorithm` interface. It calls a
local ARC worker with the versioned policy-decision contract and returns a
native `RoutingOutcome`. Switchyard does not implement ARC tensor arithmetic.

The adapter preserves the source conversation and tools. The integrating host
must provide `Metadata.wire_format`, a complete source body in `raw_request` or
codec preservation, and JSON in `Metadata.extra_metadata["arc_context"]`:

```json
{
  "episode_id_hash": "<64 lowercase hexadecimal characters>",
  "context_epoch": "initial",
  "attribution": [],
  "selection": { "available_action_ids": ["<action ID>"] }
}
```

Use the recorded session/context identity, attribution, and selection from the
acceptance driver. Attributions name assistant messages or Responses output items. Missing
history attribution and missing arm IDs remain absent for the worker to handle.
It never infers an attribution from a model name or a previous local result.
The worker owns session revisions, context handling, and model computation.

Build the server with `--features arc-router`. Configure a runner route:

```toml
[routes.local_arc]
id = "local-arc"
type = "arc"
config = "/path/to/private/arc-bindings.json"
targets = ["local_model"]
```

The JSON file holds `endpoint`, `package_alias`, `package_sha256`, and `actions`.
Each opaque action ID maps to `target`, `request_format`, `steering_suffix`, and
`controls`. The target is a configured Switchyard target name. The endpoint must
be an explicit loopback HTTP URL ending in `/v1/rayline/arc/policy/decide`.
Redirects and HTTP proxies are disabled.

An action's `controls` is an explicit map of provider thinking fields:
`reasoning_effort`, `reasoning`, `thinking`, or `output_config`. Null removes a
field. The adapter clears prior thinking fields, applies these controls, then
re-decodes the source body so exact-format preservation uses the selected
controls. Bindings must come from the pinned action catalog and captured VSR
wire controls. Do not translate an effort or budget by guessing. Only bindings
without steering are currently supported; `steering_suffix` must be empty.
Cross-format dispatch still needs validation before parity can be claimed.

The adapter checks the returned schema, package pin, and selected action against
request eligibility. Errors propagate without a default model or fallback list.
The response is retained in outcome evidence for the acceptance driver.

## Native Messages sessions

For ordinary HTTP clients, add an optional `session` object to the bindings:

```json
{ "session": { "endpoint": "http://127.0.0.1:18900", "owner_id": "my-host" } }
```

The operator runs this local service separately. It exposes `/prepare`, `/commit`,
and `/abort`. Switchyard sends the complete original Messages body, operation
identity, normalized conversation and agent lineage, eligible action IDs, and
optional `x-switchyard-compaction-ordinal` to `/prepare`. The service owns the
private steering ledger, attribution matching, concurrency, and local ARC
execution. Missing session identity is refused. Existing Claude Code identity
headers work; other clients can send `x-switchyard-session-id` and optional
`x-switchyard-agent-id`, `x-switchyard-parent-agent-id`, and
`x-switchyard-request-id`. Compaction ordinals must be positive integers.

The prepared reply carries a session token, exact package and decision identity,
`request_format`, and final `request`. Its destination must match the selected
eligible action. Native Messages dispatch is the default; the pinned Chat opt-in
below adds a prepared Chat destination. The worker applies
private append instructions and a fixed per-model thinking baseline before
encoding. Action `controls` must be empty in session mode. Stage 2 must not
change that native baseline. Switchyard retains the exact prepared body through
a process-local typed marker; this cannot be enabled through client JSON or
headers. A backend override or changed body is refused.

The server commits after a successful buffered response or native SSE
`message_stop` reaches its HTTP body consumer. It captures the actual emitted
content blocks, including tool IDs and signed thinking. Unknown stream shapes
retain unknown attribution. Errors, missing terminal events, and detected
response cancellation abort the staged operation. The server queues requests
for the same native conversation and agent until settlement returns, including
abort cleanup. Other sessions can proceed concurrently. This process-local
queue prevents an immediate tool continuation from racing the prior commit;
it does not provide durable state or coordinate separate server processes.
This measures acceptance by
the HTTP transport, not a remote client acknowledgment. Settlement requires a
successful JSON reply whose `state` is exactly `committed` or `aborted` for the
requested operation. Missing, malformed, wrong-state, or failed replies leave
the outcome uncertain. The server then fences that native session and agent
lineage before releasing its queue: later requests receive HTTP 409 before
another prepare. This includes failed abort cleanup after invalid preparation
or a dropped response. Other native sessions can still proceed. The fence is
shared across routes within one server state so route aliases cannot bypass it;
it is process-local and is not a durable recovery mechanism. Resolve the exact
attempt through the operator-managed service and host recovery procedure; do
not treat restarting the server as evidence that an uncertain outcome is safe. Process crashes, lost prepare replies,
and restart recovery require an operator-managed durable service; this adapter
does not supply one. Cancellation while `/prepare` is in flight can leave a
transaction whose token never reached this process. A failed settlement can
leave an uncertain transaction. Durable exactly-once session acceptance remains
open; these synthetic tests do not establish it.

Without `session`, explicit replay mode and its required `arc_context` remain
unchanged. Responses conversion remains unsupported.

## Prepared Chat destinations in Messages sessions

Set `session.codec_sha256` to the operator-pinned return codec SHA-256 to allow
a prepared Chat destination. The service must bind the receipt to the same
owner, source Messages format, selected action, model and stream setting. Its
`response_codec` must be `rayline.arc.response-codec.v1`, with source
`openai_chat`, target `anthropic_messages` and the configured
`implementation_sha256`. Without that opt-in, native Messages behavior stays
unchanged. Replay mode cannot use this path.

The host sends the prepared Chat body unchanged through the registered Chat
backend and its usual authentication. Backend body overrides, omitted fields,
and separate reasoning-effort overrides are refused. The service owns the fixed
worker controls and private append. A prepared generation is dispatched once;
ordinary backend retry settings do not repeat it. The process-only receipt and
codec capability are never included in the provider body.

The literal-loopback service exposes `/codec` beside prepare and settlement.
Calls carry the receipt owner, token, configured implementation hash and an
operation. `response` translates a buffered body. Streaming uses `stream_start`,
then numbered `stream_push` calls with complete SSE frames, then `stream_finish`.
Startup may omit `frames`; push and finish must return them. Every reply must
match the configured hash. Provider and translated responses each have a 16 MiB
limit. Codec calls have a ten-second timeout.

The host holds native `message_stop` until it observes a provider finish reason,
a complete `[DONE]` frame and successful codec finish. It does not wait for
provider EOF. Exact codec-produced native JSON and events pass through the
existing response preservation boundary; generic model and tool-name rewrites
do not alter them. The existing successful-terminal settlement, disconnect abort,
same-session queue and uncertain-ACK fence apply. This authenticates and
transports the selected request; synthetic tests do not qualify model inference,
all catalog actions, or the private codec implementation itself.

## Use an installed session runtime

An ARC deployment can supply a separate session-runtime bundle. Install the
pinned bundle in a new private directory. Keep its package manifest, provider
settings and full action catalog outside this repository. The bundle contains
no encoder, heads, model weights or provider keys. Run the numerical policy
service separately.

Use unused ports and isolated runtime/server configuration when testing beside
existing agent sessions. These steps require no global client settings changes.

Set `ARC_RUNTIME` to the installation directory and the other variables below
to your private settings file, package manifest and package alias. Read the
codec identity from the installed launcher:

```sh
"$ARC_RUNTIME/arc-session" --settings "$ARC_SETTINGS" --package "$ARC_PACKAGE" \
  --package-alias "$ARC_PACKAGE_ALIAS" --describe-config
```

Copy `codec_sha256` into the bindings file's `session` object. This pin includes
provider and backend profiles. It is not just the executable hash. Start the
same launcher and settings with `--policy-endpoint
http://127.0.0.1:9012/v1/rayline/arc/policy/decide --port 9013`. Set the session
`endpoint` to `http://127.0.0.1:9013/experimental/arc/session`. Use a stable,
distinct `owner_id` for this server.

Run the ordinary server with the `arc-router` feature and its normal TOML config.
For a prepared Chat target, use an `openai_chat` client and its usual
`api_key_env` authentication. A Chat client using `forward_auth` cannot receive
native Messages ingress. Keep action `controls` empty in session mode and retain
the full catalog. An unsupported selected winner must fail, not select again.

A llama usage profile must name the actual backend revision and executable
identity. Its response model alias must match the prepared target. Early timing
counts can support incremental output only when that exact profile verifies
against final usage. Missing cache-creation fields stay absent. Other Chat
providers may need final usage before translation and may refuse unknown cache
partitions. This is not universal streaming or cache support.

The session runtime keeps state in memory. Resolve an uncertain attempt before
retrying; changing identity or restarting does not make it safe. Numeric ARC
parity, durable recovery, Responses ingress and real-provider behavior need
separate acceptance. Installation and synthetic transport checks alone do not
make the integration ready to launch.

## Design choices

A separate algorithm crate follows the existing `prefill-router` boundary and
keeps optional inference dependencies out of libsy. `Driver::call_decision`
was considered, but the existing HTTP LLM driver rejects those calls; it would
need new transport plumbing. A gateway-only shim would bypass native routing
outcomes and algorithm observability. The runner integration is behind an
optional Cargo feature, like the existing learned router.

The session lifecycle belongs at the existing server response boundary, while
request preparation stays in the optional algorithm crate. A gateway-only
implementation would bypass native routing. A generic metadata bypass for
provider transformations would be too broad, so this path uses a typed native
Messages request checked against the final encoded body. Reimplementing the
private ledger inside Rust would duplicate policy/session logic; the local
service owns it instead.

Unlike the prefill router, ARC receives the full conversation every turn and
does not add user-turn affinity or silently truncate history.

## Validation status

Tests use a synthetic loopback worker and public invented inputs. They test the
native libsy routing path and exact-format outgoing control preservation. They
do not execute an encoder, load Hugging Face weights, or establish ARC parity.

A separate private development check ran nine native Messages requests through
one resident local ARC encoder and both heads, using the ordinary installed
server. It covered a tool result, new user turns, two sessions, the next model
selection boundary, and compaction. Actual decisions controlled private steering
emission and replay. The check compared every provider request and committed
assistant response. All nine decisions selected Opus; provider responses were
synthetic. This establishes that session flow, not mixed-model routing or real
provider generation.

The native encoder's numerical parity remains unqualified. Launch acceptance
still requires VSR reference parity, broader package-action coverage, actual
provider behavior, and the remaining retry, cache-eviction, worker-loss and
recovery cases. The runtime remains in memory; this check does not establish
durable restart or native streaming.
Ordinary native Messages ingress can use the session mode above. Its HTTP
composition tests use a synthetic service and provider; they establish no
local encoder, head, or reference-decision parity. Explicit replay still needs
an embedding or acceptance driver to supply context. Keep private catalogs,
weights, fixtures, and credentials out of this repository.

Run the acceptance helper with external private files:

```sh
cargo run -p switchyard-arc-router --example decide -- /private/bindings.json /private/decision-request.json
```

It exercises libsy routing and prints the decision and outgoing source-format
request. It never calls a destination model. Redirect its output to private
scratch; it contains the supplied conversation.

## Cloud-generation integration preview

This preview runs the small ARC encoder and routing heads locally. Generation
runs at configured cloud endpoints. It does not establish numerical parity with
the reference runtime or availability of every provider in the catalog.

Use the supplied private **OPERATOR-PREVIEW.md** and pinned checkpoint
manifest for exact Hugging Face acquisition, checksum verification, installation,
export, foreground startup and shutdown commands. `ARC_INSTALL` names the
selected host installation; `ARC_SERVER_CONFIG` is its exported `switchyard.toml`. Keep the checksums, source pins, licenses and install receipts. Do not
commit credentials or model files to this repository. The installed server must
include the `arc-router` feature. A source build uses:

```sh
cargo build --locked --release -p switchyard-server --features arc-router
```

Use the installed setup helper with the exact package and provider settings to
export the server TOML and ARC bindings. Retain the full action catalog. The
numerical endpoint and session endpoint are separate loopback services. The
session calls the numerical service; Switchyard calls the session; the selected
generation target calls cloud HTTPS. Keep the session's configured codec hash
and exported settings together. Start the services using the operator kit, then
start the normal server with its exported configuration:

```sh
ARC_SERVER_CONFIG="$ARC_EXPORT/switchyard.toml"
"$ARC_INSTALL/host/bin/switchyard-server" \
  --config "$ARC_SERVER_CONFIG" --host 127.0.0.1 --port 18103
```

Use fresh configuration and state directories when running beside other clients.
Switchyard exposes a native Messages API; it does not supply a Claude launcher.
Point an isolated client's process-local base URL at this server. Keep client
context and output limits explicit for the deployment, and do not modify global
Claude or terminal settings. The preview uses cloud generation only. A local
Qwen3.8-27B example must remain a separate, unselected configuration, absent from
active targets and routes. Do not download, load or start it on this Mac, and do
not configure it as automatic fallback.

Session-mode action `controls` stay empty. The selected provider profile and
pinned session prepare the native thinking baseline; private Stage 2 text is a
separate on-change instruction. Do not infer native effort from an action name.
The prepared body must remain exact through dispatch, including tools and cache
intent. Missing provider usage remains unknown. Cache directives alone do not
prove a cache read or write, and reported cost is not a final bill.

An explicit `x-switchyard-request-id` becomes the session operation ID. Without
it, the adapter generates a new ID. The session rejects redispatch of a committed
operation and changed content under an aborted operation ID. A successful abort
acknowledgement allows a retry under the service contract. Tool continuations
wait for settlement; uncertain settlement fences that conversation and agent
with HTTP 409. Resolve the receipt before attempting recovery. Restarting does
not prove the previous attempt was safe.

For a refusal, keep the original error and receipts. Check the server feature,
exported target names, package and codec pins, loopback service readiness,
conversation identity and cloud authentication. Responses conversion, durable
restart recovery, all-provider availability and numerical parity remain outside
this preview. Synthetic transport tests and real cloud tool cycles are separate
evidence; neither should be described as full reference qualification.

The future local option names the [ggml-org published GGUF candidate](https://huggingface.co/ggml-org/Qwen3.8-27B-GGUF/tree/71bc7b627595dc8a91039addd9c791ae548d6747). This is an identity reference, not a download or launch instruction. Mac serving, quantization choice and local ARC profile qualification remain untested; keep the candidate mapping unselected.
