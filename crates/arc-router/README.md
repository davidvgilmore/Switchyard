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
eligible action. Only native Messages dispatch is supported. The worker applies
private append instructions and a fixed per-model thinking baseline before
encoding. Action `controls` must be empty in session mode. Stage 2 must not
change that native baseline. Switchyard retains the exact prepared body through
a process-local typed marker; this cannot be enabled through client JSON or
headers. A backend override or changed body is refused.

The server commits after a successful buffered response or native SSE
`message_stop` reaches its HTTP body consumer. It captures the actual emitted
content blocks, including tool IDs and signed thinking. Unknown stream shapes
retain unknown attribution. Errors, missing terminal events, and detected
response cancellation abort the staged operation. This measures acceptance by
the HTTP transport, not a remote client acknowledgment. Settlement failure is
logged and requires service recovery. Process crashes, lost prepare replies,
and restart recovery require an operator-managed durable service; this adapter
does not supply one. Cancellation while `/prepare` is in flight can leave a
transaction whose token never reached this process. A failed settlement can
leave an uncertain transaction. Durable exactly-once session acceptance remains
open; these synthetic tests do not establish it.

Without `session`, explicit replay mode and its required `arc_context` remain
unchanged. Session mode does not enable Chat or Responses conversion.

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

Remaining acceptance requires pinned private package weights, actual local
encoder and both heads, VSR reference decisions, matching session sequences,
all package actions (including steering), and final provider-wire comparison.
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
