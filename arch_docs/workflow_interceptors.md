# Workflow Interceptors - Rust SDK

**Status:** Draft, corrected scope, replay behavior explicit
**Goal:** Add workflow interceptors for actions that enter or exit workflow context, with an
explicit replay-aware side-effect boundary.

This is intentionally different from client interceptors. It should not cover calls like fetching
workflow history, describing a workflow from a client, or listing workflows. It should cover
workflow code boundaries: executing workflow code, handling queries/signals/updates, and outbound
workflow commands such as timers, activities, child workflows, external workflow signals/cancels,
continue-as-new, and Nexus operations.

These interceptors are instrumentation and policy hooks around workflow execution. They need to
support OpenTelemetry, but the SDK should not conflate command-affecting interception with
side-effect export. Interceptor code that transforms inputs used for workflow commands must remain
deterministic and replay-compatible; user-defined telemetry/log/metric side effects must be gated
by replay state. The first implementation should provide the interceptor traits and dispatch
machinery only, not SDK-provided OpenTelemetry interceptors or telemetry helpers.

For v1, outbound workflow interceptors should align with other SDK behavior: an interceptor may
pass the original input or a modified input to `next`, and that forwarded input is what the SDK uses
to produce commands. For history-producing outbound hooks, successful short-circuiting should not be
supported in v1; user-defined interceptors must continue the call chain exactly once.

Workflow interceptor side effects must not be emitted while replaying history events. During replay,
the SDK still executes workflow code and emits commands through the normal deterministic path; if an
interceptor participates in deterministic input transformation, that transformation must remain
replay-compatible while telemetry/log/metric export is suppressed.

---

## 1. Motivation

The existing Rust `WorkerInterceptor` operates at workflow activation boundaries:

- `on_workflow_activation`
- `on_workflow_activation_completion`
- `on_shutdown`

That is useful for coarse worker instrumentation, but it is too low-level for SDK users who want to
intercept the same conceptual operations exposed by other Temporal SDKs:

- inbound workflow calls, such as workflow execution, query handling, signal handling, update
  validation, and update handling
- outbound workflow calls made from workflow context, such as starting timers, scheduling
  activities, starting child workflows, signaling/canceling external workflows, continuing as new,
  and starting Nexus operations

These interceptors are workflow-runtime boundary interceptors. They observe and wrap workflow entry
and exit points, but they should not be modeled as client operations, raw Core RPC hooks, or a
general-purpose way to run arbitrary nondeterministic workflow code.

---

## 2. Prior Art: Ruby Shape

Ruby splits workflow interceptors into inbound and outbound objects.

Inbound methods:

- `execute`
- `handle_query`
- `handle_signal`
- `validate_update`
- `handle_update`
- `init`, which initializes/wraps the outbound interceptor for that workflow instance

Outbound methods:

- `sleep`
- `execute_activity`
- `execute_local_activity`
- `start_child_workflow`
- `signal_child_workflow`
- `signal_external_workflow`
- `cancel_external_workflow`
- `initialize_continue_as_new_error`
- `start_nexus_operation`

That split maps cleanly to Rust:

- inbound hooks sit around workflow implementation dispatch in `crates/sdk/src/workflows.rs`
- outbound hooks sit around `WorkflowContext` / `SyncWorkflowContext` methods that emit workflow
  commands in `crates/sdk/src/workflow_context.rs`

Rust does not need to copy Ruby's class hierarchy exactly, but it should preserve the distinction
between operations entering workflow code and operations workflow code sends outward.

### 2.1 Python Shape And Important Divergence

Python documents five interceptor categories:

- outbound client
- inbound workflow
- outbound workflow
- inbound activity
- outbound activity

This matches the Rust scoping decision to keep this document focused on worker-configured workflow
interceptors and not client interceptors.

Python also documents two details that should influence Rust:

- interceptors form a chain and can inspect or modify input and result data before forwarding to the
  next interceptor
- workflow inbound and outbound interceptors run in the Workflow sandbox and may execute during
  replay

Python's OpenTelemetry implementations are important because they do not simply perform arbitrary
unguarded network I/O from sandboxed workflow code:

- the older `TracingInterceptor` registers an unsafe extern function for completed workflow spans;
  workflow-side interceptor code skips most span creation during replay and calls the extern to
  create/end the span outside the sandbox
- the newer `OpenTelemetryPlugin` passes the `opentelemetry` module through the sandbox, but it
  requires a `ReplaySafeTracerProvider` and deterministic `TemporalIdGenerator`; the replay-safe
  span wrapper suppresses `end()` during `is_replaying_history_events`, which prevents duplicate
  export

The Rust lesson is not "workflow interceptors must always run outside workflow context" or "Rust
needs Python/TypeScript host-boundary machinery." Rust workflows are futures driven by the SDK in
the same process, so v1 should not introduce a sink/extern architecture just to mirror SDKs with
workflow isolation. The lesson to preserve is narrower: separate the replay-sensitive interceptor
chain from side-effect emission. In v1, Rust should expose the replay state needed for
user-defined interceptors to suppress their own side effects; SDK-provided OTEL helpers can be
added later on top of the same context.

### 2.2 TypeScript Replay Lesson

The TypeScript SDK has two related replay signals on unsafe workflow info:

- `isReplaying`: the workflow is currently replaying
- `isReplayingHistoryEvents`: the workflow is currently replaying history events

`isReplayingHistoryEvents` is narrower. It is false for live read-only operations such as query
handlers and update validators, even when those run after replay catch-up.

TypeScript's OpenTelemetry workflow interceptor creates spans in the workflow isolate. The duplicate
export prevention happens at the sink boundary:

- workflow sink calls carry the `WorkflowInfo` captured at call time
- worker sink processing drops calls when `callDuringReplay` is false and
  `workflowInfo.unsafe.isReplayingHistoryEvents` is true
- replay workers suppress all non-`callDuringReplay` sinks, even if Core sends a final non-replay
  activation while replaying a history
- the OpenTelemetry workflow span exporter sink does not set `callDuringReplay`, so replay-created
  spans are not exported

Rust should encode the same user-visible outcome without copying TypeScript's sink mechanism or
Python's extern mechanism. Because Rust wants OTEL-capable interceptors without duplicate replay
emission, the SDK needs explicit raw and computed replay state on interceptor inputs. User-defined
interceptors can suppress telemetry/log/metric emission without skipping deterministic input
transformations needed for command generation.

TypeScript, Ruby, and Java workflow outbound interceptors can affect command data when they forward
modified inputs. Rust v1 should copy this behavior for SDK parity, with one additional Rust-specific
constraint: history-producing outbound hooks must continue the call chain exactly once so command
generation remains owned by the SDK.

---

## 3. Correct Scope

### 3.1 In Scope

Inbound workflow operations:

- execute workflow run function
- handle signal
- handle query
- validate update
- handle update

Outbound workflow operations:

- start timer / sleep
- execute activity
- execute local activity
- start child workflow
- signal child workflow
- signal external workflow
- cancel external workflow
- initialize continue-as-new
- start Nexus operation

Likely later workflow-context operations:

- upsert search attributes
- upsert memo
- patch markers
- set current details
- force workflow task failure
- cancellation observation hooks, if a concrete use case appears

### 3.2 Out Of Scope

Client-level workflow operations:

- start workflow from a client
- signal workflow from a client
- query workflow from a client
- fetch workflow history
- describe workflow
- list/count workflows
- client-side update handles

Worker-level but not workflow-context operations:

- activity execution interceptors
- Nexus handler interceptors
- poller/slot/worker lifecycle instrumentation beyond the existing activation-level
  `WorkerInterceptor`

---

## 4. Start Narrower

Do not implement every Ruby-equivalent hook in the first PR. The first slice should prove the
architecture with the smallest useful set.

Recommended first slice:

- inbound `execute`
- inbound `handle_query`
- inbound `handle_signal`
- outbound `sleep` / timer

Why this slice:

- `execute` proves a workflow instance can be initialized with an interceptor chain.
- `handle_query` proves read-only inbound calls can be wrapped and can return a value.
- `handle_signal` proves inbound mutation-capable calls can be wrapped and can observe headers.
- `sleep` proves outbound command interception without involving payload conversion or typed
  generic output.

Recommended second slice:

- outbound `execute_activity`
- outbound `execute_local_activity`
- outbound `start_child_workflow`

These prove typed input serialization and typed result futures on outbound calls.

Recommended third slice:

- inbound `validate_update`
- inbound `handle_update`
- outbound `signal_child_workflow`
- outbound `signal_external_workflow`
- outbound `cancel_external_workflow`
- outbound `initialize_continue_as_new_error`

Do not implement:

- outbound `start_nexus_operation`
- patch/upsert/current-details APIs

---

## 5. Design Constraints

### 5.1 Execution Context And Determinism Boundary

Separate the interceptor chain from the effect-export boundary. Command-affecting hooks may need to
participate in the deterministic workflow execution/replay path so that forwarded input mutations
produce the same commands as other SDKs. Non-deterministic work such as OTEL export, log shipping,
metrics export, and external policy network calls must not run unguarded in that replay-sensitive
path.

Other SDKs solve this split differently:

- TypeScript creates spans in the workflow isolate, serializes them through a workflow sink, and
  drops non-`callDuringReplay` sink calls when `isReplayingHistoryEvents` is true.
- Python's older tracing interceptor calls an unsafe extern function to create completed spans
  outside the sandbox and avoids most replay calls.
- Python's newer OpenTelemetry plugin allows OTel objects in the sandbox but requires a replay-safe
  tracer provider and deterministic ID generator; span end/export is suppressed during replay.

Rust does not need those isolation-boundary mechanisms for v1. The command-side interceptor can run
inline with the Rust workflow future because the SDK directly drives workflow futures. The simplest
v1 model is likely:

- let command-transforming interceptor code run wherever the SDK currently builds workflow
  commands, including replay when needed
- expose a read-only workflow/operation context on interceptor inputs, including raw and computed
  replay state
- require user-defined interceptors to use that replay state to suppress their own side effects
- defer SDK-provided OTEL/log/metric helpers to a later PR
- do not document arbitrary network I/O as safe from the replay-sensitive part of a workflow
  interceptor

Allowed interceptor behavior:

- create and end tracing spans
- export telemetry or logs only when guarded by replay state
- read wall-clock time for instrumentation
- enqueue or hand off network I/O for observability or policy systems only when guarded by replay
  state, and only if the result cannot affect workflow commands or handler results
- inspect workflow metadata, replay state, headers, payloads, and outbound command inputs

Required constraints:

- interceptor side effects must not affect deterministic workflow behavior unless the resulting
  command/result is derived only from deterministic workflow inputs
- history-producing outbound hooks must call `next` exactly once
- any input changes forwarded to `next` must be deterministic and replay-safe
- interceptor inputs must expose only read-only workflow context; do not pass command-capable
  `WorkflowContext<W>` or `SyncWorkflowContext<W>` to interceptors

In practice, an interceptor may record that a timer was scheduled or emit a span for an activity
call when `is_replaying_history_events` is false. It must not call an external service and use that
response to decide which timer duration, activity args, query result, or update result the workflow
sees. It may, however, forward modified headers, arguments, or options to `next` when those
modifications are deterministic from workflow history/input data.

Because command-side hooks may run while the SDK is polling workflow futures, v1 should not promise
that awaiting arbitrary external I/O inside an interceptor hook is safe. Interceptors that need
network I/O should hand work off to worker-owned state, such as a non-workflow task, channel, or
span processor, and must not wait for that result to continue workflow execution. For the eventual
OTEL case, the interceptor should create/end or enqueue the span and continue; the span processor
or exporter is responsible for persistence I/O.

Replay behavior also needs explicit handling. See the next section; replay suppression should be
based on replay state carried in the read-only interceptor context.

### 5.2 Replay Behavior

Workflow interceptors may contain side-effecting instrumentation code, but side effects must be
behind a replay-aware boundary. The SDK must avoid duplicate side effects while replaying history
events. However, because outbound interceptors may forward modified inputs that affect command
generation, replay handling cannot simply bypass the whole interceptor chain for history-producing
hooks without changing replayed command data.

Implementation rule:

- capture raw activation replay state at the SDK dispatch/context boundary from Core activation
  metadata
- expose both raw `is_replaying` and SDK-computed `is_replaying_history_events` through the
  read-only interceptor context carried by each input
- compute `is_replaying_history_events` at each operation hook from raw replay state plus that
  operation's semantics, so live-only operations are not incorrectly suppressed
- query handlers should always receive `is_replaying_history_events = false`, because queries are
  not replayed history events
- for history-producing hooks, preserve deterministic input transformation during replay; this
  usually means invoking the chain during replay and requiring the chain to call `next` exactly once
- custom interceptors that perform side effects must check `is_replaying_history_events`
- resume normal side-effect emission once replay catch-up reaches new, non-replay work

This is especially important after workflow cache eviction. A workflow may replay earlier history to
rebuild deterministic state, but OTEL spans, logs, metrics, and external policy calls for those
already-observed logical operations must not be emitted a second time.

This replay rule constrains what v1 interceptors can safely do. If an interceptor forwards modified
input to `next`, that transformation must be deterministic and must run consistently during replay.
Side effects in the same interceptor must be guarded by replay state.

### 5.3 Continuation And Mutation Semantics

Rust v1 should model other SDK APIs where `next(input)` consumes the input forwarded by the
interceptor. This means input mutation is represented by passing a modified input value to `next`,
not by mutating shared SDK state in place.

Rust v1 should use these semantics:

- input structs describe SDK operations and may be transformed before forwarding
- `Next` accepts the input value to continue with
- history-producing outbound hooks must call `next` exactly once
- successful short-circuiting of history-producing outbound hooks is not supported in v1
- nested collections should avoid shared mutable aliasing; use owned values or copy-on-write
  `with_*` APIs so replacement is explicit
- input transformations that affect commands must be deterministic and must not depend on
  non-replayable side effects

### 5.4 Typed vs. Serialized Inputs

Inbound handlers already operate at points where the SDK dispatch layer has access to handler names,
payloads, headers, and a workflow context. Outbound workflow calls often start with typed Rust
inputs and serialize them inside `WorkflowContext` methods.

For a first implementation, prefer serialized payloads at interceptor boundaries:

- inbound inputs contain handler name, payloads, headers, and context/view
- outbound inputs contain operation name/type, serialized payloads, options, and command metadata

This keeps the interceptor traits object-safe and avoids generic methods over activity/workflow
definitions. If a later design requires typed visibility, it should be a separate generic/static
extension, not the base interceptor API.

### 5.5 Input Struct Stability

Input structs are public because they appear in public interceptor method signatures, but they are
not intended as a stable construction API.

Document this explicitly:

> Workflow interceptor input structs may change in backwards-incompatible ways. Interceptors should
> inspect or transform inputs received from the SDK; users should not construct input structs
> themselves.

Implementation guidance:

- mark input structs `#[non_exhaustive]`
- use crate-private constructors/builders for SDK construction
- provide `with_*` methods or builders for intended input replacement
- avoid shared mutable aliasing for args, headers, options, and other history-producing data
- provide output constructors only when successful short-circuiting is supported and replay-safe

### 5.6 Async And `Send`

Do not couple command-side interceptor futures to OTEL exporter futures. If the chain is invoked
while the SDK is interacting with workflow-local `Rc`/`RefCell` state, the command-side public
future may need to be non-`Send`. Future side-effect/export helpers can still hand work to normal
worker-runtime tasks and `Send` futures without changing the command-side trait shape.

The first implementation should decide this based on where the chain is invoked:

- If hooks run while borrowing workflow-local internals, use non-`Send` command-side futures.
- If hooks can be invoked on the normal worker runtime without borrowing workflow-local internals,
  `Send` futures may be acceptable, but this should be proven by the implementation hook points.

Default assumption: do not require `Send` for command-side workflow interceptor futures until the
hook points prove that requirement is sound.

---

## 6. Proposed Shape

Use separate inbound and outbound traits. A workflow interceptor factory wires them together for a
workflow instance. Individual operations should use an explicit `Next` value so ordering and
lifetime constraints are clear.

`Next` should be public enough to appear in interceptor method signatures, but opaque enough that
users cannot construct it themselves. `Next` accepts the input to continue with, matching other SDK
interceptor APIs.

Sketch:

```rust
pub type WorkflowExecuteOutput = LocalBoxFuture<'static, WorkflowResult<Payload>>;
pub type WorkflowSignalOutput = LocalBoxFuture<'static, Result<(), WorkflowError>>;
pub type WorkflowQueryOutput = Result<Payload, WorkflowError>;
pub type SleepOutput = BoxedCancellableFuture<TimerResult>;

#[must_use = "workflow interceptor continuations must be run to continue the call chain"]
pub struct Next<'a, I, O> {
    // private SDK-owned continuation
    inner: Box<dyn FnOnce(I) -> O + 'a>,
}

impl<'a, I, O> Next<'a, I, O> {
    #[must_use = "the returned workflow interceptor output must be used"]
    pub fn run(self, input: I) -> O {
        (self.inner)(input)
    }
}

pub trait WorkflowInterceptor: Send + Sync + 'static {
    fn intercept_workflow(&self, ctx: WorkflowInterceptorContext) -> WorkflowInterceptors;
}

pub struct WorkflowInterceptors {
    pub inbound: Box<dyn WorkflowInboundInterceptor>,
    pub outbound: Box<dyn WorkflowOutboundInterceptor>,
}

pub trait WorkflowInboundInterceptor: Send + Sync + 'static {
    fn execute<'a>(
        &'a self,
        input: ExecuteInput,
        next: Next<'a, ExecuteInput, WorkflowExecuteOutput>,
    ) -> WorkflowExecuteOutput {
        next.run(input)
    }

    fn handle_signal<'a>(
        &'a self,
        input: HandleSignalInput,
        next: Next<'a, HandleSignalInput, WorkflowSignalOutput>,
    ) -> WorkflowSignalOutput {
        next.run(input)
    }

    fn handle_query<'a>(
        &'a self,
        input: HandleQueryInput,
        next: Next<'a, HandleQueryInput, WorkflowQueryOutput>,
    ) -> WorkflowQueryOutput {
        next.run(input)
    }

    // Later slices:
    // fn validate_update<'a>(..., next: Next<'a, ValidateUpdateInput, WorkflowUpdateValidationResult>) -> ...;
    // fn handle_update<'a>(..., next: Next<'a, HandleUpdateInput, WorkflowUpdateResult>) -> ...;
}

pub trait WorkflowOutboundInterceptor: Send + Sync + 'static {
    fn sleep<'a>(
        &'a self,
        input: SleepInput,
        next: Next<'a, SleepInput, SleepOutput>,
    ) -> SleepOutput {
        next.run(input)
    }

    // Later slices:
    // fn execute_activity<'a>(..., next: Next<'a, ExecuteActivityInput, ActivityOutput>) -> ...;
    // fn execute_local_activity<'a>(..., next: Next<'a, ExecuteLocalActivityInput, ActivityOutput>) -> ...;
    // fn start_child_workflow<'a>(..., next: Next<'a, StartChildWorkflowInput, ChildWorkflowStartOutput>) -> ...;
    // fn signal_child_workflow<'a>(..., next: Next<'a, SignalChildWorkflowInput, SignalChildWorkflowOutput>) -> ...;
    // fn signal_external_workflow<'a>(..., next: Next<'a, SignalExternalWorkflowInput, SignalExternalWorkflowOutput>) -> ...;
    // fn cancel_external_workflow<'a>(..., next: Next<'a, CancelExternalWorkflowInput, CancelExternalWorkflowOutput>) -> ...;
    // fn initialize_continue_as_new_error<'a>(..., next: Next<'a, InitializeContinueAsNewErrorInput, WorkflowTermination>) -> ...;
    // fn start_nexus_operation<'a>(..., next: Next<'a, StartNexusOperationInput, NexusOperationOutput>) -> ...;
}
```

`Next` lifetime constraints:

- `Next<'a, I, O>` borrows SDK chain/dispatch state for `'a`
- it is single-use because `run` consumes `self`
- `run` returns the operation output directly; operations that are async use boxed local futures as
  their output types, while synchronous operations such as queries and timer creation can remain
  synchronous
- v1 history-producing outbound hooks must call `next` exactly once; `run` consuming `self`
  enforces at-most-once, and `#[must_use]` should catch accidental failure to continue, but the
  exact-once rule remains a documented user contract rather than a heavy runtime mechanism
- successful short-circuiting should not be documented for history-producing hooks until a
  replay-safe model exists
- it accepts input, so interceptors can forward modified command data just like other SDKs
- it must not be stored, leaked, or spawned onto a task requiring `'static`
- it must not expose mutable workflow internals; inputs should own copied/serialized data or use
  explicit replacement APIs for nested values

The SDK adapter is responsible for deciding how replay affects side effects. It must not skip a
history-producing interceptor chain if that chain may deterministically transform input before
calling `next`.

---

## 7. Input Type Sketches

Each input should carry a read-only context view. This can wrap the existing
`WorkflowContextView`, but it should also include replay/operation information that is relevant to
side-effect decisions.

Sketch:

```rust
#[non_exhaustive]
pub struct WorkflowInterceptorContext {
    pub workflow: WorkflowContextView,
    pub operation: WorkflowOperationContext,
}

#[non_exhaustive]
pub struct WorkflowOperationContext {
    /// Raw replay state for the current activation/task as observed from Core.
    pub is_replaying: bool,

    /// True when this operation is being executed only to replay history events.
    ///
    /// This is computed by the Rust SDK from `is_replaying` plus operation context. Interceptors
    /// should use this, not raw `is_replaying`, to suppress duplicate telemetry/log/metric side
    /// effects. The value should be narrow enough that live read-only work, such as queries or
    /// update validators after replay catch-up, is not incorrectly treated as replay.
    pub is_replaying_history_events: bool,
}
```

Do not expose `WorkflowContext<W>`, `SyncWorkflowContext<W>`, or command-sending methods through
this view. Interceptors should be able to inspect workflow identity and replay state, but workflow
commands should still be produced only by forwarding input to `next`.

First slice:

```rust
#[non_exhaustive]
pub struct ExecuteInput {
    workflow_type: String,
    args: Vec<Payload>,
    headers: HashMap<String, Payload>,
    context: WorkflowInterceptorContext,
}

#[non_exhaustive]
pub struct HandleSignalInput {
    signal_name: String,
    args: Vec<Payload>,
    headers: HashMap<String, Payload>,
    context: WorkflowInterceptorContext,
}

#[non_exhaustive]
pub struct HandleQueryInput {
    query_name: String,
    args: Vec<Payload>,
    headers: HashMap<String, Payload>,
    context: WorkflowInterceptorContext,
}

#[non_exhaustive]
pub struct SleepInput {
    duration: Duration,
    summary: Option<String>,
    context: WorkflowInterceptorContext,
}
```

Second slice examples:

```rust
#[non_exhaustive]
pub struct ExecuteActivityInput {
    activity_type: String,
    args: Vec<Payload>,
    options: ActivityOptions,
}

#[non_exhaustive]
pub struct StartChildWorkflowInput {
    workflow_type: String,
    workflow_id: String,
    args: Vec<Payload>,
    options: ChildWorkflowOptions,
}
```

The input structs should be designed around workflow SDK operations, not core activation protobufs.
Expose proto fields only when they are already the public SDK type or when there is no practical
wrapper yet. Provide accessor methods instead of public fields for history-producing data, plus
`with_*` replacement methods where input transformation is supported.

---

## 8. Where This Hooks In Rust Today

Inbound hook points:

- workflow run dispatch in `crates/sdk/src/workflows.rs`
- signal dispatch in `WorkflowDispatch::dispatch_signal`
- query dispatch in `WorkflowDispatch::dispatch_query`
- update validation and handling in `WorkflowDispatch::validate_update` and `start_update`

Outbound hook points:

- `BaseWorkflowContext::timer`
- `BaseWorkflowContext::start_activity`
- `BaseWorkflowContext::start_local_activity`
- `BaseWorkflowContext::child_workflow`
- `StartedChildWorkflow::signal`
- `StartedChildWorkflow::cancel`
- `ExternalWorkflowHandle::signal`
- `ExternalWorkflowHandle::cancel`
- `SyncWorkflowContext::continue_as_new` / `WorkflowContext::continue_as_new`
- `SyncWorkflowContext::start_nexus_operation`

The first implementation should not hook directly into core state machines. Hook at the SDK context
and dispatch layers where the operation still has SDK-level names, headers, payloads, and options.

---

## 9. Implementation Plan

1. Keep this PR focused on traits and dispatch machinery. Do not add SDK-provided OTEL
   interceptors, telemetry helpers, activity interceptors, or client interceptors.
2. Add `WorkflowInterceptor`, `WorkflowInboundInterceptor`, and `WorkflowOutboundInterceptor`
   scaffolding under `crates/sdk/src/interceptors.rs` or a new `crates/sdk/src/interceptors/`
   module.
3. Define public opaque `Next<'a, I, O>` and its lifetime/single-use/input-forwarding semantics.
4. Thread workflow interceptor configuration through `WorkerOptions`, not `ClientOptions`.
5. Build inbound and outbound chains when a workflow instance is created.
6. For each workflow instance, create/wrap an outbound interceptor chain and store it in worker/SDK
   state associated with that instance. Do not expose this as user workflow context state.
7. Add `WorkflowInterceptorContext` to interceptor inputs with both raw `is_replaying` and
   computed `is_replaying_history_events`. Do not skip history-producing interceptor chains during
   replay if they can transform command inputs.
8. Implement first-slice hooks: `execute`, `handle_signal`, `handle_query`, and `sleep`.
9. Add workflow/unit or integration tests:
   - interceptor ordering
   - default forwarding
   - `Next` is single-use and marked `#[must_use]`
   - error behavior where supported
   - signal/query headers visible in inbound inputs
   - timer interception observes the logical sleep operation
   - modified input forwarded to `next` affects emitted workflow commands
   - raw `is_replaying` is exposed on relevant operations
   - computed `is_replaying_history_events` is true for history replay and false for live
     read-only operations after replay catch-up
   - query interceptor inputs always have `is_replaying_history_events = false`
   - deterministic input transformations still produce replay-compatible commands
   - live query/update-validator work after replay catch-up can still invoke inbound interceptors
   - interceptor errors/panics are surfaced as failures of the intercepted operation, not worker
     crashes
10. Add activity and child-workflow outbound hooks after the first slice is stable.
11. Add updates and external workflow hooks after activity/child workflow hooks are stable.

Integration tests must be run with `cargo integ-test <test_name>`.

---

## 10. Implementation Details To Watch

- Replay state is operation-local. Each hook should set `is_replaying_history_events` according to
  the operation being intercepted. For example, query handlers should always receive false because
  queries cannot be replayed as history events.
- Interceptor methods should return the same operation result types as the wrapped SDK operation.
  Do not introduce a separate interceptor error channel in v1.
- If an interceptor returns an error where the operation result type supports errors, treat it as
  if the intercepted call returned that error.
- Panics from interceptor code should be caught and converted through the same user-code failure
  mapping as a panic in the intercepted call. They should not crash the worker process; for
  application-level operation failures, this should become an application failure, analogous to
  activity panic handling.
- Awaiting external I/O inline in command-side hooks is not a supported v1 contract. Side-effecting
  interceptors should hand work off, for example to a span processor, without waiting for the
  result to continue workflow execution.

## 11. Deferred Work

- SDK-provided OTEL/log/metric helpers or interceptors. V1 only exposes replay state so user-defined
  interceptors can make their own side-effect decisions.
- Activity interceptors and client interceptors.
- Additional workflow commands such as `upsert_search_attributes`, `upsert_memo`, patch markers,
  and current details.

---

## 12. Risks

- **Nondeterminism leaking into history:** workflow interceptors may perform non-deterministic
  side effects, but must not use those side effects to change workflow commands or handler results.
  Documentation and tests must treat this boundary as a first-class constraint.
- **Replay-gated mutation mismatch:** if user interceptors are skipped during replay, they cannot
  also be responsible for command mutations required to reconstruct history. Do not skip
  history-producing chains that can transform input; suppress side effects separately.
- **Accidental command mutation:** if input structs expose shared mutable nested data, interceptors
  can accidentally affect commands. Prefer owned values or explicit `with_*` replacement APIs.
- **Wrong abstraction level:** hooking core activations directly would expose too much internal
  machinery. Hook SDK dispatch/context methods instead.
- **Generic explosion:** typed outbound hooks for every activity/workflow type would not be
  object-safe. Prefer serialized payloads for the base trait.
- **Over-broad first slice:** implementing every Ruby-equivalent method at once will obscure
  whether inbound/outbound chaining works.
- **Replay semantics:** logging, metrics, and spans can double-count under replay unless
  interceptors use the computed `is_replaying_history_events` value to suppress duplicate
  side effects.
- **Cross-SDK expectation mismatch:** TypeScript and Python both let workflow-side interception
  participate in the replay-sensitive path, but they route/suppress side effects separately.
  Rust documentation must be explicit about which part is replay-sensitive and which part is
  side-effect-capable.
