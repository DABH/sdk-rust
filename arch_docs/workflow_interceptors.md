# Workflow Interceptors - Rust SDK

**Status:** Draft, middleware semantics explicit
**Goal:** Add workflow interceptors for actions that enter or exit workflow context, with
deterministic command mutation and an explicit replay-aware side-effect boundary.

**Shape:** synchronous continuation middleware with async-capable outputs. Interceptor trait
methods are synchronous functions (`fn`, not `async fn`). They must call `next.run(input)`
synchronously before returning. Async operations return futures as their output type, so an
interceptor can still wrap or observe the operation's *completion* by composing on the returned
future. What it cannot do is `.await` before forwarding to `next`. See section 5.6 for the
rationale.

This is intentionally different from client interceptors. It should not cover calls like fetching
workflow history, describing a workflow from a client, or listing workflows. It should cover
workflow code boundaries: executing workflow code, handling queries/signals/updates, and outbound
workflow commands such as timers, activities, child workflows, external workflow signals/cancels,
continue-as-new, and Nexus operations.

These interceptors are workflow middleware. They are allowed to inspect, replace, or transform the
input forwarded to `next`, and that forwarded input is the input the SDK uses to invoke handlers,
return results, or produce workflow commands. They need to support OpenTelemetry, but the SDK should
not conflate command-affecting middleware with side-effect export. Interceptor code that transforms
inputs used for workflow commands must remain deterministic and replay-compatible; user-defined
telemetry/log/metric side effects must be gated by replay state. The first implementation should
provide the interceptor traits and dispatch machinery only, not SDK-provided OpenTelemetry
interceptors or telemetry helpers.

For v1, workflow interceptors should align with other SDK behavior: an interceptor may pass the
original input or a modified input to `next`, and that forwarded input is what the SDK uses. For
history-producing outbound hooks, successful short-circuiting should not be supported in v1;
user-defined interceptors must continue the call chain exactly once, and they must do so
synchronously — without awaiting any future before calling `next`.

Workflow interceptor side effects must not be emitted while replaying history events. During replay,
the SDK still executes workflow code and reconstructs commands through the normal deterministic
path; if an interceptor participates in deterministic input transformation, that transformation
must run during replay while telemetry/log/metric export is suppressed.

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
and exit points, and they can behave as middleware over the values forwarded through those
boundaries. They should not be modeled as client operations, raw Core RPC hooks, or a
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

- interceptors form a chain and can inspect or modify input and result data before forwarding to
  the next interceptor; forwarded modifications are semantically part of the intercepted operation
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

TypeScript, Ruby, and Java workflow interceptors can affect operation data when they forward
modified inputs. Rust v1 should copy this middleware behavior for SDK parity, with one additional
Rust-specific constraint: history-producing outbound hooks must continue the call chain exactly
once so command generation remains owned by the SDK.

#### 2.2.1 Async Workflow Interceptors Are A Footgun TypeScript Already Tripped Over

TypeScript advertises async workflow interceptor methods (`async scheduleActivity(...): Promise<unknown>`,
`async handleSignal(...)`, etc.). Awaiting before forwarding to `next` inside a workflow
interceptor inserts a yield point into the workflow coroutine. That yield point is part of the
workflow's deterministic schedule: if the interceptor changes — or if it was added/removed
between SDK versions — the yield count for the same logical history changes, and replay raises an
NDE.

This is not theoretical. The TypeScript SDK ships permanent replay-compatibility flags whose sole
job is to paper over yield-point drift introduced by `await` calls in OTEL workflow interceptors.
Examples in the local TypeScript checkout:

- `packages/interceptors-opentelemetry/src/workflow/index.ts:75,94,174,196,234,253,273` —
  `if (!getActivator().hasFlag(SdkFlags.OpenTelemetryInterceporsAvoidsExtraYields)) await Promise.resolve();`
  is injected to reproduce yields that older versions of the OTEL interceptor introduced.
- `packages/workflow/src/flags.ts:48–88` — three flags
  (`OpenTelemetryInterceptorsTracesInboundSignals`, `OpenTelemetryInterceptorsTracesLocalActivities`,
  `OpenTelemetryInterceporsAvoidsExtraYields`) document specific incidents where an interceptor
  yield was added in 1.11.5, removed in 1.13.2, etc., and a flag was needed to keep historical
  workflows replayable.

The accidental nature of those yields is also evidence that pre-`next` `await` is *not* a
necessary capability: a survey of every official Temporal workflow interceptor (TS/Python/Go/Java
OTEL, Python LangSmith, openai-agents tracing, OpenTracing, Datadog) and every documented sample
found zero workflow interceptors that perform semantic async I/O before forwarding to `next`. Real
interceptors do synchronous header/span work and then forward. Forum threads asking how to do
async work in workflow interceptors are consistently redirected to activities (e.g.,
[community.temporal.io thread 5390](https://community.temporal.io/t/interceptor-determinism-workflow-replay-with-interceptors-is-executing-same-interceptor-code-again/5390)).

The lesson for Rust: do not adopt TypeScript's async interceptor shape. Make interceptor methods
synchronous so the coroutine cannot yield mid-chain, and let async-capable outputs (returned
futures) carry post-`next` async behavior. This eliminates an entire category of replay-flag
scaffolding without blocking any documented user pattern.

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

These prove the typed/erased middleware boundary: outbound calls should carry SDK-owned erased
typed arguments through the interceptor chain and serialize them only after the final `next`
continuation reaches SDK command construction.

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
- inspect workflow metadata, replay state, headers, typed/erased values, payloads, and outbound
  command inputs

Required constraints:

- interceptor side effects must not affect deterministic workflow behavior unless the resulting
  command/result is derived only from deterministic workflow inputs
- history-producing outbound hooks must call `next` exactly once
- interceptor methods must call `next.run(input)` synchronously; awaiting any future before
  forwarding to `next` is forbidden because that inserts a yield point into the workflow
  coroutine and breaks replay determinism (see section 2.2.1)
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
interceptor. This is middleware semantics: the value passed to `next` is the operation value the
SDK continues with. Input mutation is represented by passing a modified input value to `next`, not
by mutating shared SDK state in place.

Rust v1 should use these semantics:

- input structs describe SDK operations and may be transformed before forwarding
- `Next` accepts the input value to continue with
- forwarded input modifications affect handler invocation, operation results, or emitted commands
  according to the operation being intercepted
- history-producing outbound hooks must call `next` exactly once
- successful short-circuiting of history-producing outbound hooks is not supported in v1
- nested collections should avoid shared mutable aliasing; use owned values or copy-on-write
  `with_*` APIs so replacement is explicit
- input transformations that affect commands must be deterministic and must not depend on
  non-replayable side effects

### 5.4 Typed, Erased Values vs. Serialized Payloads

Inbound handlers already operate at points where the SDK dispatch layer has access to handler names,
payloads, headers, and a workflow context. Outbound workflow calls often start with typed Rust
inputs and serialize them inside `WorkflowContext` methods.

The base interceptor traits should remain object-safe, but that does not require eagerly converting
typed Rust values to `Payload`. For operations that start with typed Rust values and later emit
history commands or responses, the SDK should carry an SDK-owned erased value through the
interceptor chain and perform payload serialization only at the final SDK boundary.

Proposed wrapper:

```rust
pub struct WorkflowValue {
    type_id: TypeId,
    inner: Box<dyn TemporalSerializable>,
}

impl WorkflowValue {
    pub fn downcast_ref<T>(&self) -> Option<&T>
    where
        T: TemporalSerializable + 'static
    {
        // ...
    }

    pub fn downcast_mut<T>(&mut self) -> Option<&mut T>
    where
        T: TemporalSerializable + 'static
    {
        // ...
    }

    pub fn replace<T>(&mut self, value: T)
    where
        T: TemporalSerializable + 'static
    {
        // ...
    }
}
```

The exact name is open, but the wrapper should be owned by the SDK rather than exposing
`Box<dyn TemporalSerializable>` directly. The wrapper can capture `TypeId::of::<T>()` at
construction time and use that stored `TypeId` to implement downcast helpers without requiring
`TemporalSerializable: Any`. This mirrors the shape of `std::error::Error` downcasting: the public
downcast bound is the domain trait plus `'static`, while the implementation compares `TypeId`s and
uses an unsafe cast guarded by that check. To keep this sound, all constructors and replacement APIs
must set the stored `TypeId` from the same concrete value placed in `inner`; users should not be
able to construct an arbitrary `(TypeId, dyn TemporalSerializable)` pair.

Argument lists should be represented as one erased value, usually the same tuple or `RawValue`
already accepted by the typed SDK API. This mirrors TypeScript's `unknown[]` in spirit: most
interceptors will treat the value opaquely, while domain-specific interceptors can downcast to a
known input type or tuple and replace it deterministically.

For operations that originate as payloads from history or clients, the SDK has two viable choices:

- expose the payloads as `RawValue` through the same wrapper when no typed value has been
  constructed yet
- deserialize to the registered handler input type before the interceptor chain and pass that typed
  erased value to `next`

The first implementation can choose the simpler path per hook, but the target API should not make
early payload serialization the only extensibility point for history-producing outbound calls.
Outbound activity, local activity, child workflow, external signal, continue-as-new, workflow
completion, query result, and update result hooks should all serialize after the last interceptor
that can modify the value.

This should be implemented in a submodule of the interceptors with stringent testing around the `unsafe` behavior.

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

### 5.6 Synchronous Methods, Async-Capable Outputs

The interceptor trait methods are synchronous functions, not `async fn`. The output type of each
method is the same as the output type of the SDK operation being intercepted: synchronous
operations return values directly, asynchronous operations return futures (typically
`LocalBoxFuture<'static, _>` or a domain-specific cancellable future).

This shape preserves async-capable middleware while making pre-`next` yield points
*expressible-in-Rust impossible*:

- An interceptor cannot `.await` before calling `next.run(input)` because the method is not
  async.
- `Next<'a, I, O>` borrows SDK chain state for `'a`, and `run` consumes `self`. The `Next`
  cannot be moved into a `'static` future, stored, or deferred — it must be consumed before the
  method returns, which means `next.run(input)` must be called synchronously inside the method
  body.
- For async operations, the value returned by `next.run(input)` is the operation future. The
  interceptor can wrap that future (e.g., `Box::pin(async move { let r = fut.await; record(r) })`)
  to observe completion or transform the result. That wrapping is post-`next` and does not insert
  a coroutine yield before SDK command generation, so it is replay-safe.

This is the same conclusion every official Temporal workflow-interceptor implementation reaches
in practice (see section 2.2.1): synchronous header/span/log work, then forward, then optionally
wrap the result. Rust enforces it at the type level rather than relying on convention.

`Send` requirements on the returned future:

- If hooks run while the SDK is interacting with workflow-local `Rc`/`RefCell` state, the
  returned future should be non-`Send` (e.g., `LocalBoxFuture<'static, _>`).
- Side-effect/export helpers can still hand work off to normal worker-runtime tasks and `Send`
  futures without changing the command-side trait shape; this is a separate dispatch concern.
- Default assumption: do not require `Send` on workflow interceptor output futures until the
  hook points prove that requirement is sound.

---

## 6. Proposed Shape

Use separate inbound and outbound traits. A workflow interceptor factory wires them together for a
workflow instance. Individual operations should use an explicit `Next` value so ordering and
lifetime constraints are clear.

Trait methods are **synchronous** (`fn`, not `async fn`). The output type of each method matches
the SDK operation being intercepted: synchronous operations (e.g., query handling, timer
creation) return values directly; asynchronous operations (e.g., workflow execution, signal
handling, activity completion) return futures. Interceptors observe operation completion by
wrapping the returned future, not by awaiting before forwarding to `next`. See section 5.6.

`Next` should be public enough to appear in interceptor method signatures, but opaque enough that
users cannot construct it themselves. `Next` accepts the input to continue with, matching other SDK
interceptor APIs. `Next<'a, I, O>` is borrowed for `'a` and consumed by `run`, which structurally
prevents an interceptor method from awaiting before calling `next.run(input)` — the method body
must call `run` synchronously and return either the result or a future composed from it.

Sketch:

```rust
pub type WorkflowExecuteOutput = LocalBoxFuture<'static, WorkflowResult<WorkflowValue>>;
pub type WorkflowSignalOutput = LocalBoxFuture<'static, Result<(), WorkflowError>>;
pub type WorkflowQueryOutput = Result<WorkflowValue, WorkflowError>;
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
- because the enclosing interceptor method is `fn`, not `async fn`, `run` must be invoked
  synchronously in the method body before the method returns — there is no way to `.await`
  before calling `run`; this is the structural enforcement of the "no pre-`next` yield" rule
- for operations that produce payloads or commands, final serialization happens only after the
  last interceptor has forwarded to SDK-owned dispatch/command construction
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
    args: WorkflowValue,
    headers: HashMap<String, Payload>,
    context: WorkflowInterceptorContext,
}

#[non_exhaustive]
pub struct HandleSignalInput {
    signal_name: String,
    args: WorkflowValue,
    headers: HashMap<String, Payload>,
    context: WorkflowInterceptorContext,
}

#[non_exhaustive]
pub struct HandleQueryInput {
    query_name: String,
    args: WorkflowValue,
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
    args: WorkflowValue,
    options: ActivityOptions,
}

#[non_exhaustive]
pub struct StartChildWorkflowInput {
    workflow_type: String,
    workflow_id: String,
    args: WorkflowValue,
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
and dispatch layers where the operation still has SDK-level names, headers, typed values or
payloads, and options.

---

## 9. Implementation Plan

1. Keep this PR focused on traits and dispatch machinery. Do not add SDK-provided OTEL
   interceptors, telemetry helpers, activity interceptors, or client interceptors.
2. Add `WorkflowInterceptor`, `WorkflowInboundInterceptor`, and `WorkflowOutboundInterceptor`
   scaffolding under `crates/sdk/src/interceptors.rs` or a new `crates/sdk/src/interceptors/`
   module.
3. Define public opaque `Next<'a, I, O>` and its lifetime/single-use/input-forwarding semantics.
4. Define an SDK-owned `WorkflowValue`/erased-serializable wrapper for typed middleware values,
   including captured `TypeId`, downcast, replacement, and crate-private final serialization.
5. Thread workflow interceptor configuration through `WorkerOptions`, not `ClientOptions`.
6. Build inbound and outbound chains when a workflow instance is created.
7. For each workflow instance, create/wrap an outbound interceptor chain and store it in worker/SDK
   state associated with that instance. Do not expose this as user workflow context state.
8. Add `WorkflowInterceptorContext` to interceptor inputs with both raw `is_replaying` and
   computed `is_replaying_history_events`. Do not skip history-producing interceptor chains during
   replay as they can transform command inputs.
9. Implement first-slice hooks: `execute`, `handle_signal`, `handle_query`, and `sleep`.
10. Add workflow/unit or integration tests:
   - interceptor ordering
   - default forwarding
   - `Next` is single-use and marked `#[must_use]`
   - interceptor trait methods are synchronous (`fn`, not `async fn`); the trait shape
     structurally rejects `.await` before `next.run(input)`
   - async-output operations can be wrapped post-`next` (interceptor returns a composed future
     that observes completion of the operation future returned by `next.run(input)`)
   - error behavior where supported
   - signal/query headers visible in inbound inputs
   - timer interception observes the logical sleep operation
   - typed values can be downcast and replaced before forwarding to `next`
   - final payload serialization happens after the last interceptor
   - modified input forwarded to `next` affects handler invocation, operation results, or emitted
     workflow commands as appropriate for the hook
   - raw `is_replaying` is exposed on relevant operations
   - computed `is_replaying_history_events` is true for history replay and false for live
     read-only operations after replay catch-up
   - query interceptor inputs always have `is_replaying_history_events = false`
   - deterministic input transformations still produce replay-compatible commands
   - live query/update-validator work after replay catch-up can still invoke inbound interceptors
   - interceptor errors/panics are surfaced as failures of the intercepted operation, not worker
     crashes
11. Add activity and child-workflow outbound hooks after the first slice is stable. These are the
    first hooks that should prove typed/erased outbound argument mutation before command
    serialization.
12. Add updates and external workflow hooks after activity/child workflow hooks are stable.

Integration tests must be run with `cargo integ-test <test_name>`.

---

## 10. Implementation Details To Watch

- Replay state is operation-local. Each hook should set `is_replaying_history_events` according to
  the operation being intercepted. For example, query handlers should always receive false because
  queries cannot be replayed as history events.
- Interceptor methods should return the same operation result types as the wrapped SDK operation.
  Do not introduce a separate interceptor error channel in v1.
- Do not expose `Box<dyn TemporalSerializable>` directly. Use an SDK wrapper so the SDK can pair
  the trait object with a trustworthy `TypeId`, add downcasting/replacement, and control final
  serialization without committing to trait-object construction details.
- The payload converter path may need `?Sized` or wrapper-specific entry points so crate-private
  final serialization can serialize a `dyn TemporalSerializable` value.
- If an interceptor returns an error where the operation result type supports errors, treat it as
  if the intercepted call returned that error.
- Panics from interceptor code should be caught and converted through the same user-code failure
  mapping as a panic in the intercepted call. They should not crash the worker process; for
  application-level operation failures, this should become an application failure, analogous to
  activity panic handling.
- Interceptor methods are synchronous functions; awaiting external I/O before calling `next` is
  not expressible at the type level (no `async fn`, `Next` is borrowed and must be consumed
  synchronously) and is not a supported v1 contract for any future variant. Side-effecting
  interceptors should hand work off, for example to a span processor, without waiting for the
  result to continue workflow execution. Async observation of operation completion is supported
  by wrapping the future returned from `next.run(input)`.

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
- **Typed erasure footguns:** downcasting only works when the interceptor knows the exact concrete
  type, often the generated tuple input type. Documentation should make clear that this is an
  advanced middleware capability, not a general reflection system.
- **TypeId soundness:** downcasting without an `Any` supertrait is sound only if the SDK controls
  wrapper construction and always keeps the stored `TypeId` aligned with the concrete value in the
  trait object.
- **Wrong abstraction level:** hooking core activations directly would expose too much internal
  machinery. Hook SDK dispatch/context methods instead.
- **Generic explosion:** typed outbound hooks for every activity/workflow type would not be
  object-safe. Prefer SDK-owned erased typed values for the base trait, with downcasting for
  interceptors that know the concrete type.
- **Over-broad first slice:** implementing every Ruby-equivalent method at once will obscure
  whether inbound/outbound chaining works.
- **Replay semantics:** logging, metrics, and spans can double-count under replay unless
  interceptors use the computed `is_replaying_history_events` value to suppress duplicate
  side effects.
- **Cross-SDK expectation mismatch:** TypeScript and Python both let workflow-side interception
  participate in the replay-sensitive path, but they route/suppress side effects separately.
  Rust documentation must be explicit about which part is replay-sensitive and which part is
  side-effect-capable.
- **Async-interceptor expectation from TypeScript users:** users coming from TypeScript may expect
  `async fn` workflow interceptor methods. Rust deliberately rejects that shape (section 2.2.1,
  section 5.6). Documentation should explain the `fn`-with-future-output shape up front and point
  to the TS replay-flag history as concrete justification, so users do not file this as a
  missing-feature issue.
