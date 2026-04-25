# Client Interceptors — Rust SDK

**Status:** Draft, for discussion
**Authors:** @chris.olszewski
**Goal:** Introduce a user-extensible client interceptor layer to the Rust SDK, roughly on par with the
Go and Python SDK surfaces.

**Reading note:** when this document mentions an earlier possibility and a later explicit
`Decision (...)`, treat the later decision as authoritative. The earlier text is retained when it
explains the trade-off that led to the decision.

---

## 1. Motivation

Today, the Rust SDK client has **no user-facing interception layer** for high-level client
operations (`start_workflow`, `signal`, `query`, update, schedule, async activity completion, etc.).
The only extension points are:

- **`tonic::Interceptor` at the gRPC layer** (`ServiceCallInterceptor` in
  `crates/client/src/lib.rs:461-498`) — injects `client-name`, `client-version`, user headers, and
  API key metadata. Not user-extensible in a structured way.
- **`DataConverter`** — customizable payload serialization/codec, but only for payload bytes.
- **`WorkerInterceptor`** trait (`crates/sdk/src/interceptors.rs:18`) — intercepts worker-side
  workflow activations, not client calls.

Use cases driving this work:

- **Tracing / observability** — inject trace context into Temporal `Header` on outbound calls,
  create client spans (OpenTelemetry, parity with
  `go/contrib/opentelemetry` and `python/temporalio/contrib/opentelemetry`).
- **Auth / propagation** — attach dynamic credentials or request-scoped metadata per call.
- **Logging** — structured logging of every client operation with workflow IDs, types, outcomes.
- **Testing** — test doubles that record calls, simulate failures, or assert on headers/args.
- **Policy enforcement** — reject or rewrite calls (e.g., force a specific task queue, enforce
  allowed workflow types, require a `tenant-id` header).

Both Go and Python have been shipping this pattern for years; the Rust SDK should match.

---

## 2. Prior Art

### 2.1 Go SDK (`~/code/temporal/sdk/go`)

Two traits define the surface:

```go
// interceptor/interceptor.go
type ClientInterceptor interface {
    ClientInterceptorBase                 // required embed
    InterceptClient(next ClientOutboundInterceptor) ClientOutboundInterceptor
}

type ClientOutboundInterceptor interface {
    ClientOutboundInterceptorBase         // required embed
    ExecuteWorkflow(ctx, *ClientExecuteWorkflowInput) (WorkflowRun, error)
    SignalWorkflow(ctx, *ClientSignalWorkflowInput) error
    SignalWithStartWorkflow(ctx, *ClientSignalWithStartWorkflowInput) (WorkflowRun, error)
    CancelWorkflow(ctx, *ClientCancelWorkflowInput) error
    TerminateWorkflow(ctx, *ClientTerminateWorkflowInput) error
    QueryWorkflow(ctx, *ClientQueryWorkflowInput) (converter.EncodedValue, error)
    UpdateWorkflow(ctx, *ClientUpdateWorkflowInput) (WorkflowUpdateHandle, error)
    UpdateWithStartWorkflow(ctx, *ClientUpdateWithStartWorkflowInput) (WorkflowUpdateHandle, error)
    PollWorkflowUpdate(ctx, *ClientPollWorkflowUpdateInput) (*ClientPollWorkflowUpdateOutput, error)
    DescribeWorkflow(ctx, *ClientDescribeWorkflowInput) (*ClientDescribeWorkflowOutput, error)
    CreateSchedule(ctx, *ScheduleClientCreateInput) (ScheduleHandle, error)
    // + experimental activity execution methods
}
```

Wiring (`internal/client.go:1439-1443`):

```go
client.interceptor = &workflowClientInterceptor{client: client}
for i := len(options.Interceptors) - 1; i >= 0; i-- {
    client.interceptor = options.Interceptors[i].InterceptClient(client.interceptor)
}
```

Earlier entries in `options.Interceptors` wrap later ones (outermost is first). Users must embed
`ClientInterceptorBase` / `ClientOutboundInterceptorBase` so adding new methods is non-breaking.

Headers are mutable: `interceptor.Header(ctx)` returns a `map[string]*commonpb.Payload` that the
interceptor can edit on the `ExecuteWorkflow`/`SignalWithStart` paths before calling `next`.

### 2.2 Python SDK (`~/code/temporal/sdk/python`)

Same shape, adapted to Python idioms (`temporalio/client.py:7743`):

```python
class Interceptor:
    def intercept_client(self, next: OutboundInterceptor) -> OutboundInterceptor: ...

class OutboundInterceptor:
    def __init__(self, next: OutboundInterceptor) -> None: ...
    async def start_workflow(self, input: StartWorkflowInput) -> WorkflowHandle: ...
    async def signal_workflow(self, input: SignalWorkflowInput) -> None: ...
    async def query_workflow(self, input: QueryWorkflowInput) -> Any: ...
    # ... ~30 methods covering workflow, update, schedule, async-activity, worker-admin ops
```

Inputs are dataclasses, `headers: Mapping[str, Payload]` is a mutable field. Chaining matches Go:

```python
self._impl: OutboundInterceptor = _ClientImpl(self)
for interceptor in reversed(list(self._config["interceptors"])):
    self._impl = interceptor.intercept_client(self._impl)
```

### 2.3 TypeScript SDK (`~/code/temporal/sdk/typescript`)

TypeScript uses a distinct shape: interceptor methods take `(input, next)` where `next` is a
closure for the rest of the chain, rather than capturing `next` at construction time via a
factory. Roughly:

```ts
interface WorkflowClientCallsInterceptor {
  start?(input: StartInput, next: Next<StartInput, StartOutput>): Promise<StartOutput>;
  signal?(input: SignalInput, next: Next<SignalInput, void>): Promise<void>;
  // ...
}
```

Interceptors are plain objects with optional methods — no factory, no `next` field to store. The
Client walks the interceptor list per call, composing `next` closures right-to-left.

### 2.4 Key observations

1. **Two shapes in the wild**:
   - **Factory (Go/Python)**: `intercept_client(next) -> Outbound` captures `next` once at
     `Client::new`; interceptor holds it as a field. Chain-per-client, amortized.
   - **Per-call next (TypeScript)**: `method(input, next)` — `next` passed as an argument every
     call. Interceptor has no factory and no `next` field.

   Both work in Rust dyn-safely. **We propose the per-call shape** (see §4.5 and §5) — it collapses
   two traits into one, eliminates the factory + wrapping struct + `next()` accessor, and lets
   users override just the methods they care about. Cost is a small per-call closure allocation,
   which is noise vs. a gRPC RTT.
2. **Per-operation input structs with mutable headers**: makes it straightforward to add fields
   without breaking the trait surface.
3. **One interceptor, multiple kinds of interception**: Go and Python share an umbrella
   `Interceptor` that embeds both `ClientInterceptor` and `WorkerInterceptor` so a single
   `TracingInterceptor` implements both sides. We considered matching this, but v1 keeps the client
   and worker traits independent; see Decision Q4 in §5.7.
4. **Typed vs. erased args**: Go/Python interceptors see erased args (`[]any` / `Sequence[Any]`).
   They do **not** see typed generic inputs. This has implications for Rust (see §4).

---

## 3. Current Rust Client Surface

### 3.1 Relevant types (in `crates/client/src/`)

- `Connection` (`lib.rs:132-337`) — owns the gRPC channel, `ServiceCallInterceptor`, retry layer.
- `Client` (`lib.rs:600-703`) — namespace-bound client, wraps `Connection`.
- `WorkflowHandle<CT, W>` (`workflow_handle.rs:306-922`) — typed workflow handle. Provides
  `signal`, `query`, `execute_update`, `start_update`, `cancel`, `terminate`, `describe`,
  `fetch_history`, `get_result`.
- `WorkflowUpdateHandle<CT, T>` (`workflow_handle.rs:927-`) — provides `get_result`.
- `AsyncActivityHandle<CT>` (`async_activity_handle.rs:58-268`) — `complete`, `fail`,
  `report_cancelation`, `heartbeat`.
- `ScheduleHandle<CT>` (`schedules.rs:714-920`) + `Client::create_schedule` /
  `Client::list_schedules`.

### 3.2 The `WorkflowClientTrait` and how calls flow today

`Client::start_workflow` forwards to a private-crate `WorkflowClientTrait` (`lib.rs:741-792`), which
has a **blanket impl for any `T: WorkflowService + NamespacedClient + Clone + Send + Sync`**
(`lib.rs:1012-1015`). That blanket impl is where typed inputs get serialized via
`DataConverter::to_payloads(...)` and a gRPC `start_workflow_execution` is issued.

`WorkflowHandle`'s methods sidestep this trait — they call `WorkflowService::signal_workflow_execution`
etc. directly on `&mut self.client.clone()` (`workflow_handle.rs:592-612` for `signal`).

**Key boundary**: today the serialization step runs *immediately before* the gRPC call. There is no
layer between "typed args" and "gRPC request" other than `DataConverter`.

### 3.3 Existing tonic-level interceptor

`ServiceCallInterceptor` (`crates/client/src/lib.rs:461-498`) is a tonic `Interceptor` attached to
the `InterceptedService` at connection construction. It injects standard metadata on every outbound
RPC. We keep it. Client interceptors are a **separate, higher** layer that sits between `Client`
public methods and the gRPC services.

---

## 4. Rust-Specific Design Challenges

### 4.1 Typed vs. erased inputs

Go/Python interceptors see erased args (`[]any`, `Sequence[Any]`). Rust has no ergonomic `Any`
equivalent, and `W::Input` varies per call site, so we cannot put a generic associated type in a
dyn-safe trait method.

**Decision (Q2)**: interceptors operate on **already-serialized `Payloads`**. Conceptually this
matches Go/Python (erased representation) while being dyn-safe. Serialization via `DataConverter`
runs **before** the interceptor chain, not after.

Consequences:

- Interceptors that want to inspect typed args (e.g., redact by field) must do so through
  `DataConverter` (codec layer) or by downcasting after `Payload.deserialize`. Same limitation
  Go/Python effectively have.
- Interceptors can freely mutate `Payloads` (e.g., wrap in envelope, add signature payload).
- The chain sits **below** `DataConverter` serialization and **above** the gRPC request.

Trade-off: this is the opposite order from Go/Python where DC runs *inside* the base interceptor.
Flipping the order in Rust buys us dyn safety at the cost of interceptors not seeing typed args.
If preserving typed-arg visibility is critical, we need a generic-per-method design (see
alternatives, §7).

### 4.2 Async trait shape

We have two options:

- **`async_trait` with `?Send` disabled**: match `WorkerInterceptor`'s `#[async_trait(?Send)]`. But
  client interceptors must be `Send + Sync` since a `Client` may be cloned across threads.
- **Native `async fn` in traits** (stable since 1.75): we can return `-> impl Future + Send`.
  Cleaner, but the returned future types aren't namable for manual impls and some chaining patterns
  need pinning.

**Proposal**: Use `#[async_trait]` (Send bounds on) for the interceptor traits — this matches the
Go/Python developer ergonomics (implement a method, no lifetime juggling) and keeps chaining simple
with a type-erased `Next<I, O>` continuation.

### 4.3 Chain storage and dyn safety

The interceptor trait must be object-safe (we store it as `Arc<dyn ClientInterceptor>`). That
means:
- No generic methods.
- No `Self`-returning methods.
- All args/returns are concrete (Payloads, strings, option types, concrete `Next<I, O>`).

Typed output handles (`WorkflowHandle<Client, W>`) are a problem: we can't name `W` in an
object-safe trait. **Proposal**: the interceptor chain returns a non-generic
`StartWorkflowOutput { run_id, first_execution_run_id, eager_task: Option<...> }`, and
`Client::start_workflow<W>` assembles the typed `WorkflowHandle<Self, W>` from that output
outside the interceptor chain. Same pattern for `start_update` (returns a non-generic
`StartWorkflowUpdateOutput` that carries the `Outcome` proto; typed decoding happens outside).

### 4.5 `Next<I, O>` is dyn-safe

The per-call `next` closure is erased into a concrete struct:

```rust
pub struct Next<I, O> {
    f: Box<dyn FnOnce(I) -> Pin<Box<dyn Future<Output = O> + Send + 'static>> + Send + 'static>,
}

impl<I, O> Next<I, O> {
    pub fn new<F, Fut>(f: F) -> Self
    where
        F: FnOnce(I) -> Fut + Send + 'static,
        Fut: Future<Output = O> + Send + 'static,
    {
        Self {
            f: Box::new(move |input| Box::pin(f(input))),
        }
    }

    pub async fn call(self, input: I) -> O { (self.f)(input).await }
}
```

At each interceptor method, the `I` and `O` type parameters are pinned to that method's input
and output (e.g. `Next<StartWorkflowInput, Result<StartWorkflowOutput, WorkflowStartError>>`).
There are **no generic method parameters**, so the trait remains object-safe.

`Next` is intentionally `'static`: dispatchers capture owned values (`Client` clones and
`Arc<dyn ClientInterceptor>` clones) rather than borrowing from the stack. That avoids exposing a
`Next<'a, I, O>` lifetime in every user-implemented interceptor method. A `Client` clone is cheap,
so the ergonomics win is worth the small cost on the intercepted path. If we later need to optimize
this, we can change the internal dispatcher shape without changing the public trait.

### 4.4 Relationship to `WorkflowHandle` / `ScheduleHandle` / `AsyncActivityHandle`

Currently these handles hold a `CT: WorkflowService + NamespacedClient + Clone` and call services
directly. If the interceptor chain lives on `Client`, then calls issued through a `WorkflowHandle`
must also go through the chain.

**Decision (Q7)**: keep `CT` generic — handles stay parameterized over a client type so that test
doubles can continue to implement the bound without holding a concrete `Client`. We do **not**
pin `CT = Client`.

**Implication**: the trait that `CT` must satisfy is what becomes the "dispatch surface" for
handle methods. Concretely, we introduce a dispatch trait (working name `ClientDispatch`) with one
`async fn` per operation, each taking `OutboundCall<Input>` and returning the operation's typed
output. `Client` implements it by folding through its interceptor chain and calling `RootOutbound`;
internal test mocks can implement it directly with canned responses.

This trait probably should not be a long-term end-user API. It may need to be public initially
because handle types are generic over `CT`, but users should not be encouraged to mock clients by
implementing it. If exposed, document it as unstable/internal and expect it to change as new client
operations are added.

```rust
/// Dispatch surface used by SDK handle types.
///
/// This is public only if required by generic handle bounds. It is not intended as a stable
/// end-user mocking API and may change as client operations are added.
#[async_trait]
pub trait ClientDispatch: NamespacedClient + Clone + Send + Sync + 'static {
    async fn start_workflow(
        &self,
        call: OutboundCall<StartWorkflowInput>,
    ) -> Result<StartWorkflowOutput, WorkflowStartError>;

    async fn signal_workflow(
        &self,
        call: OutboundCall<SignalWorkflowInput>,
    ) -> Result<(), WorkflowInteractionError>;

    // ... one method per operation (mirrors the ClientInterceptor surface)
}
```

Handle bounds change from today's `CT: WorkflowService + NamespacedClient + Clone` to `CT:
ClientDispatch` (which already requires `NamespacedClient + Clone + Send + Sync`).

Consequences:

- **`Client` implements `ClientDispatch`** via the per-op dispatchers that fold the interceptor
  chain (§5.4). Interceptors only engage when going through `Client::method`.
- **Handles route through `ClientDispatch`**, so `WorkflowHandle<Client, W>::signal` automatically
  goes through the chain. `WorkflowHandle<MyMock, W>::signal` uses the mock's `ClientDispatch`
  impl directly — no interceptor involvement for mocks (matches today's behavior where mocks
  don't enter the current `ServiceCallInterceptor` machinery either).
- **Handles no longer directly call `WorkflowService::*`**. They call `self.client.method(call)`
  via `ClientDispatch`. That's a larger refactor of `crates/client/src/workflow_handle.rs` than
  originally scoped.
- **`ClientDispatch` has many methods**. Test mocks that today implement `WorkflowService`
  (generated trait, big surface anyway) now implement `ClientDispatch` instead — fewer methods
  typically but higher-level. Net: probably a wash for mock authors; more ergonomic since inputs
  are our typed `OutboundCall<Input>` rather than raw proto requests.

---

## 5. Proposed Design

### 5.1 Crate placement

- `crates/client/src/interceptor/` — new module.
  - `mod.rs` — `ClientInterceptor` trait, `Next<I, O>` helper, convenience re-exports.
  - `inputs.rs` — one `*Input`/`*Output` struct per operation.
- Re-exported from `crates/sdk/src/lib.rs` under `sdk::client::interceptor::*`.

### 5.2 Single trait with per-call `next` (mirrors TypeScript)

One trait, no factory, no wrapping struct. Every method has a default that just forwards via
`next.call(call).await`, so users override only the operations they care about. Per-call RPC
metadata and timeout live on an `OutboundCall<T>` envelope, so each operation's `*Input` struct
only carries Temporal-semantic fields.

```rust
// crates/client/src/interceptor/mod.rs

/// Envelope wrapping every outbound client call with RPC-transport concerns.
/// Interceptors can mutate `rpc_metadata` and `rpc_timeout`; `input` is the
/// operation-specific Temporal payload.
pub struct OutboundCall<T> {
    pub rpc_metadata: MetadataMap,
    pub rpc_timeout: Option<Duration>,
    pub input: T,
}

#[async_trait]
pub trait ClientInterceptor: Send + Sync + 'static {
    async fn start_workflow(
        &self,
        call: OutboundCall<StartWorkflowInput>,
        next: Next<OutboundCall<StartWorkflowInput>,
                   Result<StartWorkflowOutput, WorkflowStartError>>,
    ) -> Result<StartWorkflowOutput, WorkflowStartError> {
        next.call(call).await
    }

    async fn signal_workflow(
        &self,
        call: OutboundCall<SignalWorkflowInput>,
        next: Next<OutboundCall<SignalWorkflowInput>, Result<(), WorkflowInteractionError>>,
    ) -> Result<(), WorkflowInteractionError> {
        next.call(call).await
    }

    async fn query_workflow(
        &self,
        call: OutboundCall<QueryWorkflowInput>,
        next: Next<OutboundCall<QueryWorkflowInput>,
                   Result<QueryWorkflowOutput, WorkflowQueryError>>,
    ) -> Result<QueryWorkflowOutput, WorkflowQueryError> {
        next.call(call).await
    }

    async fn start_workflow_update(
        &self,
        call: OutboundCall<StartWorkflowUpdateInput>,
        next: Next<OutboundCall<StartWorkflowUpdateInput>,
                   Result<StartWorkflowUpdateOutput, WorkflowUpdateError>>,
    ) -> Result<StartWorkflowUpdateOutput, WorkflowUpdateError> {
        next.call(call).await
    }

    async fn execute_workflow_update(
        &self,
        call: OutboundCall<ExecuteWorkflowUpdateInput>,
        next: Next<OutboundCall<ExecuteWorkflowUpdateInput>,
                   Result<ExecuteWorkflowUpdateOutput, WorkflowUpdateError>>,
    ) -> Result<ExecuteWorkflowUpdateOutput, WorkflowUpdateError> {
        next.call(call).await
    }

    async fn get_workflow_update_result(
        &self,
        call: OutboundCall<GetWorkflowUpdateResultInput>,
        next: Next<OutboundCall<GetWorkflowUpdateResultInput>,
                   Result<GetWorkflowUpdateResultOutput, WorkflowUpdateError>>,
    ) -> Result<GetWorkflowUpdateResultOutput, WorkflowUpdateError> {
        next.call(call).await
    }

    async fn cancel_workflow(
        &self,
        call: OutboundCall<CancelWorkflowInput>,
        next: Next<OutboundCall<CancelWorkflowInput>, Result<(), WorkflowInteractionError>>,
    ) -> Result<(), WorkflowInteractionError> {
        next.call(call).await
    }

    async fn terminate_workflow(
        &self,
        call: OutboundCall<TerminateWorkflowInput>,
        next: Next<OutboundCall<TerminateWorkflowInput>, Result<(), WorkflowInteractionError>>,
    ) -> Result<(), WorkflowInteractionError> {
        next.call(call).await
    }

    async fn describe_workflow(
        &self,
        call: OutboundCall<DescribeWorkflowInput>,
        next: Next<OutboundCall<DescribeWorkflowInput>,
                   Result<DescribeWorkflowOutput, WorkflowInteractionError>>,
    ) -> Result<DescribeWorkflowOutput, WorkflowInteractionError> {
        next.call(call).await
    }

    async fn fetch_workflow_history(
        &self,
        call: OutboundCall<FetchWorkflowHistoryInput>,
        next: Next<OutboundCall<FetchWorkflowHistoryInput>,
                   Result<FetchWorkflowHistoryOutput, WorkflowInteractionError>>,
    ) -> Result<FetchWorkflowHistoryOutput, WorkflowInteractionError> {
        next.call(call).await
    }

    async fn list_workflows(
        &self,
        call: OutboundCall<ListWorkflowsInput>,
        next: Next<OutboundCall<ListWorkflowsInput>, Result<ListWorkflowsStream, ClientError>>,
    ) -> Result<ListWorkflowsStream, ClientError> {
        next.call(call).await
    }

    async fn count_workflows(
        &self,
        call: OutboundCall<CountWorkflowsInput>,
        next: Next<OutboundCall<CountWorkflowsInput>,
                   Result<WorkflowExecutionCount, ClientError>>,
    ) -> Result<WorkflowExecutionCount, ClientError> {
        next.call(call).await
    }

    // --- async activity completion ---
    async fn heartbeat_async_activity(
        &self,
        call: OutboundCall<HeartbeatAsyncActivityInput>,
        next: Next<OutboundCall<HeartbeatAsyncActivityInput>,
                   Result<ActivityHeartbeatResponse, AsyncActivityError>>,
    ) -> Result<ActivityHeartbeatResponse, AsyncActivityError> {
        next.call(call).await
    }
    async fn complete_async_activity(
        &self,
        call: OutboundCall<CompleteAsyncActivityInput>,
        next: Next<OutboundCall<CompleteAsyncActivityInput>, Result<(), AsyncActivityError>>,
    ) -> Result<(), AsyncActivityError> {
        next.call(call).await
    }
    async fn fail_async_activity(
        &self,
        call: OutboundCall<FailAsyncActivityInput>,
        next: Next<OutboundCall<FailAsyncActivityInput>, Result<(), AsyncActivityError>>,
    ) -> Result<(), AsyncActivityError> {
        next.call(call).await
    }
    async fn report_cancellation_async_activity(
        &self,
        call: OutboundCall<ReportCancellationAsyncActivityInput>,
        next: Next<OutboundCall<ReportCancellationAsyncActivityInput>,
                   Result<(), AsyncActivityError>>,
    ) -> Result<(), AsyncActivityError> {
        next.call(call).await
    }

    // --- schedules (abbreviated; all follow the same pattern) ---
    async fn create_schedule(
        &self,
        call: OutboundCall<CreateScheduleInput>,
        next: Next<OutboundCall<CreateScheduleInput>,
                   Result<CreateScheduleOutput, ScheduleError>>,
    ) -> Result<CreateScheduleOutput, ScheduleError> { next.call(call).await }
    async fn describe_schedule(
        &self,
        call: OutboundCall<DescribeScheduleInput>,
        next: Next<OutboundCall<DescribeScheduleInput>,
                   Result<ScheduleDescription, ScheduleError>>,
    ) -> Result<ScheduleDescription, ScheduleError> { next.call(call).await }
    async fn update_schedule(
        &self,
        call: OutboundCall<UpdateScheduleInput>,
        next: Next<OutboundCall<UpdateScheduleInput>, Result<(), ScheduleError>>,
    ) -> Result<(), ScheduleError> { next.call(call).await }
    async fn delete_schedule(
        &self,
        call: OutboundCall<DeleteScheduleInput>,
        next: Next<OutboundCall<DeleteScheduleInput>, Result<(), ScheduleError>>,
    ) -> Result<(), ScheduleError> { next.call(call).await }
    async fn pause_schedule(
        &self,
        call: OutboundCall<PauseScheduleInput>,
        next: Next<OutboundCall<PauseScheduleInput>, Result<(), ScheduleError>>,
    ) -> Result<(), ScheduleError> { next.call(call).await }
    async fn unpause_schedule(
        &self,
        call: OutboundCall<UnpauseScheduleInput>,
        next: Next<OutboundCall<UnpauseScheduleInput>, Result<(), ScheduleError>>,
    ) -> Result<(), ScheduleError> { next.call(call).await }
    async fn trigger_schedule(
        &self,
        call: OutboundCall<TriggerScheduleInput>,
        next: Next<OutboundCall<TriggerScheduleInput>, Result<(), ScheduleError>>,
    ) -> Result<(), ScheduleError> { next.call(call).await }
    async fn backfill_schedule(
        &self,
        call: OutboundCall<BackfillScheduleInput>,
        next: Next<OutboundCall<BackfillScheduleInput>, Result<(), ScheduleError>>,
    ) -> Result<(), ScheduleError> { next.call(call).await }
    async fn list_schedules(
        &self,
        call: OutboundCall<ListSchedulesInput>,
        next: Next<OutboundCall<ListSchedulesInput>,
                   Result<ListSchedulesStream, ScheduleError>>,
    ) -> Result<ListSchedulesStream, ScheduleError> { next.call(call).await }
}

/// Type-erased continuation for the interceptor chain.
/// `I` is the input (typically `OutboundCall<SomeInput>`), `O` is the method's return type.
pub struct Next<I, O> {
    f: Box<dyn FnOnce(I) -> Pin<Box<dyn Future<Output = O> + Send + 'static>> + Send + 'static>,
}

impl<I, O> Next<I, O> {
    pub fn new<F, Fut>(f: F) -> Self
    where
        F: FnOnce(I) -> Fut + Send + 'static,
        Fut: Future<Output = O> + Send + 'static,
    {
        Self {
            f: Box::new(move |input| Box::pin(f(input))),
        }
    }

    pub async fn call(self, input: I) -> O {
        (self.f)(input).await
    }
}
```

`Next` is single-use (`FnOnce`) and intentionally owns a `'static` future. The dispatcher satisfies
that by moving cheap `Client` clones and `Arc<dyn ClientInterceptor>` clones into each continuation.
This keeps user implementations free of explicit lifetimes.

#### User code for a typical interceptor

```rust
struct TenantInterceptor { tenant: String }

#[async_trait]
impl ClientInterceptor for TenantInterceptor {
    async fn start_workflow(
        &self,
        mut call: OutboundCall<StartWorkflowInput>,
        next: Next<OutboundCall<StartWorkflowInput>,
                   Result<StartWorkflowOutput, WorkflowStartError>>,
    ) -> Result<StartWorkflowOutput, WorkflowStartError> {
        // RPC-level: gRPC metadata on the wire.
        call.rpc_metadata.insert("x-tenant", self.tenant.parse().unwrap());
        // Temporal-level: travels with the workflow.
        call.input.header.get_or_insert_default().fields.insert(
            "tenant".into(), as_payload(&self.tenant),
        );
        next.call(call).await
    }
    // every other method inherits the default forward; no code needed.
}
```

A generic helper can pull common logic out of per-op overrides:

```rust
fn add_auth<T>(call: &mut OutboundCall<T>, token: &str) {
    call.rpc_metadata.insert("authorization",
        format!("Bearer {token}").parse().unwrap());
}
```

- **One trait, one impl.** No factory, no wrapping struct with a `next: Arc<dyn ...>` field.
- **Unoverridden methods just work** via the trait's default `next.call(input).await`.
- **Short-circuit by not calling `next`** — same semantics as Go/Python/TS. Intended use cases
  include policy-rejection, test stubs, and cache hits.

Document on the trait: *"Implementors overriding a method must call `next.call(input).await` to
forward, or intentionally short-circuit by returning a value without invoking `next`. Short-circuit
is a supported pattern (e.g., policy rejections, test stubs)."*

#### Operation granularity

Interceptors target logical SDK API calls in the supported surface, not raw protobuf RPCs. A single
interceptor method may map to one RPC, multiple RPCs, or no RPC if the SDK can answer from
already-known state. Examples:

- `start_workflow` remains one logical interceptor call even when
  `WorkflowStartOptions.start_signal` causes the root implementation to use
  `SignalWithStartWorkflowExecution`.
- `execute_workflow_update`, `start_workflow_update`, and `get_workflow_update_result` are distinct
  logical operations. `execute_workflow_update` should not be modeled as nested
  `start_workflow_update` plus `get_workflow_update_result` interceptor calls.
- Schedule patch operations are exposed as logical `pause_schedule`, `unpause_schedule`,
  `trigger_schedule`, and `backfill_schedule` calls even though they share the same underlying RPC.
- Async activity completion methods are logical operations; whether the identifier is a task token
  or workflow/activity ID is part of the input, not a separate interceptor method.

### 5.3 Input struct shapes

Interceptors receive `OutboundCall<T>` (the envelope defined in §5.2). The envelope carries
RPC-transport concerns; the inner `T` carries only Temporal-semantic fields for the operation.

#### 5.3.1 `OutboundCall<T>` envelope

```rust
pub struct OutboundCall<T> {
    /// gRPC metadata for this call only. Merged with (and overrides) connection-level
    /// metadata injected by `ServiceCallInterceptor` at the tonic layer.
    pub rpc_metadata: MetadataMap,

    /// Per-call gRPC deadline override. If `Some`, `RootOutbound` applies it to the tonic
    /// `Request` before the connection-level `set_default_timeout` runs; if `None`, the
    /// connection default wins.
    pub rpc_timeout: Option<Duration>,

    /// Operation-specific input.
    pub input: T,
}
```

The envelope is **present on every interceptor method input** (no exceptions). This is functional
parity with Python's `rpc_metadata` / `rpc_timeout` mandatory kwargs and lets interceptors add
per-call auth tokens, trace baggage, or tighter deadlines without caring which operation they're
wrapping. (See §5.2 for a generic `add_auth<T>(call: &mut OutboundCall<T>)` helper.)

Users access the inner fields explicitly through `call.input.<field>` — no `Deref` (foot-guns,
and explicit access keeps the envelope visible in reader code).

#### 5.3.2 Per-operation inner structs

Input structs are public because they appear in public interceptor method signatures, but they are
not intended as a stable construction API. Interceptors should inspect and mutate instances they
receive from the SDK; users should not construct these inputs themselves. Mark them
`#[non_exhaustive]` and provide crate-private constructors/builders for SDK construction. This
matches the Ruby SDK guidance: client interceptor input classes may change in backwards-incompatible
ways, and users should not instantiate them directly.

Each `*Input` carries:
- Operation-specific identifying fields (workflow_id, run_id, signal_name, query_type, etc.).
- Already-serialized `Payloads` for args.
- Mutable `header: Option<Header>` — Temporal `Header` (travels with the workflow/signal/update
  payload, visible to workflow code and the worker). Distinct from the envelope's
  `rpc_metadata`.
- The original typed options struct (`WorkflowStartOptions`, etc.) — interceptors can inspect
  things like `task_queue`, `retry_policy`, `search_attributes`.

Example:

```rust
#[non_exhaustive]
pub struct StartWorkflowInput {
    pub workflow_type: String,      // e.g. "my_workflow"
    pub workflow_id: String,
    pub task_queue: String,
    pub args: Payloads,             // already-serialized via DataConverter
    pub header: Option<Header>,     // Temporal header (not gRPC metadata)
    pub options: WorkflowStartOptions, // retry policy, timeouts, search attrs, etc.
}

pub struct StartWorkflowOutput {
    pub run_id: String,
    pub first_execution_run_id: String,
    /// If the server returned an eager-execution task, it's threaded here.
    pub eager_task: Option<PollWorkflowTaskQueueResponse>,
}
```

Output types are intentionally **non-generic** (see §4.3). Typed handle assembly happens in
`Client::start_workflow<W>` after the chain returns.

Unlike input structs, output structs that are useful for short-circuiting should have explicit
public constructors or smart constructors. Otherwise the trait advertises short-circuiting but makes
it impossible for external interceptors to return successful synthetic values for operations whose
output fields are private.

Note: no `OutboundCall`-equivalent on the output side today. If we ever want to surface response
trailer metadata, we can add a symmetric `OutboundResponse<T>` without breaking callers.

#### 5.3.3 `rpc_metadata` type choice

`tonic::metadata::MetadataMap` is the honest type — it handles ASCII vs. binary keys correctly
(keys ending in `-bin` take `Vec<u8>` values) and is what the gRPC request already carries. The
downside is it's less ergonomic than a `HashMap<String, String>`.

Two alternatives to discuss:

1. **`MetadataMap` directly**. Users learn one type (tonic's), binary headers Just Work, no
   conversion at the `RootOutbound` boundary.
2. **Mirror `ConnectionOptions`: split `headers: HashMap<String, String>` and `binary_headers:
   HashMap<String, Vec<u8>>`**. Matches today's surface, simpler for new users, but duplicates a
   type tonic already provides.

Leaning **(1)**. Small `MetadataMap::from_headers` helpers can smooth the common case.

#### 5.3.4 Interaction with connection-level metadata and default timeout

- Connection-level metadata (`ConnectionOptions.headers` + API key) is still injected by
  `ServiceCallInterceptor` at the tonic layer. Per-call `rpc_metadata` set by interceptors is
  merged into the tonic `Request` **before** it hits that layer; existing keys set by the
  interceptor win over connection-level defaults (matching Python semantics).
- Default RPC timeout: today `ServiceCallInterceptor` calls `request.set_default_timeout(
  OTHER_CALL_TIMEOUT)` if none is set (`lib.rs:494`). If an interceptor populates
  `rpc_timeout`, `RootOutbound` writes it onto the request **before** the tonic interceptor
  runs, so `set_default_timeout` becomes a no-op and the interceptor's deadline takes effect.

### 5.4 Wiring on `Client`

1. Add `interceptors: Vec<Arc<dyn ClientInterceptor>>` to `ClientOptions`. Store it internally as
   an `Arc<[Arc<dyn ClientInterceptor>]>` for cheap cloning. Because `ClientOptions` currently
   derives `Debug`, either skip this field from `Debug` or wrap it in a type with a manual `Debug`
   impl.
2. `Client::new` stores the list; no chain is built eagerly. Instead, `Client`'s impl of
   `ClientDispatch` folds the list right-to-left into a `Next` per call. Each operation gets its
   own dispatcher function — **we write these out by hand** (Decision Q10), one per logical API
   call. The bodies are mechanical but we avoid hiding public signatures behind a macro (hurts
   rustdoc and IDE navigation).
   ```rust
   #[async_trait]
   impl ClientDispatch for Client {
       async fn start_workflow(
           &self,
           call: OutboundCall<StartWorkflowInput>,
       ) -> Result<StartWorkflowOutput, WorkflowStartError> {
           let root = RootOutbound::new(self.clone());
           let interceptors = self.options.interceptors.clone();
           // Fast path: no interceptors → call root directly, no allocation.
           if interceptors.is_empty() {
               return root.start_workflow(call).await;
           }
           // Leaf: the gRPC-issuing terminal closure.
           let mut next: Next<OutboundCall<StartWorkflowInput>, _> =
               Next::new(move |call| async move { root.start_workflow(call).await });
           // Fold from innermost interceptor outward.
           for interceptor in interceptors.iter().rev().cloned() {
               let next_inner = next;
               next = Next::new(move |call| async move {
                   interceptor.start_workflow(call, next_inner).await
               });
           }
           next.call(call).await
       }
       // ... one hand-written impl per operation.
   }
   ```
3. `Client::start_workflow<W>` (the typed convenience) becomes a thin wrapper that serializes
   input and invokes `ClientDispatch::start_workflow`:
   ```rust
   pub async fn start_workflow<W: HasWorkflowDefinition>(...) -> Result<WorkflowHandle<Self, W>, _> {
       let args = self.data_converter().to_payloads(ctx, &input).await?;
       let call = OutboundCall {
           rpc_metadata, rpc_timeout,
           input: StartWorkflowInput {
               workflow_type: W::name().into(), workflow_id, task_queue,
               args, header, options,
           },
       };
       let out = <Self as ClientDispatch>::start_workflow(self, call).await?;
       Ok(WorkflowHandle::new(self.clone(), out.run_id, out.first_execution_run_id, ...))
   }
   ```
4. `WorkflowHandle<CT, W>` (with `CT: ClientDispatch`) methods — `signal`, `query`,
   `execute_update`, `start_update`, `cancel`, `terminate`, `describe`, `fetch_history` — and
   `WorkflowUpdateHandle<CT, T>::get_result` serialize typed inputs into `OutboundCall<SomeInput>`
   and invoke the corresponding `ClientDispatch` method on `self.client`. When `CT = Client`, calls
   go through the interceptor chain; when `CT = SomeMock`, they hit the mock's impl directly.
5. `AsyncActivityHandle<CT>`, `ScheduleHandle<CT>`, and the list/count methods follow the same
   pattern.

#### Decision (Q8): chain-per-call allocation

Accepted for v1. The fast path for empty interceptor list (shown above) keeps the common case
allocation-free. If we later see allocation pressure in hot paths with non-empty chains, we can
revisit (e.g., small-vec backing, arena, or a position-index `Next` variant) without changing the
public trait.

#### Decision (Q10): hand-written dispatchers

Write the `ClientDispatch` methods out by hand. Bodies are mechanical, but macro-generated
public trait impls degrade rustdoc and IDE jump-to-def. One logical operation per file under
`crates/client/src/interceptor/dispatch/` keeps diffs contained when new operations are added.

### 5.5 The root / leaf

There's no separate `ClientOutboundInterceptor` trait. The leaf is a crate-private struct
`RootOutbound` exposing one `async fn` per operation, taking the same `OutboundCall<T>` envelope
used by the trait (e.g.,
`async fn start_workflow(&self, OutboundCall<StartWorkflowInput>) -> Result<StartWorkflowOutput, _>`).
`RootOutbound` owns a cheap `Client` clone so terminal `Next` continuations can be `'static`
without borrowing the dispatcher's stack frame. Its methods are the current `WorkflowClientTrait`
blanket impls — same code, just relocated, plus applying `rpc_metadata` and `rpc_timeout` from the
envelope to the outgoing tonic `Request`. The per-op dispatcher (§5.4) wraps each
`RootOutbound::method` into the terminal `Next`.

### 5.6 Layer ordering summary

```
Client::start_workflow<W>(typed W::Input)
       │
       ▼  DataConverter::to_payloads — typed → Payloads
       │
       ▼  ClientInterceptor chain (outermost first)  ← NEW
       │
       ▼  RootOutbound — builds gRPC request
       │
       ▼  tonic InterceptedService (ServiceCallInterceptor) — default gRPC metadata
       │                                                      + default timeout (only if unset)
       │
       ▼  RetryClient layer (retry.rs) — retries/backoff
       │
       ▼  gRPC / tonic channel
```

### 5.7 Interaction with `WorkerInterceptor`

**Decision (Q4)**: keep `ClientInterceptor` and `WorkerInterceptor` independent. No umbrella
`Interceptor` trait in v1. A user that wants a single tracing type covering both sides implements
both traits on the same struct and registers the same `Arc<T>` with `ClientOptions.interceptors`
and the worker's interceptor list. We can revisit adding an umbrella later if convention demands
it; no harm done by starting simple.

---

## 6. Operations covered (initial scope)

**In scope for v1:**

- Workflow: `start_workflow`, `signal_workflow`, `query_workflow`, `cancel_workflow`,
  `terminate_workflow`, `describe_workflow`, `fetch_workflow_history`, `list_workflows`,
  `count_workflows`.
- Workflow update: `start_workflow_update`, `execute_workflow_update`,
  `get_workflow_update_result`. **Decision (Q6)**: interceptors target logical SDK calls, so
  `execute_update` is a distinct interceptor method. It may be implemented by one RPC, multiple
  RPCs, or a server long-poll under the hood, but interceptors see the user's logical call once.
  `WorkflowUpdateHandle::get_result` maps to `get_workflow_update_result`; if the outcome is
  already known, the root can return without issuing an RPC.
- Async activity completion: `heartbeat`, `complete`, `fail`, `report_cancellation`.
- Schedules: all nine operations (`create`, `describe`, `update`, `delete`, `pause`, `unpause`,
  `trigger`, `backfill`, `list`).

**Deferred (v2 candidates):**

- Signal-with-start as a distinct method. Today `WorkflowStartOptions.start_signal` carries this;
  we can fold it into `start_workflow` like the current client does, OR split it. Decide during
  review.
- Update-with-start. Same question.
- Worker build ID / task-queue admin RPCs (Python has these; low demand).
- Client-executed activities (Go "experimental").
- Nexus operation start/cancel client APIs once those land in the Rust client.

---

## 7. Alternatives considered

### 7.0 Factory pattern with two traits (Go/Python shape)

Two traits: `ClientInterceptor { fn intercept_client(next) -> Arc<dyn ClientOutboundInterceptor> }`
and the big `ClientOutboundInterceptor` trait the user implements by wrapping `next`. Chain is
built once at `Client::new` and stored as `Arc<dyn ClientOutboundInterceptor>`. Defaults on the
outbound trait methods require a `fn next(&self) -> &dyn ClientOutboundInterceptor` accessor.

**Rejected** for the Rust SDK in favor of the per-call `Next` shape (§5.2). The factory version
forces each interceptor to define two types (factory + wrapping struct) and carry a `next: Arc<dyn
...>` field, for the sake of amortizing chain construction — and that amortization buys basically
nothing relative to a gRPC RTT. Go and Python pay this tax because closures-as-values are awkward
in those languages; Rust has no such constraint. TypeScript's shape ports cleanly.

### 7.1 Typed interceptors (generic over `W`)

Mirror typed args end-to-end with a trait like:

```rust
trait ClientInterceptor {
    async fn start_workflow<W: HasWorkflowDefinition>(
        &self, input: StartWorkflowInput<W>, next: impl StartWorkflowNext<W>,
    ) -> ...;
}
```

**Rejected** — not object-safe. Would force static dispatch only (no boxed chains), precluding
dynamic configuration. Rust's existing blanket-impl trait pattern works per-call-site but not for
a registry of heterogeneous interceptors.

### 7.2 Tower-style middleware

Expose each operation as a `tower::Service<Req, Response = Resp>` and let users stack any
tower `Layer`. Very idiomatic Rust. Downsides:
- Per-operation `Service` types means ~25 layer variants to compose; users who want
  "headers on every call" would need to register against every op.
- Harder to port Go/Python examples.
- Loses the single `ClientInterceptor` trait shape that Temporal users across languages expect.

**Rejected** as the primary API, but worth considering as an *alternative front-door* for users who
prefer `tower`. Could be layered on top: `fn from_tower_layers(...) -> impl ClientInterceptor`.

### 7.3 Interceptor operates before DataConverter (typed args visible)

Preserve typed args by making the interceptor generic at the call site and boxing only the erased
parts. This is doable with a helper trait for each operation plus dynamic dispatch of the "extras"
(headers, options). Complexity high for unclear benefit; deferred.

### 7.4 Event-based / callback-only interceptors

A minimal "on_call_started / on_call_completed" hook with no ability to modify the call. Simpler
but insufficient — trace header injection requires mutating the outbound request.

---

## 8. Resolved questions

All v1 design questions resolved. Summary (each decision is also surfaced near its relevant
section, linked below):

1. ~~**Default method impls vs. required forwarding**~~ — resolved by the per-call `Next` shape:
   every trait method has a default `next.call(call).await`. Intentional short-circuit (not
   calling `next`) is documented and supported. See §5.2.
2. ~~**Args format**~~ — interceptors see already-serialized `Payloads`. See §4.1.
3. ~~**Input ownership**~~ — by value. Required by `Next<I, O>::call(self, input: I)`. See §5.2.
4. ~~**Umbrella `Interceptor` trait**~~ — keep `ClientInterceptor` and `WorkerInterceptor`
   independent for v1. One struct can implement both if the user wants cross-cutting behavior.
   See §5.7.
5. ~~**gRPC metadata interceptors**~~ — `rpc_metadata: MetadataMap` and `rpc_timeout:
   Option<Duration>` live on the `OutboundCall<T>` envelope. See §5.3.1. Sub-decision on
   `MetadataMap` vs. split maps still bikesheddable — leaning `MetadataMap` (§5.3.3).
6. ~~**`execute_update` semantics**~~ — model it as one logical `execute_workflow_update`
   interceptor call. `start_workflow_update` and `get_workflow_update_result` remain separate
   logical calls when users invoke those APIs directly. See §6.
7. ~~**`WorkflowHandle` call routing**~~ — keep `CT` generic. Introduce a public `ClientDispatch`
   trait that handles are bound over; `Client` implements it by folding through the interceptor
   chain, mocks implement it directly. See §4.4 and §5.4.
8. ~~**Chain-per-call allocation**~~ — acceptable for v1. Fast-path empty-list case to avoid
   allocation when no interceptors are configured. Revisit if hot-path pressure warrants. See
   §5.4.
9. ~~**Error types at the trait boundary**~~ — keep per-operation concrete errors
   (`WorkflowStartError`, `WorkflowInteractionError`, …). Trades abstraction for fidelity; users
   stay in our existing error taxonomy. No change.
10. ~~**Dispatcher plumbing**~~ — hand-written, one impl per operation. No macro-hidden public
    signatures. See §5.4.

---

## 9. Implementation phases

0. **Finalize the operation matrix** — for each logical SDK API call, record the interceptor method,
   input type, output type, error type, and whether the root may issue zero, one, or multiple RPCs.
   This prevents raw-RPC leakage into the public interceptor surface.
1. **`ClientDispatch` trait + `ClientInterceptor` trait + `OutboundCall<T>` + input/output structs**
   in `crates/client/src/interceptor/`. `Next` owns `'static` continuations; `RootOutbound` owns a
   cheap `Client` clone and hosts the existing `WorkflowClientTrait` logic (same code, relocated,
   plus `OutboundCall` envelope application onto the tonic request).
2. **Hand-write `impl ClientDispatch for Client`** — one method per operation. Each folds the
   interceptor chain with the empty-list fast path. Wire `ClientOptions.interceptors`.
3. **Rewrite typed `Client::start_workflow<W>` / `list_workflows` / `count_workflows` /
   `get_async_activity_handle` / schedule methods** to serialize typed inputs into
   `OutboundCall<Input>` and invoke the matching `ClientDispatch` method.
4. **Port `WorkflowHandle`, `WorkflowUpdateHandle`, `AsyncActivityHandle`, `ScheduleHandle`** to
   require `CT: ClientDispatch` instead of `CT: WorkflowService`. Handle methods now serialize
   typed inputs into `OutboundCall<Input>` and invoke `self.client.<method>(call)` via
   `ClientDispatch`.
5. **Tests** — add a `RecordingInterceptor` in `crates/client/tests/` that captures inputs for
   assertion; verify every operation flows through the chain. Port a few Python tracing tests.
6. **Docs + example**: `examples/client_interceptor` showing a trivial header-injection
   interceptor and a tracing interceptor skeleton.
7. **OpenTelemetry contrib**: follow-up, separate PR. Mirror
   `python/temporalio/contrib/opentelemetry`.

No dedicated umbrella-trait phase (Decision Q4). If we later add `Interceptor: ClientInterceptor
+ WorkerInterceptor`, that's a backwards-compatible additive change.

---

## 10. Risks

- **`WorkflowService` mock migration**: handles switching from `CT: WorkflowService` to `CT:
  ClientDispatch` means anything in the tree that stubs `WorkflowService` for handle-based tests
  must migrate to `ClientDispatch`. Survey tests/external usages in phase 4. The migration is
  mostly mechanical: implement higher-level methods instead of raw `WorkflowService` methods.
  Mock impls get *nicer* inputs (`OutboundCall<StartWorkflowInput>` vs. raw
  `StartWorkflowExecutionRequest`).
- **Performance**: one extra `Box<dyn FnOnce>` allocation per interceptor per call on the
  non-empty-chain path. Empty-chain fast path (§5.4) preserves today's cost. Worth benchmarking on
  long-poll-dense workloads (notably list-workflows streaming).
- **Surface creep**: each new logical client operation needs a new `ClientInterceptor` method +
  `ClientDispatch` method + hand-written dispatcher. Same cost Go/Python pay; manageable because
  defaults on `ClientInterceptor` methods make additions non-breaking for existing interceptor
  implementors. `ClientDispatch` remains the sharper edge because external implementors would need
  updates.
- **`ClientDispatch` exposure**: if `ClientDispatch` is public, adding logical operations is a
  breaking change for external implementors. Mitigate by documenting it as unstable/internal, not
  recommending end-user client mocks, and considering a sealed/private replacement once handle
  generics are refactored.

---

## 11. References

- Go client interceptors: `go/interceptor/interceptor.go`, `go/internal/interceptor.go`,
  `go/internal/client.go:1439`.
- Python client interceptors: `python/temporalio/client.py:7743-7972`.
- OpenTelemetry examples: `go/contrib/opentelemetry/tracing_interceptor.go`,
  `python/temporalio/contrib/opentelemetry/_interceptor.py`.
- Rust current state: `crates/client/src/lib.rs`, `crates/client/src/workflow_handle.rs`,
  `crates/client/src/schedules.rs`, `crates/client/src/async_activity_handle.rs`,
  `crates/sdk/src/interceptors.rs`.
