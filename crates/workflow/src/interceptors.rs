//! Workflow interceptor APIs.

use crate::{
    runtime::{
        entry::WorkflowError,
        model::{TimerResult, WorkflowResult},
    },
    workflow_context::{CancellableFuture, TimerOptions, WorkflowContextView},
};
use futures_util::{FutureExt, future::LocalBoxFuture};
use std::{
    any::TypeId,
    collections::HashMap,
    fmt,
    future::Future,
    pin::Pin,
    rc::Rc,
    sync::Arc,
    task::{Context, Poll},
};
use temporalio_common_wasm::{
    data_converters::TemporalSerializable,
    protos::temporal::api::common::v1::{Payload, Payloads},
};

/// Boxed cancellable future used by workflow interceptor operation outputs.
pub struct BoxedCancellableFuture<T> {
    inner: Pin<Box<dyn CancellableFuture<T>>>,
}

impl<T> BoxedCancellableFuture<T> {
    pub(crate) fn new<F>(future: F) -> Self
    where
        F: CancellableFuture<T> + 'static,
    {
        Self {
            inner: Box::pin(future),
        }
    }
}

impl<T> Future for BoxedCancellableFuture<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.inner.poll_unpin(cx)
    }
}

impl<T> futures_util::future::FusedFuture for BoxedCancellableFuture<T> {
    fn is_terminated(&self) -> bool {
        self.inner.is_terminated()
    }
}

impl<T> CancellableFuture<T> for BoxedCancellableFuture<T> {
    fn cancel(&self) {
        self.inner.cancel();
    }
}

/// SDK-owned erased typed value carried through the workflow interceptor chain.
pub struct WorkflowValue {
    type_id: TypeId,
    inner: Box<dyn TemporalSerializable>,
}

impl WorkflowValue {
    pub(crate) fn new<T>(value: T) -> Self
    where
        T: TemporalSerializable + 'static,
    {
        Self {
            type_id: TypeId::of::<T>(),
            inner: Box::new(value),
        }
    }

    /// `TypeId` of the wrapped concrete type.
    pub fn type_id(&self) -> TypeId {
        self.type_id
    }

    /// Returns true if the wrapped value's concrete type is `T`.
    pub fn is<T>(&self) -> bool
    where
        T: TemporalSerializable + 'static,
    {
        self.type_id == TypeId::of::<T>()
    }

    /// Borrow the wrapped value as `&T` if its concrete type matches.
    pub fn downcast_ref<T>(&self) -> Option<&T>
    where
        T: TemporalSerializable + 'static,
    {
        if self.is::<T>() {
            let ptr = self.inner.as_ref() as *const dyn TemporalSerializable as *const T;
            Some(unsafe { &*ptr })
        } else {
            None
        }
    }

    /// Mutably borrow the wrapped value as `&mut T` if its concrete type matches.
    pub fn downcast_mut<T>(&mut self) -> Option<&mut T>
    where
        T: TemporalSerializable + 'static,
    {
        if self.is::<T>() {
            let ptr = self.inner.as_mut() as *mut dyn TemporalSerializable as *mut T;
            Some(unsafe { &mut *ptr })
        } else {
            None
        }
    }

    pub(crate) fn into_typed<T>(self) -> Result<T, Self>
    where
        T: TemporalSerializable + 'static,
    {
        if self.is::<T>() {
            let raw: *mut dyn TemporalSerializable = Box::into_raw(self.inner);
            Ok(*unsafe { Box::from_raw(raw as *mut T) })
        } else {
            Err(self)
        }
    }
}

impl fmt::Debug for WorkflowValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WorkflowValue")
            .field("type_id", &self.type_id)
            .finish_non_exhaustive()
    }
}

/// Output type for workflow execution interception.
pub type WorkflowExecuteOutput = LocalBoxFuture<'static, WorkflowResult<Payload>>;

/// Output type for workflow signal interception.
pub type WorkflowSignalOutput = LocalBoxFuture<'static, Result<(), WorkflowError>>;

/// Output type for workflow query interception.
pub type WorkflowQueryOutput = Result<Payload, WorkflowError>;

/// Output type for workflow timer interception.
pub type SleepOutput = BoxedCancellableFuture<TimerResult>;

/// Continuation for an interceptor operation.
#[must_use = "workflow interceptor continuations must be run to continue the call chain"]
pub struct Next<'a, I, O> {
    inner: Box<dyn FnOnce(I) -> O + 'a>,
}

impl<'a, I, O> Next<'a, I, O> {
    #[doc(hidden)]
    pub fn new(f: impl FnOnce(I) -> O + 'a) -> Self {
        Self { inner: Box::new(f) }
    }

    /// Continue the call chain with the provided input.
    #[must_use = "the returned workflow interceptor output must be used"]
    pub fn run(self, input: I) -> O {
        (self.inner)(input)
    }
}

/// Read-only context passed to workflow interceptors.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct WorkflowInterceptorContext {
    /// Read-only workflow metadata.
    pub workflow: WorkflowContextView,
    /// Operation-specific metadata.
    pub operation: WorkflowOperationContext,
}

/// Operation-specific workflow interceptor context.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct WorkflowOperationContext {
    /// Raw replay state for the current activation/task as observed from Core.
    pub is_replaying: bool,
    /// True when this operation is being executed only to replay history events.
    pub is_replaying_history_events: bool,
}

impl WorkflowOperationContext {
    pub(crate) fn new(is_replaying: bool, is_replaying_history_events: bool) -> Self {
        Self {
            is_replaying,
            is_replaying_history_events,
        }
    }
}

/// Input for workflow execution interception.
#[derive(Debug)]
#[non_exhaustive]
pub struct ExecuteInput {
    workflow_type: String,
    args: WorkflowValue,
    headers: HashMap<String, Payload>,
    context: WorkflowInterceptorContext,
}

impl ExecuteInput {
    pub(crate) fn new(
        workflow_type: String,
        args: WorkflowValue,
        headers: HashMap<String, Payload>,
        context: WorkflowInterceptorContext,
    ) -> Self {
        Self {
            workflow_type,
            args,
            headers,
            context,
        }
    }

    /// Workflow type being executed.
    pub fn workflow_type(&self) -> &str {
        &self.workflow_type
    }

    /// Workflow arguments as an erased typed value.
    pub fn args(&self) -> &WorkflowValue {
        &self.args
    }

    /// Mutable access to the workflow arguments.
    pub fn args_mut(&mut self) -> &mut WorkflowValue {
        &mut self.args
    }

    /// Workflow headers.
    pub fn headers(&self) -> &HashMap<String, Payload> {
        &self.headers
    }

    /// Read-only workflow interceptor context.
    pub fn context(&self) -> &WorkflowInterceptorContext {
        &self.context
    }

    pub(crate) fn into_args(self) -> WorkflowValue {
        self.args
    }
}

/// Input for workflow signal interception.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct HandleSignalInput {
    signal_name: String,
    args: Vec<Payload>,
    headers: HashMap<String, Payload>,
    context: WorkflowInterceptorContext,
}

impl HandleSignalInput {
    pub(crate) fn new(
        signal_name: String,
        args: Vec<Payload>,
        headers: HashMap<String, Payload>,
        context: WorkflowInterceptorContext,
    ) -> Self {
        Self {
            signal_name,
            args,
            headers,
            context,
        }
    }

    /// Signal name being handled.
    pub fn signal_name(&self) -> &str {
        &self.signal_name
    }

    /// Serialized signal arguments.
    pub fn args(&self) -> &[Payload] {
        &self.args
    }

    /// Signal headers.
    pub fn headers(&self) -> &HashMap<String, Payload> {
        &self.headers
    }

    /// Read-only workflow interceptor context.
    pub fn context(&self) -> &WorkflowInterceptorContext {
        &self.context
    }

    pub(crate) fn into_parts(self) -> (String, Payloads, HashMap<String, Payload>) {
        (
            self.signal_name,
            Payloads {
                payloads: self.args,
            },
            self.headers,
        )
    }
}

/// Input for workflow query interception.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct HandleQueryInput {
    query_name: String,
    args: Vec<Payload>,
    headers: HashMap<String, Payload>,
    context: WorkflowInterceptorContext,
}

impl HandleQueryInput {
    pub(crate) fn new(
        query_name: String,
        args: Vec<Payload>,
        headers: HashMap<String, Payload>,
        context: WorkflowInterceptorContext,
    ) -> Self {
        Self {
            query_name,
            args,
            headers,
            context,
        }
    }

    /// Query name being handled.
    pub fn query_name(&self) -> &str {
        &self.query_name
    }

    /// Serialized query arguments.
    pub fn args(&self) -> &[Payload] {
        &self.args
    }

    /// Query headers.
    pub fn headers(&self) -> &HashMap<String, Payload> {
        &self.headers
    }

    /// Read-only workflow interceptor context.
    pub fn context(&self) -> &WorkflowInterceptorContext {
        &self.context
    }

    pub(crate) fn into_parts(self) -> (String, Payloads, HashMap<String, Payload>) {
        (
            self.query_name,
            Payloads {
                payloads: self.args,
            },
            self.headers,
        )
    }
}

/// Input for workflow timer interception.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct SleepInput {
    options: TimerOptions,
    context: WorkflowInterceptorContext,
}

impl SleepInput {
    pub(crate) fn new(options: TimerOptions, context: WorkflowInterceptorContext) -> Self {
        Self { options, context }
    }

    /// Timer duration.
    pub fn duration(&self) -> std::time::Duration {
        self.options.duration
    }

    /// Timer summary.
    pub fn summary(&self) -> Option<&str> {
        self.options.summary.as_deref()
    }

    /// Read-only workflow interceptor context.
    pub fn context(&self) -> &WorkflowInterceptorContext {
        &self.context
    }

    /// Return a copy of this input with a different timer duration.
    pub fn with_duration(mut self, duration: std::time::Duration) -> Self {
        self.options.duration = duration;
        self
    }

    /// Return a copy of this input with a different timer summary.
    pub fn with_summary(mut self, summary: impl Into<Option<String>>) -> Self {
        self.options.summary = summary.into();
        self
    }

    pub(crate) fn into_options(self) -> TimerOptions {
        self.options
    }
}

/// Factory trait for workflow interceptors.
pub trait WorkflowInterceptor: Send + Sync + 'static {
    /// Build interceptors for a workflow instance.
    fn intercept_workflow(&self, ctx: WorkflowInterceptorContext) -> WorkflowInterceptors;
}

/// Inbound and outbound interceptors for one workflow instance.
pub struct WorkflowInterceptors {
    /// Inbound workflow interceptor.
    pub inbound: Box<dyn WorkflowInboundInterceptor>,
    /// Outbound workflow interceptor.
    pub outbound: Box<dyn WorkflowOutboundInterceptor>,
}

impl Default for WorkflowInterceptors {
    fn default() -> Self {
        Self {
            inbound: Box::new(NoopWorkflowInboundInterceptor),
            outbound: Box::new(NoopWorkflowOutboundInterceptor),
        }
    }
}

/// Inbound workflow interceptor hooks.
pub trait WorkflowInboundInterceptor: Send + Sync + 'static {
    /// Intercept workflow execution.
    fn execute<'a>(
        &'a self,
        input: ExecuteInput,
        next: Next<'a, ExecuteInput, WorkflowExecuteOutput>,
    ) -> WorkflowExecuteOutput {
        next.run(input)
    }

    /// Intercept workflow signal handling.
    fn handle_signal<'a>(
        &'a self,
        input: HandleSignalInput,
        next: Next<'a, HandleSignalInput, WorkflowSignalOutput>,
    ) -> WorkflowSignalOutput {
        next.run(input)
    }

    /// Intercept workflow query handling.
    fn handle_query<'a>(
        &'a self,
        input: HandleQueryInput,
        next: Next<'a, HandleQueryInput, WorkflowQueryOutput>,
    ) -> WorkflowQueryOutput {
        next.run(input)
    }
}

/// Outbound workflow interceptor hooks.
pub trait WorkflowOutboundInterceptor: Send + Sync + 'static {
    /// Intercept workflow timer creation.
    fn sleep<'a>(
        &'a self,
        input: SleepInput,
        next: Next<'a, SleepInput, SleepOutput>,
    ) -> SleepOutput {
        next.run(input)
    }
}

struct NoopWorkflowInboundInterceptor;

impl WorkflowInboundInterceptor for NoopWorkflowInboundInterceptor {}

struct NoopWorkflowOutboundInterceptor;

impl WorkflowOutboundInterceptor for NoopWorkflowOutboundInterceptor {}

#[derive(Clone, Default)]
pub(crate) struct WorkflowInterceptorInstance {
    inbound: Rc<Vec<Rc<dyn WorkflowInboundInterceptor>>>,
    outbound: Rc<Vec<Rc<dyn WorkflowOutboundInterceptor>>>,
}

impl WorkflowInterceptorInstance {
    pub(crate) fn new(
        interceptors: &[Arc<dyn WorkflowInterceptor>],
        ctx: WorkflowInterceptorContext,
    ) -> Self {
        let mut inbound = Vec::with_capacity(interceptors.len());
        let mut outbound = Vec::with_capacity(interceptors.len());
        for interceptor in interceptors {
            let WorkflowInterceptors {
                inbound: next_inbound,
                outbound: next_outbound,
            } = interceptor.intercept_workflow(ctx.clone());
            inbound.push(Rc::from(next_inbound));
            outbound.push(Rc::from(next_outbound));
        }
        Self {
            inbound: Rc::new(inbound),
            outbound: Rc::new(outbound),
        }
    }

    pub(crate) fn execute(
        &self,
        input: ExecuteInput,
        next: impl FnOnce(ExecuteInput) -> WorkflowExecuteOutput,
    ) -> WorkflowExecuteOutput {
        call_execute(&self.inbound, input, Next::new(next))
    }

    pub(crate) fn handle_signal(
        &self,
        input: HandleSignalInput,
        next: impl FnOnce(HandleSignalInput) -> WorkflowSignalOutput,
    ) -> WorkflowSignalOutput {
        call_handle_signal(&self.inbound, input, Next::new(next))
    }

    pub(crate) fn handle_query(
        &self,
        input: HandleQueryInput,
        next: impl FnOnce(HandleQueryInput) -> WorkflowQueryOutput,
    ) -> WorkflowQueryOutput {
        call_handle_query(&self.inbound, input, Next::new(next))
    }

    pub(crate) fn sleep(
        &self,
        input: SleepInput,
        next: impl FnOnce(SleepInput) -> SleepOutput,
    ) -> SleepOutput {
        call_sleep(&self.outbound, input, Next::new(next))
    }
}

macro_rules! workflow_interceptor_call {
    ($call_fn:ident, $interceptor_trait:ident, $method:ident, $input:ty, $output:ty) => {
        fn $call_fn<'a>(
            interceptors: &'a [Rc<dyn $interceptor_trait>],
            input: $input,
            next: Next<'a, $input, $output>,
        ) -> $output {
            if let Some((first, rest)) = interceptors.split_first() {
                first.$method(input, Next::new(move |input| $call_fn(rest, input, next)))
            } else {
                next.run(input)
            }
        }
    };
}

workflow_interceptor_call!(
    call_execute,
    WorkflowInboundInterceptor,
    execute,
    ExecuteInput,
    WorkflowExecuteOutput
);
workflow_interceptor_call!(
    call_handle_signal,
    WorkflowInboundInterceptor,
    handle_signal,
    HandleSignalInput,
    WorkflowSignalOutput
);
workflow_interceptor_call!(
    call_handle_query,
    WorkflowInboundInterceptor,
    handle_query,
    HandleQueryInput,
    WorkflowQueryOutput
);
workflow_interceptor_call!(
    call_sleep,
    WorkflowOutboundInterceptor,
    sleep,
    SleepInput,
    SleepOutput
);
