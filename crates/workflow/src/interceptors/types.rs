//! Input and Output types for Workflow interceptor APIs.
use std::{
    any::Any,
    collections::HashMap,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use futures_util::future::{FutureExt, LocalBoxFuture};
use temporalio_common_wasm::{
    data_converters::PayloadConversionError,
    error::{ActivityExecutionError, ChildWorkflowStartError},
    protos::{
        coresdk::{
            activity_result::ActivityResolution, common::NamespacedWorkflowExecution,
            workflow_commands::signal_external_workflow_execution,
        },
        temporal::api::common::v1::Payload,
    },
};

use super::WorkflowInterceptorContext;
pub use crate::workflow_context::StartChildWorkflowExecutionResult;
use crate::{
    ActivityOptions, CancellableFuture, CancellableFutureWithReason, ChildWorkflowOptions,
    LocalActivityOptions, TimerOptions, TimerResult, WorkflowResult,
    runtime::model::SignalExternalWfResult, workflows::WorkflowError,
};

/// Boxed cancellable future used by workflow interceptor operation outputs.
pub struct BoxedCancellableFuture<T> {
    inner: Pin<Box<dyn CancellableFuture<T>>>,
}

impl<T> BoxedCancellableFuture<T> {
    /// Box a cancellable future for use as a workflow interceptor output.
    pub fn new<F>(future: F) -> Self
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

/// Boxed cancellable future used by workflow interceptor operation outputs that support a
/// cancellation reason.
pub struct BoxedCancellableFutureWithReason<T> {
    inner: Pin<Box<dyn CancellableFutureWithReason<T>>>,
}

impl<T> BoxedCancellableFutureWithReason<T> {
    /// Box a cancellable future for use as a workflow interceptor output.
    pub fn new<F>(future: F) -> Self
    where
        F: CancellableFutureWithReason<T> + 'static,
    {
        Self {
            inner: Box::pin(future),
        }
    }
}

impl<T> Future for BoxedCancellableFutureWithReason<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.inner.poll_unpin(cx)
    }
}

impl<T> futures_util::future::FusedFuture for BoxedCancellableFutureWithReason<T> {
    fn is_terminated(&self) -> bool {
        self.inner.is_terminated()
    }
}

impl<T> CancellableFuture<T> for BoxedCancellableFutureWithReason<T> {
    fn cancel(&self) {
        self.inner.cancel();
    }
}

impl<T> CancellableFutureWithReason<T> for BoxedCancellableFutureWithReason<T> {
    fn cancel_with_reason(&self, reason: String) {
        self.inner.cancel_with_reason(reason);
    }
}

/// Output type for workflow execution interception.
pub type ExecuteOutput = LocalBoxFuture<'static, WorkflowResult<Payload>>;

/// Output type for workflow signal interception.
pub type HandleSignalOutput = LocalBoxFuture<'static, Result<(), WorkflowError>>;

/// Output type for workflow query interception.
pub type HandleQueryOutput = Result<Payload, WorkflowError>;

/// Output type for workflow update validation interception.
pub type ValidateUpdateOutput = Option<Result<(), WorkflowError>>;

/// Output type for workflow update handling interception.
pub type HandleUpdateOutput = Option<LocalBoxFuture<'static, Result<Payload, WorkflowError>>>;

/// Output type for workflow timer interception.
pub type SleepOutput = BoxedCancellableFuture<TimerResult>;

/// Output type for workflow activity scheduling interception.
pub type ScheduleActivityOutput =
    Result<BoxedCancellableFuture<ActivityResolution>, Box<ActivityExecutionError>>;

/// Output type for workflow local activity scheduling interception.
pub type ScheduleLocalActivityOutput =
    Result<BoxedCancellableFuture<ActivityResolution>, Box<ActivityExecutionError>>;

/// Output type for workflow signal scheduling interception.
pub type SignalWorkflowOutput =
    Result<BoxedCancellableFuture<SignalExternalWfResult>, PayloadConversionError>;

/// Output type for workflow child workflow start interception.
pub type StartChildWorkflowExecutionOutput = Result<
    BoxedCancellableFutureWithReason<StartChildWorkflowExecutionResult>,
    ChildWorkflowStartError,
>;

macro_rules! impl_any_args_accessors {
    ($ty:ty) => {
        impl $ty {
            /// Attempt to access decoded arguments as a concrete type.
            pub fn args_ref<T: Any>(&self) -> Option<&T> {
                self.args.downcast_ref()
            }

            /// Attempt to mutably access decoded arguments as a concrete type.
            pub fn args_mut<T: Any>(&mut self) -> Option<&mut T> {
                self.args.downcast_mut()
            }
        }
    };
}

macro_rules! impl_headers_accessors {
    ($ty:ty) => {
        impl $ty {
            /// Headers attached to this interceptor input.
            pub fn headers(&self) -> &HashMap<String, Payload> {
                &self.headers
            }

            /// Mutably access headers attached to this interceptor input.
            pub fn headers_mut(&mut self) -> &mut HashMap<String, Payload> {
                &mut self.headers
            }
        }
    };
}

/// Input for workflow execution interception.
#[derive(derive_more::Debug)]
#[non_exhaustive]
pub struct ExecuteInput {
    workflow_type: String,
    #[debug(skip)]
    args: Box<dyn Any>,
    headers: HashMap<String, Payload>,
    context: WorkflowInterceptorContext,
}

impl ExecuteInput {
    pub(crate) fn new<T>(
        workflow_type: String,
        args: T,
        headers: HashMap<String, Payload>,
        context: WorkflowInterceptorContext,
    ) -> Self
    where
        T: Any,
    {
        Self {
            workflow_type,
            args: Box::new(args),
            headers,
            context,
        }
    }

    /// Workflow type being executed.
    pub fn workflow_type(&self) -> &str {
        &self.workflow_type
    }

    /// Read-only workflow interceptor context.
    pub fn context(&self) -> &WorkflowInterceptorContext {
        &self.context
    }

    pub(crate) fn into_args<T: Any>(self) -> Result<T, Box<Self>> {
        let Self {
            workflow_type,
            args,
            headers,
            context,
        } = self;
        match args.downcast::<T>() {
            Ok(args) => Ok(*args),
            Err(args) => Err(Box::new(Self {
                workflow_type,
                args,
                headers,
                context,
            })),
        }
    }
}
impl_any_args_accessors!(ExecuteInput);
impl_headers_accessors!(ExecuteInput);

/// Input for workflow signal interception.
#[derive(derive_more::Debug)]
#[non_exhaustive]
pub struct HandleSignalInput {
    signal_name: String,
    #[debug(skip)]
    args: Box<dyn Any>,
    headers: HashMap<String, Payload>,
    context: WorkflowInterceptorContext,
}

impl HandleSignalInput {
    pub(crate) fn new(
        signal_name: String,
        args: Box<dyn Any>,
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

    /// Read-only workflow interceptor context.
    pub fn context(&self) -> &WorkflowInterceptorContext {
        &self.context
    }

    pub(crate) fn into_parts(self) -> (String, Box<dyn Any>, HashMap<String, Payload>) {
        (self.signal_name, self.args, self.headers)
    }
}
impl_any_args_accessors!(HandleSignalInput);
impl_headers_accessors!(HandleSignalInput);

/// Input for workflow query interception.
#[derive(derive_more::Debug)]
#[non_exhaustive]
pub struct HandleQueryInput {
    query_name: String,
    #[debug(skip)]
    args: Box<dyn Any>,
    headers: HashMap<String, Payload>,
    context: WorkflowInterceptorContext,
}

impl HandleQueryInput {
    pub(crate) fn new(
        query_name: String,
        args: Box<dyn Any>,
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

    /// Read-only workflow interceptor context.
    pub fn context(&self) -> &WorkflowInterceptorContext {
        &self.context
    }

    pub(crate) fn into_parts(self) -> (String, Box<dyn Any>, HashMap<String, Payload>) {
        (self.query_name, self.args, self.headers)
    }
}
impl_any_args_accessors!(HandleQueryInput);
impl_headers_accessors!(HandleQueryInput);

/// Input for workflow update validation interception.
#[derive(derive_more::Debug)]
#[non_exhaustive]
pub struct ValidateUpdateInput {
    update_name: String,
    #[debug(skip)]
    args: Box<dyn Any>,
    headers: HashMap<String, Payload>,
    context: WorkflowInterceptorContext,
}

impl ValidateUpdateInput {
    pub(crate) fn new(
        update_name: String,
        args: Box<dyn Any>,
        headers: HashMap<String, Payload>,
        context: WorkflowInterceptorContext,
    ) -> Self {
        Self {
            update_name,
            args,
            headers,
            context,
        }
    }

    /// Update name being validated.
    pub fn update_name(&self) -> &str {
        &self.update_name
    }

    /// Read-only workflow interceptor context.
    pub fn context(&self) -> &WorkflowInterceptorContext {
        &self.context
    }

    pub(crate) fn into_parts(self) -> (String, Box<dyn Any>, HashMap<String, Payload>) {
        (self.update_name, self.args, self.headers)
    }
}
impl_any_args_accessors!(ValidateUpdateInput);
impl_headers_accessors!(ValidateUpdateInput);

/// Input for workflow update handling interception.
#[derive(derive_more::Debug)]
#[non_exhaustive]
pub struct HandleUpdateInput {
    update_name: String,
    #[debug(skip)]
    args: Box<dyn Any>,
    headers: HashMap<String, Payload>,
    context: WorkflowInterceptorContext,
}

impl HandleUpdateInput {
    pub(crate) fn new(
        update_name: String,
        args: Box<dyn Any>,
        headers: HashMap<String, Payload>,
        context: WorkflowInterceptorContext,
    ) -> Self {
        Self {
            update_name,
            args,
            headers,
            context,
        }
    }

    /// Update name being handled.
    pub fn update_name(&self) -> &str {
        &self.update_name
    }

    /// Read-only workflow interceptor context.
    pub fn context(&self) -> &WorkflowInterceptorContext {
        &self.context
    }

    pub(crate) fn into_parts(self) -> (String, Box<dyn Any>, HashMap<String, Payload>) {
        (self.update_name, self.args, self.headers)
    }
}
impl_any_args_accessors!(HandleUpdateInput);
impl_headers_accessors!(HandleUpdateInput);

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

/// Target workflow for a workflow signal command.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum SignalWorkflowTarget {
    /// Signal a known child workflow by workflow id.
    ChildWorkflow {
        /// Workflow id of the child workflow.
        workflow_id: String,
    },
    /// Signal an external workflow execution.
    ExternalWorkflow {
        /// Namespace containing the target workflow.
        namespace: String,
        /// Workflow id of the target workflow.
        workflow_id: String,
        /// Run id of the target workflow, or `None` to target the latest run.
        run_id: Option<String>,
    },
}

impl SignalWorkflowTarget {
    pub(crate) fn into_proto(self) -> signal_external_workflow_execution::Target {
        match self {
            Self::ChildWorkflow { workflow_id } => {
                signal_external_workflow_execution::Target::ChildWorkflowId(workflow_id)
            }
            Self::ExternalWorkflow {
                namespace,
                workflow_id,
                run_id,
            } => signal_external_workflow_execution::Target::WorkflowExecution(
                NamespacedWorkflowExecution {
                    namespace,
                    workflow_id,
                    run_id: run_id.unwrap_or_default(),
                },
            ),
        }
    }
}

type SignalWorkflowParts<T> = (SignalWorkflowTarget, String, T, HashMap<String, Payload>);

/// Input for workflow activity scheduling interception.
#[derive(derive_more::Debug)]
#[non_exhaustive]
pub struct ScheduleActivityInput {
    activity_type: String,
    #[debug(skip)]
    args: Box<dyn Any>,
    options: ActivityOptions,
    context: WorkflowInterceptorContext,
}

impl ScheduleActivityInput {
    pub(crate) fn new<T>(
        activity_type: String,
        args: T,
        options: ActivityOptions,
        context: WorkflowInterceptorContext,
    ) -> Self
    where
        T: Any,
    {
        Self {
            activity_type,
            args: Box::new(args),
            options,
            context,
        }
    }

    /// Activity type being scheduled.
    pub fn activity_type(&self) -> &str {
        &self.activity_type
    }

    /// Mutably access the activity type being scheduled.
    pub fn activity_type_mut(&mut self) -> &mut String {
        &mut self.activity_type
    }

    /// Activity scheduling options.
    pub fn options(&self) -> &ActivityOptions {
        &self.options
    }

    /// Mutably access activity scheduling options.
    pub fn options_mut(&mut self) -> &mut ActivityOptions {
        &mut self.options
    }

    /// Read-only workflow interceptor context.
    pub fn context(&self) -> &WorkflowInterceptorContext {
        &self.context
    }

    pub(crate) fn into_parts<T: Any>(self) -> Result<(String, T, ActivityOptions), Box<Self>> {
        let Self {
            activity_type,
            args,
            options,
            context,
        } = self;
        match args.downcast::<T>() {
            Ok(args) => Ok((activity_type, *args, options)),
            Err(args) => Err(Box::new(Self {
                activity_type,
                args,
                options,
                context,
            })),
        }
    }
}
impl_any_args_accessors!(ScheduleActivityInput);

/// Input for workflow local activity scheduling interception.
#[derive(derive_more::Debug)]
#[non_exhaustive]
pub struct ScheduleLocalActivityInput {
    activity_type: String,
    #[debug(skip)]
    args: Box<dyn Any>,
    options: LocalActivityOptions,
    context: WorkflowInterceptorContext,
}

impl ScheduleLocalActivityInput {
    pub(crate) fn new<T>(
        activity_type: String,
        args: T,
        options: LocalActivityOptions,
        context: WorkflowInterceptorContext,
    ) -> Self
    where
        T: Any,
    {
        Self {
            activity_type,
            args: Box::new(args),
            options,
            context,
        }
    }

    /// Activity type being scheduled.
    pub fn activity_type(&self) -> &str {
        &self.activity_type
    }

    /// Mutably access the activity type being scheduled.
    pub fn activity_type_mut(&mut self) -> &mut String {
        &mut self.activity_type
    }

    /// Local activity scheduling options.
    pub fn options(&self) -> &LocalActivityOptions {
        &self.options
    }

    /// Mutably access local activity scheduling options.
    pub fn options_mut(&mut self) -> &mut LocalActivityOptions {
        &mut self.options
    }

    /// Read-only workflow interceptor context.
    pub fn context(&self) -> &WorkflowInterceptorContext {
        &self.context
    }

    pub(crate) fn into_parts<T: Any>(self) -> Result<(String, T, LocalActivityOptions), Box<Self>> {
        let Self {
            activity_type,
            args,
            options,
            context,
        } = self;
        match args.downcast::<T>() {
            Ok(args) => Ok((activity_type, *args, options)),
            Err(args) => Err(Box::new(Self {
                activity_type,
                args,
                options,
                context,
            })),
        }
    }
}
impl_any_args_accessors!(ScheduleLocalActivityInput);

/// Input for workflow signal scheduling interception.
#[derive(derive_more::Debug)]
#[non_exhaustive]
pub struct SignalWorkflowInput {
    target: SignalWorkflowTarget,
    signal_name: String,
    #[debug(skip)]
    args: Box<dyn Any>,
    headers: HashMap<String, Payload>,
    context: WorkflowInterceptorContext,
}

impl SignalWorkflowInput {
    pub(crate) fn new<T>(
        target: SignalWorkflowTarget,
        signal_name: String,
        args: T,
        headers: HashMap<String, Payload>,
        context: WorkflowInterceptorContext,
    ) -> Self
    where
        T: Any,
    {
        Self {
            target,
            signal_name,
            args: Box::new(args),
            headers,
            context,
        }
    }

    /// Target workflow for this signal.
    pub fn target(&self) -> &SignalWorkflowTarget {
        &self.target
    }

    /// Mutably access the target workflow for this signal.
    pub fn target_mut(&mut self) -> &mut SignalWorkflowTarget {
        &mut self.target
    }

    /// Signal name being sent.
    pub fn signal_name(&self) -> &str {
        &self.signal_name
    }

    /// Mutably access the signal name being sent.
    pub fn signal_name_mut(&mut self) -> &mut String {
        &mut self.signal_name
    }

    /// Read-only workflow interceptor context.
    pub fn context(&self) -> &WorkflowInterceptorContext {
        &self.context
    }

    pub(crate) fn into_parts<T: Any>(self) -> Result<SignalWorkflowParts<T>, Box<Self>> {
        let Self {
            target,
            signal_name,
            args,
            headers,
            context,
        } = self;
        match args.downcast::<T>() {
            Ok(args) => Ok((target, signal_name, *args, headers)),
            Err(args) => Err(Box::new(Self {
                target,
                signal_name,
                args,
                headers,
                context,
            })),
        }
    }
}
impl_any_args_accessors!(SignalWorkflowInput);
impl_headers_accessors!(SignalWorkflowInput);

/// Input for workflow child workflow start interception.
#[derive(derive_more::Debug)]
#[non_exhaustive]
pub struct StartChildWorkflowExecutionInput {
    workflow_type: String,
    #[debug(skip)]
    args: Box<dyn Any>,
    options: ChildWorkflowOptions,
    context: WorkflowInterceptorContext,
}

impl StartChildWorkflowExecutionInput {
    pub(crate) fn new<T>(
        workflow_type: String,
        args: T,
        options: ChildWorkflowOptions,
        context: WorkflowInterceptorContext,
    ) -> Self
    where
        T: Any,
    {
        Self {
            workflow_type,
            args: Box::new(args),
            options,
            context,
        }
    }

    /// Workflow type being started.
    pub fn workflow_type(&self) -> &str {
        &self.workflow_type
    }

    /// Mutably access the workflow type being started.
    pub fn workflow_type_mut(&mut self) -> &mut String {
        &mut self.workflow_type
    }

    /// Child workflow start options.
    pub fn options(&self) -> &ChildWorkflowOptions {
        &self.options
    }

    /// Mutably access child workflow start options.
    pub fn options_mut(&mut self) -> &mut ChildWorkflowOptions {
        &mut self.options
    }

    /// Read-only workflow interceptor context.
    pub fn context(&self) -> &WorkflowInterceptorContext {
        &self.context
    }

    pub(crate) fn into_parts<T: Any>(self) -> Result<(String, T, ChildWorkflowOptions), Box<Self>> {
        let Self {
            workflow_type,
            args,
            options,
            context,
        } = self;
        match args.downcast::<T>() {
            Ok(args) => Ok((workflow_type, *args, options)),
            Err(args) => Err(Box::new(Self {
                workflow_type,
                args,
                options,
                context,
            })),
        }
    }
}
impl_any_args_accessors!(StartChildWorkflowExecutionInput);
