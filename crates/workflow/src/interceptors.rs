//! Workflow interceptor APIs.

mod types;
use crate::workflow_context::WorkflowContextView;
use std::{rc::Rc, sync::Arc};
pub use types::*;

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
        next: Next<'a, ExecuteInput, ExecuteOutput>,
    ) -> ExecuteOutput {
        next.run(input)
    }

    /// Intercept workflow signal handling.
    fn handle_signal<'a>(
        &'a self,
        input: HandleSignalInput,
        next: Next<'a, HandleSignalInput, HandleSignalOutput>,
    ) -> HandleSignalOutput {
        next.run(input)
    }

    /// Intercept workflow query handling.
    fn handle_query<'a>(
        &'a self,
        input: HandleQueryInput,
        next: Next<'a, HandleQueryInput, HandleQueryOutput>,
    ) -> HandleQueryOutput {
        next.run(input)
    }

    /// Intercept workflow update validation.
    fn validate_update<'a>(
        &'a self,
        input: ValidateUpdateInput,
        next: Next<'a, ValidateUpdateInput, ValidateUpdateOutput>,
    ) -> ValidateUpdateOutput {
        next.run(input)
    }

    /// Intercept workflow update handling.
    fn handle_update<'a>(
        &'a self,
        input: HandleUpdateInput,
        next: Next<'a, HandleUpdateInput, HandleUpdateOutput>,
    ) -> HandleUpdateOutput {
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

    /// Intercept activity scheduling.
    fn schedule_activity<'a>(
        &'a self,
        input: ScheduleActivityInput,
        next: Next<'a, ScheduleActivityInput, ScheduleActivityOutput>,
    ) -> ScheduleActivityOutput {
        next.run(input)
    }

    /// Intercept local activity scheduling.
    fn schedule_local_activity<'a>(
        &'a self,
        input: ScheduleLocalActivityInput,
        next: Next<'a, ScheduleLocalActivityInput, ScheduleLocalActivityOutput>,
    ) -> ScheduleLocalActivityOutput {
        next.run(input)
    }

    /// Intercept workflow signal scheduling.
    fn signal_workflow<'a>(
        &'a self,
        input: SignalWorkflowInput,
        next: Next<'a, SignalWorkflowInput, SignalWorkflowOutput>,
    ) -> SignalWorkflowOutput {
        next.run(input)
    }

    /// Intercept child workflow start scheduling.
    fn start_child_workflow_execution<'a>(
        &'a self,
        input: StartChildWorkflowExecutionInput,
        next: Next<'a, StartChildWorkflowExecutionInput, StartChildWorkflowExecutionOutput>,
    ) -> StartChildWorkflowExecutionOutput {
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
        next: impl FnOnce(ExecuteInput) -> ExecuteOutput,
    ) -> ExecuteOutput {
        call_execute(&self.inbound, input, Next::new(next))
    }

    pub(crate) fn handle_signal(
        &self,
        input: HandleSignalInput,
        next: impl FnOnce(HandleSignalInput) -> HandleSignalOutput,
    ) -> HandleSignalOutput {
        call_handle_signal(&self.inbound, input, Next::new(next))
    }

    pub(crate) fn handle_query(
        &self,
        input: HandleQueryInput,
        next: impl FnOnce(HandleQueryInput) -> HandleQueryOutput,
    ) -> HandleQueryOutput {
        call_handle_query(&self.inbound, input, Next::new(next))
    }

    pub(crate) fn validate_update(
        &self,
        input: ValidateUpdateInput,
        next: impl FnOnce(ValidateUpdateInput) -> ValidateUpdateOutput,
    ) -> ValidateUpdateOutput {
        call_validate_update(&self.inbound, input, Next::new(next))
    }

    pub(crate) fn handle_update(
        &self,
        input: HandleUpdateInput,
        next: impl FnOnce(HandleUpdateInput) -> HandleUpdateOutput,
    ) -> HandleUpdateOutput {
        call_handle_update(&self.inbound, input, Next::new(next))
    }

    pub(crate) fn sleep(
        &self,
        input: SleepInput,
        next: impl FnOnce(SleepInput) -> SleepOutput,
    ) -> SleepOutput {
        call_sleep(&self.outbound, input, Next::new(next))
    }

    pub(crate) fn schedule_activity(
        &self,
        input: ScheduleActivityInput,
        next: impl FnOnce(ScheduleActivityInput) -> ScheduleActivityOutput,
    ) -> ScheduleActivityOutput {
        call_schedule_activity(&self.outbound, input, Next::new(next))
    }

    pub(crate) fn schedule_local_activity(
        &self,
        input: ScheduleLocalActivityInput,
        next: impl FnOnce(ScheduleLocalActivityInput) -> ScheduleLocalActivityOutput,
    ) -> ScheduleLocalActivityOutput {
        call_schedule_local_activity(&self.outbound, input, Next::new(next))
    }

    pub(crate) fn signal_workflow(
        &self,
        input: SignalWorkflowInput,
        next: impl FnOnce(SignalWorkflowInput) -> SignalWorkflowOutput,
    ) -> SignalWorkflowOutput {
        call_signal_workflow(&self.outbound, input, Next::new(next))
    }

    pub(crate) fn start_child_workflow_execution(
        &self,
        input: StartChildWorkflowExecutionInput,
        next: impl FnOnce(StartChildWorkflowExecutionInput) -> StartChildWorkflowExecutionOutput,
    ) -> StartChildWorkflowExecutionOutput {
        call_start_child_workflow_execution(&self.outbound, input, Next::new(next))
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
    ExecuteOutput
);
workflow_interceptor_call!(
    call_handle_signal,
    WorkflowInboundInterceptor,
    handle_signal,
    HandleSignalInput,
    HandleSignalOutput
);
workflow_interceptor_call!(
    call_handle_query,
    WorkflowInboundInterceptor,
    handle_query,
    HandleQueryInput,
    HandleQueryOutput
);
workflow_interceptor_call!(
    call_validate_update,
    WorkflowInboundInterceptor,
    validate_update,
    ValidateUpdateInput,
    ValidateUpdateOutput
);
workflow_interceptor_call!(
    call_handle_update,
    WorkflowInboundInterceptor,
    handle_update,
    HandleUpdateInput,
    HandleUpdateOutput
);
workflow_interceptor_call!(
    call_sleep,
    WorkflowOutboundInterceptor,
    sleep,
    SleepInput,
    SleepOutput
);
workflow_interceptor_call!(
    call_schedule_activity,
    WorkflowOutboundInterceptor,
    schedule_activity,
    ScheduleActivityInput,
    ScheduleActivityOutput
);
workflow_interceptor_call!(
    call_schedule_local_activity,
    WorkflowOutboundInterceptor,
    schedule_local_activity,
    ScheduleLocalActivityInput,
    ScheduleLocalActivityOutput
);
workflow_interceptor_call!(
    call_signal_workflow,
    WorkflowOutboundInterceptor,
    signal_workflow,
    SignalWorkflowInput,
    SignalWorkflowOutput
);
workflow_interceptor_call!(
    call_start_child_workflow_execution,
    WorkflowOutboundInterceptor,
    start_child_workflow_execution,
    StartChildWorkflowExecutionInput,
    StartChildWorkflowExecutionOutput
);
