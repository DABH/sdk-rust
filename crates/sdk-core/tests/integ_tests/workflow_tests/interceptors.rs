use crate::common::{CoreWfStarter, activity_functions::StdActivities};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use temporalio_client::{
    WorkflowExecuteUpdateOptions, WorkflowQueryOptions, WorkflowSignalOptions, WorkflowStartOptions,
};
use temporalio_common::worker::WorkerTaskTypes;
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, ChildWorkflowOptions, LocalActivityOptions, SyncWorkflowContext, TimerOptions,
    WorkflowContext, WorkflowContextView, WorkflowResult,
    interceptors::workflows::{
        ExecuteInput, ExecuteOutput, HandleQueryInput, HandleQueryOutput, HandleSignalInput,
        HandleSignalOutput, HandleUpdateInput, HandleUpdateOutput, Next, ScheduleActivityInput,
        ScheduleActivityOutput, ScheduleLocalActivityInput, ScheduleLocalActivityOutput,
        SignalWorkflowInput, SignalWorkflowOutput, SignalWorkflowTarget, SleepInput, SleepOutput,
        StartChildWorkflowExecutionInput, StartChildWorkflowExecutionOutput, ValidateUpdateInput,
        ValidateUpdateOutput, WorkflowInboundInterceptor, WorkflowInterceptor,
        WorkflowInterceptorContext, WorkflowInterceptors, WorkflowOutboundInterceptor,
    },
};

type SharedEvents = Arc<Mutex<Vec<InterceptorEvent>>>;

#[derive(Clone, Debug, PartialEq, Eq)]
enum InterceptorEvent {
    Execute {
        workflow_type: String,
        is_replaying: bool,
        is_replaying_history_events: bool,
    },
    Signal {
        signal_name: String,
        is_replaying: bool,
        is_replaying_history_events: bool,
    },
    Query {
        query_name: String,
        is_replaying: bool,
        is_replaying_history_events: bool,
    },
    ValidateUpdate {
        update_name: String,
        is_replaying: bool,
        is_replaying_history_events: bool,
    },
    Update {
        update_name: String,
        is_replaying: bool,
        is_replaying_history_events: bool,
    },
    Sleep {
        duration: Duration,
        summary: Option<String>,
        is_replaying: bool,
        is_replaying_history_events: bool,
    },
    ScheduleActivity {
        activity_type: String,
        input: Option<String>,
        is_replaying: bool,
        is_replaying_history_events: bool,
    },
    ScheduleLocalActivity {
        activity_type: String,
        input: Option<String>,
        is_replaying: bool,
        is_replaying_history_events: bool,
    },
    SignalWorkflow {
        signal_name: String,
        target: SignalWorkflowTarget,
        input: Option<String>,
        is_replaying: bool,
        is_replaying_history_events: bool,
    },
    StartChildWorkflowExecution {
        workflow_type: String,
        input: Option<String>,
        is_replaying: bool,
        is_replaying_history_events: bool,
    },
}

impl InterceptorEvent {
    fn is_replaying_history_events(&self) -> bool {
        match self {
            InterceptorEvent::Execute {
                is_replaying_history_events,
                ..
            }
            | InterceptorEvent::Signal {
                is_replaying_history_events,
                ..
            }
            | InterceptorEvent::Query {
                is_replaying_history_events,
                ..
            }
            | InterceptorEvent::ValidateUpdate {
                is_replaying_history_events,
                ..
            }
            | InterceptorEvent::Update {
                is_replaying_history_events,
                ..
            }
            | InterceptorEvent::Sleep {
                is_replaying_history_events,
                ..
            }
            | InterceptorEvent::ScheduleActivity {
                is_replaying_history_events,
                ..
            }
            | InterceptorEvent::ScheduleLocalActivity {
                is_replaying_history_events,
                ..
            }
            | InterceptorEvent::SignalWorkflow {
                is_replaying_history_events,
                ..
            }
            | InterceptorEvent::StartChildWorkflowExecution {
                is_replaying_history_events,
                ..
            } => *is_replaying_history_events,
        }
    }
}

struct RecordingWorkflowInterceptor {
    events: SharedEvents,
}

impl WorkflowInterceptor for RecordingWorkflowInterceptor {
    fn intercept_workflow(&self, _ctx: WorkflowInterceptorContext) -> WorkflowInterceptors {
        WorkflowInterceptors {
            inbound: Box::new(RecordingInboundInterceptor {
                events: self.events.clone(),
            }),
            outbound: Box::new(RecordingOutboundInterceptor {
                events: self.events.clone(),
            }),
        }
    }
}

struct RecordingInboundInterceptor {
    events: SharedEvents,
}

impl RecordingInboundInterceptor {
    fn record(&self, event: InterceptorEvent) {
        self.events
            .lock()
            .expect("events mutex is not poisoned")
            .push(event);
    }
}

impl WorkflowInboundInterceptor for RecordingInboundInterceptor {
    fn execute<'a>(
        &'a self,
        input: ExecuteInput,
        next: Next<'a, ExecuteInput, ExecuteOutput>,
    ) -> ExecuteOutput {
        self.record(InterceptorEvent::Execute {
            workflow_type: input.workflow_type().to_string(),
            is_replaying: input.context().operation.is_replaying,
            is_replaying_history_events: input.context().operation.is_replaying_history_events,
        });
        next.run(input)
    }

    fn handle_signal<'a>(
        &'a self,
        input: HandleSignalInput,
        next: Next<'a, HandleSignalInput, HandleSignalOutput>,
    ) -> HandleSignalOutput {
        self.record(InterceptorEvent::Signal {
            signal_name: input.signal_name().to_string(),
            is_replaying: input.context().operation.is_replaying,
            is_replaying_history_events: input.context().operation.is_replaying_history_events,
        });
        next.run(input)
    }

    fn handle_query<'a>(
        &'a self,
        input: HandleQueryInput,
        next: Next<'a, HandleQueryInput, HandleQueryOutput>,
    ) -> HandleQueryOutput {
        self.record(InterceptorEvent::Query {
            query_name: input.query_name().to_string(),
            is_replaying: input.context().operation.is_replaying,
            is_replaying_history_events: input.context().operation.is_replaying_history_events,
        });
        next.run(input)
    }

    fn validate_update<'a>(
        &'a self,
        input: ValidateUpdateInput,
        next: Next<'a, ValidateUpdateInput, ValidateUpdateOutput>,
    ) -> ValidateUpdateOutput {
        self.record(InterceptorEvent::ValidateUpdate {
            update_name: input.update_name().to_string(),
            is_replaying: input.context().operation.is_replaying,
            is_replaying_history_events: input.context().operation.is_replaying_history_events,
        });
        next.run(input)
    }

    fn handle_update<'a>(
        &'a self,
        input: HandleUpdateInput,
        next: Next<'a, HandleUpdateInput, HandleUpdateOutput>,
    ) -> HandleUpdateOutput {
        self.record(InterceptorEvent::Update {
            update_name: input.update_name().to_string(),
            is_replaying: input.context().operation.is_replaying,
            is_replaying_history_events: input.context().operation.is_replaying_history_events,
        });
        next.run(input)
    }
}

struct RecordingOutboundInterceptor {
    events: SharedEvents,
}

impl RecordingOutboundInterceptor {
    fn record(&self, event: InterceptorEvent) {
        self.events
            .lock()
            .expect("events mutex is not poisoned")
            .push(event);
    }
}

impl WorkflowOutboundInterceptor for RecordingOutboundInterceptor {
    fn sleep<'a>(
        &'a self,
        input: SleepInput,
        next: Next<'a, SleepInput, SleepOutput>,
    ) -> SleepOutput {
        self.record(InterceptorEvent::Sleep {
            duration: input.duration(),
            summary: input.summary().map(ToString::to_string),
            is_replaying: input.context().operation.is_replaying,
            is_replaying_history_events: input.context().operation.is_replaying_history_events,
        });
        next.run(input)
    }

    fn schedule_activity<'a>(
        &'a self,
        input: ScheduleActivityInput,
        next: Next<'a, ScheduleActivityInput, ScheduleActivityOutput>,
    ) -> ScheduleActivityOutput {
        self.record(InterceptorEvent::ScheduleActivity {
            activity_type: input.activity_type().to_string(),
            input: input.args_ref::<String>().cloned(),
            is_replaying: input.context().operation.is_replaying,
            is_replaying_history_events: input.context().operation.is_replaying_history_events,
        });
        next.run(input)
    }

    fn schedule_local_activity<'a>(
        &'a self,
        input: ScheduleLocalActivityInput,
        next: Next<'a, ScheduleLocalActivityInput, ScheduleLocalActivityOutput>,
    ) -> ScheduleLocalActivityOutput {
        self.record(InterceptorEvent::ScheduleLocalActivity {
            activity_type: input.activity_type().to_string(),
            input: input.args_ref::<String>().cloned(),
            is_replaying: input.context().operation.is_replaying,
            is_replaying_history_events: input.context().operation.is_replaying_history_events,
        });
        next.run(input)
    }

    fn signal_workflow<'a>(
        &'a self,
        input: SignalWorkflowInput,
        next: Next<'a, SignalWorkflowInput, SignalWorkflowOutput>,
    ) -> SignalWorkflowOutput {
        self.record(InterceptorEvent::SignalWorkflow {
            signal_name: input.signal_name().to_string(),
            target: input.target().clone(),
            input: input.args_ref::<String>().cloned(),
            is_replaying: input.context().operation.is_replaying,
            is_replaying_history_events: input.context().operation.is_replaying_history_events,
        });
        next.run(input)
    }

    fn start_child_workflow_execution<'a>(
        &'a self,
        input: StartChildWorkflowExecutionInput,
        next: Next<'a, StartChildWorkflowExecutionInput, StartChildWorkflowExecutionOutput>,
    ) -> StartChildWorkflowExecutionOutput {
        self.record(InterceptorEvent::StartChildWorkflowExecution {
            workflow_type: input.workflow_type().to_string(),
            input: input.args_ref::<String>().cloned(),
            is_replaying: input.context().operation.is_replaying,
            is_replaying_history_events: input.context().operation.is_replaying_history_events,
        });
        next.run(input)
    }
}

#[workflow]
#[derive(Default)]
struct InterceptedWorkflow {
    counter: i32,
}

#[workflow_methods]
impl InterceptedWorkflow {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>, target: i32) -> WorkflowResult<i32> {
        ctx.timer(TimerOptions {
            duration: Duration::from_millis(10),
            summary: Some("intercepted timer".to_string()),
        })
        .await;
        ctx.wait_condition(|s| s.counter >= target).await;
        Ok(ctx.state(|s| s.counter))
    }

    #[signal]
    fn increment(&mut self, _ctx: &mut SyncWorkflowContext<Self>, amount: i32) {
        self.counter += amount;
    }

    #[update_validator(set_counter)]
    fn validate_set_counter(
        &self,
        _ctx: &WorkflowContextView,
        value: &i32,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if *value < 0 {
            Err("counter cannot be negative".into())
        } else {
            Ok(())
        }
    }

    #[update]
    fn set_counter(&mut self, _ctx: &mut SyncWorkflowContext<Self>, value: i32) -> i32 {
        let old = self.counter;
        self.counter = value;
        old
    }

    #[query]
    fn get_counter(&self, _ctx: &WorkflowContextView) -> i32 {
        self.counter
    }
}

#[tokio::test]
async fn workflow_interceptor_records_execute_signal_query_and_sleep() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let wf_name = InterceptedWorkflow::name();
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;
    worker
        .inner_mut()
        .add_workflow_interceptor(RecordingWorkflowInterceptor {
            events: events.clone(),
        });
    worker.register_workflow::<InterceptedWorkflow>().unwrap();

    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            InterceptedWorkflow::run,
            7,
            WorkflowStartOptions::new(
                task_queue.clone(),
                format!("{}_workflow_interceptors", starter.get_task_queue()),
            )
            .build(),
        )
        .await
        .unwrap();

    let interactions = async {
        let counter = handle
            .query(
                InterceptedWorkflow::get_counter,
                (),
                WorkflowQueryOptions::default(),
            )
            .await
            .unwrap();
        assert_eq!(counter, 0);

        handle
            .signal(
                InterceptedWorkflow::increment,
                1,
                WorkflowSignalOptions::default(),
            )
            .await
            .unwrap();

        handle
            .execute_update(
                InterceptedWorkflow::set_counter,
                7,
                WorkflowExecuteUpdateOptions::default(),
            )
            .await
            .unwrap();
    };

    let (_, worker_res) = tokio::join!(interactions, worker.run_until_done());
    worker_res.unwrap();

    let result = handle.get_result(Default::default()).await.unwrap();
    assert_eq!(result, 7);

    let events = events.lock().expect("events mutex is not poisoned").clone();
    assert!(
        events.iter().any(|e| matches!(
            e,
            InterceptorEvent::Execute {
                workflow_type,
                is_replaying: false,
                is_replaying_history_events: false,
            } if workflow_type == wf_name
        )),
        "missing execute event: {events:?}"
    );
    assert!(
        events.iter().any(|e| matches!(
            e,
            InterceptorEvent::Sleep {
                duration,
                summary,
                is_replaying: false,
                is_replaying_history_events: false,
            } if *duration == Duration::from_millis(10)
                && summary.as_deref() == Some("intercepted timer")
        )),
        "missing sleep event: {events:?}"
    );
    assert!(
        events.iter().any(|e| matches!(
            e,
            InterceptorEvent::Query {
                query_name,
                is_replaying: _,
                is_replaying_history_events: false,
            } if query_name == "get_counter"
        )),
        "missing query event: {events:?}"
    );
    assert!(
        events.iter().any(|e| matches!(
            e,
            InterceptorEvent::Signal {
                signal_name,
                is_replaying: false,
                is_replaying_history_events: false,
            } if signal_name == "increment"
        )),
        "missing signal event: {events:?}"
    );
    assert!(
        events.iter().any(|e| matches!(
            e,
            InterceptorEvent::ValidateUpdate {
                update_name,
                is_replaying: false,
                is_replaying_history_events: false,
            } if update_name == "set_counter"
        )),
        "missing update validation event: {events:?}"
    );
    assert!(
        events.iter().any(|e| matches!(
            e,
            InterceptorEvent::Update {
                update_name,
                is_replaying: false,
                is_replaying_history_events: false,
            } if update_name == "set_counter"
        )),
        "missing update event: {events:?}"
    );
    assert!(
        events.iter().all(|e| !e.is_replaying_history_events()),
        "live workflow operations should not be marked as replaying history events: {events:?}"
    );
}

struct MutatingUpdateInterceptor;

impl WorkflowInterceptor for MutatingUpdateInterceptor {
    fn intercept_workflow(&self, _ctx: WorkflowInterceptorContext) -> WorkflowInterceptors {
        WorkflowInterceptors {
            inbound: Box::new(MutatingUpdateInboundInterceptor),
            ..Default::default()
        }
    }
}

struct MutatingUpdateInboundInterceptor;

impl WorkflowInboundInterceptor for MutatingUpdateInboundInterceptor {
    fn validate_update<'a>(
        &'a self,
        mut input: ValidateUpdateInput,
        next: Next<'a, ValidateUpdateInput, ValidateUpdateOutput>,
    ) -> ValidateUpdateOutput {
        *input
            .args_mut::<i32>()
            .expect("update validation input should be decoded as i32") = 9;
        next.run(input)
    }

    fn handle_update<'a>(
        &'a self,
        mut input: HandleUpdateInput,
        next: Next<'a, HandleUpdateInput, HandleUpdateOutput>,
    ) -> HandleUpdateOutput {
        *input
            .args_mut::<i32>()
            .expect("update handler input should be decoded as i32") = 9;
        next.run(input)
    }
}

#[workflow]
#[derive(Default)]
struct MutatedUpdateWorkflow {
    value: i32,
}

#[workflow_methods]
impl MutatedUpdateWorkflow {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>, target: i32) -> WorkflowResult<i32> {
        ctx.wait_condition(|s| s.value >= target).await;
        Ok(ctx.state(|s| s.value))
    }

    #[update_validator(set_value)]
    fn validate_set_value(
        &self,
        _ctx: &WorkflowContextView,
        value: &i32,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if *value < 9 {
            Err("value must be at least 9".into())
        } else {
            Ok(())
        }
    }

    #[update]
    fn set_value(&mut self, _ctx: &mut SyncWorkflowContext<Self>, value: i32) -> i32 {
        self.value = value;
        value
    }
}

#[tokio::test]
async fn update_interceptor_arg_mutation_flows_to_validation_and_handler() {
    let wf_name = MutatedUpdateWorkflow::name();
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;
    worker
        .inner_mut()
        .add_workflow_interceptor(MutatingUpdateInterceptor);
    worker.register_workflow::<MutatedUpdateWorkflow>().unwrap();

    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            MutatedUpdateWorkflow::run,
            9,
            WorkflowStartOptions::new(
                task_queue,
                format!("{}_mutating_update", starter.get_task_queue()),
            )
            .build(),
        )
        .await
        .unwrap();

    let updates = async {
        let value = handle
            .execute_update(
                MutatedUpdateWorkflow::set_value,
                1,
                WorkflowExecuteUpdateOptions::default(),
            )
            .await
            .unwrap();
        assert_eq!(value, 9);
    };

    let (_, worker_res) = tokio::join!(updates, worker.run_until_done());
    worker_res.unwrap();

    let result = handle.get_result(Default::default()).await.unwrap();
    assert_eq!(result, 9);
}

struct MutatingWorkflowOutboundInterceptor {
    events: SharedEvents,
}

impl WorkflowInterceptor for MutatingWorkflowOutboundInterceptor {
    fn intercept_workflow(&self, _ctx: WorkflowInterceptorContext) -> WorkflowInterceptors {
        WorkflowInterceptors {
            outbound: Box::new(MutatingOutboundInterceptor {
                events: self.events.clone(),
            }),
            ..Default::default()
        }
    }
}

struct MutatingOutboundInterceptor {
    events: SharedEvents,
}

impl MutatingOutboundInterceptor {
    fn record(&self, event: InterceptorEvent) {
        self.events
            .lock()
            .expect("events mutex is not poisoned")
            .push(event);
    }
}

impl WorkflowOutboundInterceptor for MutatingOutboundInterceptor {
    fn schedule_activity<'a>(
        &'a self,
        mut input: ScheduleActivityInput,
        next: Next<'a, ScheduleActivityInput, ScheduleActivityOutput>,
    ) -> ScheduleActivityOutput {
        self.record(InterceptorEvent::ScheduleActivity {
            activity_type: input.activity_type().to_string(),
            input: input.args_ref::<String>().cloned(),
            is_replaying: input.context().operation.is_replaying,
            is_replaying_history_events: input.context().operation.is_replaying_history_events,
        });
        if let Some(value) = input.args_mut::<String>()
            && value == "activity-original"
        {
            *value = "activity-mutated".to_string();
        }
        next.run(input)
    }

    fn schedule_local_activity<'a>(
        &'a self,
        mut input: ScheduleLocalActivityInput,
        next: Next<'a, ScheduleLocalActivityInput, ScheduleLocalActivityOutput>,
    ) -> ScheduleLocalActivityOutput {
        self.record(InterceptorEvent::ScheduleLocalActivity {
            activity_type: input.activity_type().to_string(),
            input: input.args_ref::<String>().cloned(),
            is_replaying: input.context().operation.is_replaying,
            is_replaying_history_events: input.context().operation.is_replaying_history_events,
        });
        if let Some(value) = input.args_mut::<String>()
            && value == "local-original"
        {
            *value = "local-mutated".to_string();
        }
        next.run(input)
    }

    fn signal_workflow<'a>(
        &'a self,
        mut input: SignalWorkflowInput,
        next: Next<'a, SignalWorkflowInput, SignalWorkflowOutput>,
    ) -> SignalWorkflowOutput {
        self.record(InterceptorEvent::SignalWorkflow {
            signal_name: input.signal_name().to_string(),
            target: input.target().clone(),
            input: input.args_ref::<String>().cloned(),
            is_replaying: input.context().operation.is_replaying,
            is_replaying_history_events: input.context().operation.is_replaying_history_events,
        });
        if let Some(value) = input.args_mut::<String>()
            && value == "signal-original"
        {
            *value = "signal-mutated".to_string();
        }
        next.run(input)
    }

    fn start_child_workflow_execution<'a>(
        &'a self,
        mut input: StartChildWorkflowExecutionInput,
        next: Next<'a, StartChildWorkflowExecutionInput, StartChildWorkflowExecutionOutput>,
    ) -> StartChildWorkflowExecutionOutput {
        self.record(InterceptorEvent::StartChildWorkflowExecution {
            workflow_type: input.workflow_type().to_string(),
            input: input.args_ref::<String>().cloned(),
            is_replaying: input.context().operation.is_replaying,
            is_replaying_history_events: input.context().operation.is_replaying_history_events,
        });
        if let Some(value) = input.args_mut::<String>()
            && value == "child-original"
        {
            *value = "child-mutated".to_string();
        }
        next.run(input)
    }
}

#[workflow]
#[derive(Default)]
struct OutboundChildWorkflow;

#[workflow_methods]
impl OutboundChildWorkflow {
    #[run]
    async fn run(_ctx: &mut WorkflowContext<Self>, input: String) -> WorkflowResult<String> {
        Ok(input)
    }
}

#[workflow]
#[derive(Default)]
struct OutboundSignalTargetWorkflow {
    received: Option<String>,
}

#[workflow_methods]
impl OutboundSignalTargetWorkflow {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<String> {
        ctx.wait_condition(|s| s.received.is_some()).await;
        Ok(ctx.state(|s| {
            s.received
                .clone()
                .expect("signal target should have received a value")
        }))
    }

    #[signal]
    fn capture(&mut self, _ctx: &mut SyncWorkflowContext<Self>, value: String) {
        self.received = Some(value);
    }
}

#[workflow]
#[derive(Default)]
struct OutboundOperationsWorkflow;

#[workflow_methods]
impl OutboundOperationsWorkflow {
    #[run]
    async fn run(
        ctx: &mut WorkflowContext<Self>,
        signal_target: String,
    ) -> WorkflowResult<Vec<String>> {
        let activity_result = ctx
            .start_activity(
                StdActivities::echo,
                "activity-original".to_string(),
                ActivityOptions::start_to_close_timeout(Duration::from_secs(5)),
            )
            .await?;
        let local_activity_result = ctx
            .start_local_activity(
                StdActivities::echo,
                "local-original".to_string(),
                LocalActivityOptions::default(),
            )
            .await?;
        let child = ctx
            .start_child_workflow(
                OutboundChildWorkflow::run,
                "child-original".to_string(),
                ChildWorkflowOptions {
                    workflow_id: format!("{}-child", ctx.workflow_id()),
                    ..Default::default()
                },
            )
            .await
            .expect("child workflow should start");
        let child_result = child.result().await?;
        ctx.external_workflow(signal_target, None)
            .signal(
                OutboundSignalTargetWorkflow::capture,
                "signal-original".to_string(),
            )
            .await
            .expect("external signal should be delivered");
        Ok(vec![activity_result, local_activity_result, child_result])
    }
}

#[tokio::test]
async fn workflow_outbound_interceptors_can_mutate_scheduled_operations() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let wf_name = OutboundOperationsWorkflow::name();
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.register_activities(StdActivities);
    starter
        .sdk_config
        .register_workflow::<OutboundOperationsWorkflow>()
        .unwrap();
    starter
        .sdk_config
        .register_workflow::<OutboundChildWorkflow>()
        .unwrap();
    starter
        .sdk_config
        .register_workflow::<OutboundSignalTargetWorkflow>()
        .unwrap();
    let mut worker = starter.worker().await;
    worker
        .inner_mut()
        .add_workflow_interceptor(MutatingWorkflowOutboundInterceptor {
            events: events.clone(),
        });

    let task_queue = starter.get_task_queue().to_owned();
    let target_workflow_id = format!("{}_signal_target", starter.get_task_queue());
    let target_handle = worker
        .submit_workflow(
            OutboundSignalTargetWorkflow::run,
            (),
            WorkflowStartOptions::new(task_queue.clone(), target_workflow_id.clone()).build(),
        )
        .await
        .unwrap();
    let parent_handle = worker
        .submit_workflow(
            OutboundOperationsWorkflow::run,
            target_workflow_id.clone(),
            WorkflowStartOptions::new(
                task_queue,
                format!("{}_outbound_interceptors", starter.get_task_queue()),
            )
            .build(),
        )
        .await
        .unwrap();

    worker.run_until_done().await.unwrap();

    let parent_result = parent_handle.get_result(Default::default()).await.unwrap();
    assert_eq!(
        parent_result,
        vec![
            "activity-mutated".to_string(),
            "local-mutated".to_string(),
            "child-mutated".to_string()
        ]
    );
    let signal_result = target_handle.get_result(Default::default()).await.unwrap();
    assert_eq!(signal_result, "signal-mutated");

    let events = events.lock().expect("events mutex is not poisoned").clone();
    assert!(
        events.iter().any(|e| matches!(
            e,
            InterceptorEvent::ScheduleActivity {
                input: Some(input),
                is_replaying_history_events: false,
                ..
            } if input == "activity-original"
        )),
        "missing activity schedule event: {events:?}"
    );
    assert!(
        events.iter().any(|e| matches!(
            e,
            InterceptorEvent::ScheduleLocalActivity {
                input: Some(input),
                is_replaying_history_events: false,
                ..
            } if input == "local-original"
        )),
        "missing local activity schedule event: {events:?}"
    );
    assert!(
        events.iter().any(|e| matches!(
            e,
            InterceptorEvent::StartChildWorkflowExecution {
                workflow_type,
                input: Some(input),
                is_replaying_history_events: false,
                ..
            } if workflow_type == OutboundChildWorkflow::name()
                && input == "child-original"
        )),
        "missing child workflow start event: {events:?}"
    );
    assert!(
        events.iter().any(|e| matches!(
            e,
            InterceptorEvent::SignalWorkflow {
                signal_name,
                target: SignalWorkflowTarget::ExternalWorkflow { workflow_id, .. },
                input: Some(input),
                is_replaying_history_events: false,
                ..
            } if signal_name == "capture"
                && workflow_id == &target_workflow_id
                && input == "signal-original"
        )),
        "missing signal workflow event: {events:?}"
    );
}

struct NormalizeArgsInterceptor {
    floor: i32,
}

impl WorkflowInterceptor for NormalizeArgsInterceptor {
    fn intercept_workflow(&self, _ctx: WorkflowInterceptorContext) -> WorkflowInterceptors {
        WorkflowInterceptors {
            inbound: Box::new(NormalizeArgsInbound { floor: self.floor }),
            ..Default::default()
        }
    }
}

struct NormalizeArgsInbound {
    floor: i32,
}

impl WorkflowInboundInterceptor for NormalizeArgsInbound {
    fn execute<'a>(
        &'a self,
        mut input: ExecuteInput,
        next: Next<'a, ExecuteInput, ExecuteOutput>,
    ) -> ExecuteOutput {
        if let Some(target) = input.args_mut::<i32>()
            && *target < self.floor
        {
            *target = self.floor;
        }
        next.run(input)
    }
}

#[workflow]
#[derive(Default)]
struct NormalizedArgsWorkflow {
    counter: i32,
}

#[workflow_methods]
impl NormalizedArgsWorkflow {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>, target: i32) -> WorkflowResult<i32> {
        ctx.wait_condition(|s| s.counter >= target).await;
        Ok(target)
    }

    #[signal]
    fn bump(&mut self, _ctx: &mut SyncWorkflowContext<Self>, amount: i32) {
        self.counter += amount;
    }
}

#[tokio::test]
async fn execute_interceptor_can_normalize_workflow_args() {
    let wf_name = NormalizedArgsWorkflow::name();
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;
    worker
        .inner_mut()
        .add_workflow_interceptor(NormalizeArgsInterceptor { floor: 5 });
    worker
        .register_workflow::<NormalizedArgsWorkflow>()
        .unwrap();

    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            NormalizedArgsWorkflow::run,
            1,
            WorkflowStartOptions::new(
                task_queue.clone(),
                format!("{}_normalize_args", starter.get_task_queue()),
            )
            .build(),
        )
        .await
        .unwrap();

    let interactions = async {
        for _ in 0..5 {
            handle
                .signal(
                    NormalizedArgsWorkflow::bump,
                    1,
                    WorkflowSignalOptions::default(),
                )
                .await
                .unwrap();
        }
    };

    let (_, worker_res) = tokio::join!(interactions, worker.run_until_done());
    worker_res.unwrap();

    let result = handle.get_result(Default::default()).await.unwrap();
    assert_eq!(
        result, 5,
        "interceptor should have raised the workflow input from 1 to the floor of 5"
    );
}

struct SplitArgMutatingInterceptor {
    observed_value: Arc<Mutex<Option<u64>>>,
    replacement: u64,
}

impl WorkflowInterceptor for SplitArgMutatingInterceptor {
    fn intercept_workflow(&self, _ctx: WorkflowInterceptorContext) -> WorkflowInterceptors {
        WorkflowInterceptors {
            inbound: Box::new(SplitArgMutatingInbound {
                observed_value: self.observed_value.clone(),
                replacement: self.replacement,
            }),
            ..Default::default()
        }
    }
}

struct SplitArgMutatingInbound {
    observed_value: Arc<Mutex<Option<u64>>>,
    replacement: u64,
}

impl WorkflowInboundInterceptor for SplitArgMutatingInbound {
    fn execute<'a>(
        &'a self,
        mut input: ExecuteInput,
        next: Next<'a, ExecuteInput, ExecuteOutput>,
    ) -> ExecuteOutput {
        let observed = *input
            .args_ref::<u64>()
            .expect("split-init workflow should expose its typed Input to interceptors");
        self.observed_value.lock().unwrap().replace(observed);
        *input.args_mut::<u64>().unwrap() = self.replacement;
        next.run(input)
    }
}

#[workflow]
#[derive(Default)]
struct SplitArgsWorkflow {
    seeded_value: u64,
}

#[workflow_methods]
impl SplitArgsWorkflow {
    #[init]
    fn init(_ctx: &WorkflowContextView, seeded_value: u64) -> Self {
        Self { seeded_value }
    }

    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<u64> {
        Ok(ctx.state(|s| s.seeded_value))
    }
}

#[tokio::test]
async fn execute_interceptor_arg_mutation_flows_to_split_init() {
    let observed_value = Arc::new(Mutex::new(None));
    let wf_name = SplitArgsWorkflow::name();
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;
    worker
        .inner_mut()
        .add_workflow_interceptor(SplitArgMutatingInterceptor {
            observed_value: observed_value.clone(),
            replacement: 999,
        });
    worker.register_workflow::<SplitArgsWorkflow>().unwrap();

    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            SplitArgsWorkflow::run,
            42_u64,
            WorkflowStartOptions::new(
                task_queue.clone(),
                format!("{}_split_args", starter.get_task_queue()),
            )
            .build(),
        )
        .await
        .unwrap();

    let (_, worker_res) = tokio::join!(async {}, worker.run_until_done());
    worker_res.unwrap();

    let result = handle.get_result(Default::default()).await.unwrap();
    assert_eq!(
        *observed_value.lock().unwrap(),
        Some(42),
        "execute interceptor should observe the originally-submitted typed Input"
    );
    assert_eq!(
        result, 999,
        "interceptor mutation should flow into W::init for split-init workflows, so the \
         seeded_value the workflow returns is the replacement (999), not the original (42)"
    );
}
