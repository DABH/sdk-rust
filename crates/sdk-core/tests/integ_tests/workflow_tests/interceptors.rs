use crate::common::CoreWfStarter;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use temporalio_client::{WorkflowQueryOptions, WorkflowSignalOptions, WorkflowStartOptions};
use temporalio_common::worker::WorkerTaskTypes;
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{
    SyncWorkflowContext, TimerOptions, WorkflowContext, WorkflowContextView, WorkflowResult,
    interceptors::{
        ExecuteInput, HandleQueryInput, HandleSignalInput, Next, SleepInput, SleepOutput,
        WorkflowExecuteOutput, WorkflowInboundInterceptor, WorkflowInterceptor,
        WorkflowInterceptorContext, WorkflowInterceptors, WorkflowOutboundInterceptor,
        WorkflowQueryOutput, WorkflowSignalOutput,
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
    Sleep {
        duration: Duration,
        summary: Option<String>,
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
            | InterceptorEvent::Sleep {
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
        next: Next<'a, ExecuteInput, WorkflowExecuteOutput>,
    ) -> WorkflowExecuteOutput {
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
        next: Next<'a, HandleSignalInput, WorkflowSignalOutput>,
    ) -> WorkflowSignalOutput {
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
        next: Next<'a, HandleQueryInput, WorkflowQueryOutput>,
    ) -> WorkflowQueryOutput {
        self.record(InterceptorEvent::Query {
            query_name: input.query_name().to_string(),
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
    worker.register_workflow::<InterceptedWorkflow>();

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
                7,
                WorkflowSignalOptions::default(),
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
        events.iter().all(|e| !e.is_replaying_history_events()),
        "live workflow operations should not be marked as replaying history events: {events:?}"
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
        next: Next<'a, ExecuteInput, WorkflowExecuteOutput>,
    ) -> WorkflowExecuteOutput {
        if let Some(target) = input.args_mut().downcast_mut::<i32>()
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
    worker.register_workflow::<NormalizedArgsWorkflow>();

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
        next: Next<'a, ExecuteInput, WorkflowExecuteOutput>,
    ) -> WorkflowExecuteOutput {
        let observed = *input
            .args()
            .downcast_ref::<u64>()
            .expect("split-init workflow should expose its typed Input to interceptors");
        self.observed_value.lock().unwrap().replace(observed);
        *input.args_mut().downcast_mut::<u64>().unwrap() = self.replacement;
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
    worker.register_workflow::<SplitArgsWorkflow>();

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
