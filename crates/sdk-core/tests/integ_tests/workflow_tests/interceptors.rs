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
