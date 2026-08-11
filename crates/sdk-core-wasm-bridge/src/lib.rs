//! WASM ABI for driving Temporal Core's activity worker from a host-provided gRPC transport.
//!
//! WASM imports are synchronous, while Core expects several concurrent asynchronous long polls.
//! The bridge therefore exposes a small host-driven event loop: Core queues gRPC requests, the Go
//! host executes them, and then the host returns each response before ticking Core again.

use futures_util::FutureExt;
use prost::Message;
use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use temporalio_client::{
    Connection, ConnectionOptions,
    callback_based::{CallbackBasedGrpcService, GrpcRequest, GrpcSuccessResponse},
};
use temporalio_common::protos::coresdk::{
    ActivityHeartbeat, ActivityTaskCompletion, workflow_completion::WorkflowActivationCompletion,
};
use temporalio_sdk_core::{
    CoreRuntime, PollerBehavior, RuntimeOptions, Worker, WorkerConfig, WorkerTaskTypes,
    WorkerVersioningStrategy, init_worker,
};
use tokio::sync::oneshot;
#[cfg(target_arch = "wasm32")]
use tokio::task::LocalSet;
use tonic::{Code, Status};

const BRIDGE_ERROR: i32 = -1;
const BRIDGE_PENDING: i32 = 1;

type OperationSlot = Arc<Mutex<OperationState>>;
type ConnectionSlot = Arc<Mutex<Option<Result<Connection, String>>>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
enum BridgeInitMode {
    ActivityOnly = 0,
    WorkflowOnly = 1,
}

impl BridgeInitMode {
    fn worker_kind(self) -> &'static str {
        match self {
            Self::ActivityOnly => "activity",
            Self::WorkflowOnly => "workflow",
        }
    }
}

impl TryFrom<u32> for BridgeInitMode {
    type Error = String;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            x if x == Self::ActivityOnly as u32 => Ok(Self::ActivityOnly),
            x if x == Self::WorkflowOnly as u32 => Ok(Self::WorkflowOnly),
            _ => Err(format!("unsupported bridge init mode {value}")),
        }
    }
}

enum OperationState {
    Idle,
    Pending,
    Ready(Result<Vec<u8>, String>),
}

struct BridgeState {
    mode: BridgeInitMode,
    runtime: tokio::runtime::Runtime,
    _core_runtime: CoreRuntime,
    worker: Option<Arc<Worker>>,
    worker_config: Option<WorkerConfig>,
    connection_result: ConnectionSlot,
    initialization_result: OperationSlot,
    workflow_poll_result: OperationSlot,
    workflow_completion_result: OperationSlot,
    activity_poll_result: OperationSlot,
    activity_completion_result: OperationSlot,
    shutdown_result: OperationSlot,
}

struct PendingGrpcRequest {
    id: u64,
    service: String,
    rpc: String,
    headers: Vec<u8>,
    proto: Vec<u8>,
}

type GrpcResponse = Result<GrpcSuccessResponse, Status>;

#[derive(Default)]
struct HostTransport {
    next_id: AtomicU64,
    requests: Mutex<VecDeque<PendingGrpcRequest>>,
    responders: Mutex<HashMap<u64, oneshot::Sender<GrpcResponse>>>,
}

impl HostTransport {
    async fn call(&self, request: GrpcRequest) -> GrpcResponse {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (response_tx, response_rx) = oneshot::channel();
        self.responders
            .lock()
            .expect("host transport responders lock is not poisoned")
            .insert(id, response_tx);
        self.requests
            .lock()
            .expect("host transport request lock is not poisoned")
            .push_back(PendingGrpcRequest {
                id,
                service: request.service,
                rpc: request.rpc,
                headers: encode_headers(&request.headers),
                proto: request.proto.to_vec(),
            });
        response_rx
            .await
            .unwrap_or_else(|_| Err(Status::cancelled("host transport response was dropped")))
    }

    fn take_request(&self) -> Option<PendingGrpcRequest> {
        self.requests
            .lock()
            .expect("host transport request lock is not poisoned")
            .pop_front()
    }

    fn complete(&self, id: u64, response: GrpcResponse) -> Result<(), String> {
        let sender = self
            .responders
            .lock()
            .map_err(|_| "host transport responders lock is poisoned".to_owned())?
            .remove(&id)
            .ok_or_else(|| format!("unknown host transport request id {id}"))?;
        // Core may cancel a long poll before the host-side gRPC call returns during shutdown.
        let _ = sender.send(response);
        Ok(())
    }

    fn reset(&self) {
        self.requests
            .lock()
            .expect("host transport request lock is not poisoned")
            .clear();
        self.responders
            .lock()
            .expect("host transport responders lock is not poisoned")
            .clear();
    }
}

static STATE: OnceLock<Mutex<Option<BridgeState>>> = OnceLock::new();
static HOST_TRANSPORT: OnceLock<HostTransport> = OnceLock::new();
#[cfg(target_arch = "wasm32")]
thread_local! {
    static WORKFLOW_LOCAL_SET: LocalSet = LocalSet::new();
}

/// Allocate guest memory that the Go host can populate before calling an exported function.
#[unsafe(no_mangle)]
pub extern "C" fn temporal_alloc(len: usize) -> *mut u8 {
    let mut bytes = vec![0; len];
    let ptr = bytes.as_mut_ptr();
    std::mem::forget(bytes);
    ptr
}

/// Release guest memory previously returned by [`temporal_alloc`].
///
/// # Safety
///
/// `ptr` and `len` must exactly match a live allocation returned by [`temporal_alloc`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn temporal_dealloc(ptr: *mut u8, len: usize) {
    if !ptr.is_null() {
        // SAFETY: The caller promises this is the pointer and length from temporal_alloc.
        drop(unsafe { Vec::from_raw_parts(ptr, len, len) });
    }
}

/// Start initializing one activity-only Temporal Core worker.
#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_init(
    namespace_ptr: *const u8,
    namespace_len: usize,
    task_queue_ptr: *const u8,
    task_queue_len: usize,
    identity_ptr: *const u8,
    identity_len: usize,
    max_concurrent_activity_executions: usize,
    max_concurrent_activity_task_pollers: usize,
    error_ptr: *mut u8,
    error_capacity: usize,
) -> i64 {
    temporal_core_init_with_mode(
        namespace_ptr,
        namespace_len,
        task_queue_ptr,
        task_queue_len,
        identity_ptr,
        identity_len,
        max_concurrent_activity_executions,
        max_concurrent_activity_task_pollers,
        BridgeInitMode::ActivityOnly as u32,
        error_ptr,
        error_capacity,
    )
}

/// Start initializing one Temporal Core worker in the requested bridge mode.
///
/// Supported modes:
/// - `0`: activity-only
/// - `1`: workflow-only
#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_init_with_mode(
    namespace_ptr: *const u8,
    namespace_len: usize,
    task_queue_ptr: *const u8,
    task_queue_len: usize,
    identity_ptr: *const u8,
    identity_len: usize,
    max_concurrent_activity_executions: usize,
    max_concurrent_activity_task_pollers: usize,
    mode: u32,
    error_ptr: *mut u8,
    error_capacity: usize,
) -> i64 {
    write_result(error_ptr, error_capacity, || {
        let mode = BridgeInitMode::try_from(mode)?;
        let namespace = read_string(namespace_ptr, namespace_len)?;
        let task_queue = read_string(task_queue_ptr, task_queue_len)?;
        let identity = read_string(identity_ptr, identity_len)?;
        let state_lock = STATE.get_or_init(|| Mutex::new(None));
        let mut state = state_lock
            .lock()
            .map_err(|_| "Core bridge state lock is poisoned".to_owned())?;
        if state.is_some() {
            return Err("Core worker is already initialized".to_owned());
        }
        HOST_TRANSPORT.get_or_init(HostTransport::default).reset();

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|err| format!("failed to create Tokio runtime: {err}"))?;
        let runtime_options = RuntimeOptions::builder()
            .heartbeat_interval(None)
            .disable_environment_info(true)
            .build()
            .map_err(|err| format!("invalid Core runtime options: {err}"))?;
        let core_runtime = {
            let _guard = runtime.enter();
            CoreRuntime::new_assume_tokio(runtime_options)
                .map_err(|err| format!("failed to initialize Temporal Core: {err}"))?
        };

        let grpc_service = CallbackBasedGrpcService {
            callback: Arc::new(|request| {
                HOST_TRANSPORT
                    .get_or_init(HostTransport::default)
                    .call(request)
                    .boxed()
            }),
        };
        let connection_options = ConnectionOptions::new(
            temporalio_client::Url::parse("http://temporal-host.invalid:7233")
                .expect("static URL is valid"),
        )
        .identity(identity)
        .service_override(grpc_service)
        .dns_load_balancing(None)
        .keep_alive(None)
        .build();

        let worker_config = worker_config_for_mode(
            mode,
            namespace,
            task_queue,
            max_concurrent_activity_executions,
            max_concurrent_activity_task_pollers,
        )?;

        let connection_result = Arc::new(Mutex::new(None));
        let initialization_result = operation_slot();
        mark_pending(&initialization_result, "worker initialization")?;
        let connection_result_for_task = connection_result.clone();
        runtime.spawn(async move {
            let result = Connection::connect(connection_options)
                .await
                .map_err(|err| format!("failed to initialize callback gRPC client: {err}"));
            *connection_result_for_task
                .lock()
                .expect("connection result lock is not poisoned") = Some(result);
        });

        *state = Some(BridgeState {
            mode,
            runtime,
            _core_runtime: core_runtime,
            worker: None,
            worker_config: Some(worker_config),
            connection_result,
            initialization_result,
            workflow_poll_result: operation_slot(),
            workflow_completion_result: operation_slot(),
            activity_poll_result: operation_slot(),
            activity_completion_result: operation_slot(),
            shutdown_result: operation_slot(),
        });
        Ok(Vec::new())
    })
}

fn worker_config_for_mode(
    mode: BridgeInitMode,
    namespace: String,
    task_queue: String,
    max_concurrent_activity_executions: usize,
    max_concurrent_activity_task_pollers: usize,
) -> Result<WorkerConfig, String> {
    match mode {
        BridgeInitMode::ActivityOnly => activity_worker_config(
            namespace,
            task_queue,
            max_concurrent_activity_executions,
            max_concurrent_activity_task_pollers,
        ),
        BridgeInitMode::WorkflowOnly => workflow_worker_config(namespace, task_queue),
    }
}

fn activity_worker_config(
    namespace: String,
    task_queue: String,
    max_concurrent_activity_executions: usize,
    max_concurrent_activity_task_pollers: usize,
) -> Result<WorkerConfig, String> {
    WorkerConfig::builder()
        .namespace(namespace)
        .task_queue(task_queue)
        .versioning_strategy(WorkerVersioningStrategy::None {
            build_id: "go-wasm-core-prototype".to_owned(),
        })
        .task_types(WorkerTaskTypes::activity_only())
        .maybe_activity_task_poller_behavior((max_concurrent_activity_task_pollers > 0).then_some(
            PollerBehavior::SimpleMaximum(max_concurrent_activity_task_pollers),
        ))
        .max_outstanding_activities(max_concurrent_activity_executions)
        .build()
        .map_err(|err| format!("invalid activity worker configuration: {err}"))
}

fn workflow_worker_config(namespace: String, task_queue: String) -> Result<WorkerConfig, String> {
    WorkerConfig::builder()
        .namespace(namespace)
        .task_queue(task_queue)
        .versioning_strategy(WorkerVersioningStrategy::None {
            build_id: "go-wasm-core-prototype".to_owned(),
        })
        .task_types(WorkerTaskTypes::workflow_only())
        .max_cached_workflows(1_usize)
        .max_outstanding_workflow_tasks(2_usize)
        .workflow_task_poller_behavior(PollerBehavior::SimpleMaximum(2))
        .build()
        .map_err(|err| format!("invalid workflow worker configuration: {err}"))
}

/// Take the result of Core connection and worker validation, or return the pending result code.
#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_take_init(error_ptr: *mut u8, error_capacity: usize) -> i64 {
    let preparation = with_state(|state| {
        if state.worker.is_some() {
            return Ok(());
        }
        let Some(connection) = state
            .connection_result
            .lock()
            .map_err(|_| "connection result lock is poisoned".to_owned())?
            .take()
        else {
            return Ok(());
        };
        let connection = match connection {
            Ok(connection) => connection,
            Err(error) => {
                set_ready(&state.initialization_result, Err(error));
                return Ok(());
            }
        };
        let mode = state.mode;
        let worker_config = state
            .worker_config
            .take()
            .ok_or_else(|| "worker configuration is missing".to_owned())?;
        #[cfg(not(target_arch = "wasm32"))]
        let worker_result = init_worker(&state._core_runtime, worker_config, connection);
        #[cfg(target_arch = "wasm32")]
        let worker_result = WORKFLOW_LOCAL_SET.with(|local| {
            let _guard = local.enter();
            init_worker(&state._core_runtime, worker_config, connection)
        });
        let worker = match worker_result {
            Ok(worker) => Arc::new(worker),
            Err(error) => {
                set_ready(
                    &state.initialization_result,
                    Err(format!(
                        "failed to initialize {} worker: {error}",
                        mode.worker_kind()
                    )),
                );
                return Ok(());
            }
        };
        state.worker = Some(worker.clone());
        let result = state.initialization_result.clone();
        let validation = async move {
            let value =
                worker.validate().await.map(|_| Vec::new()).map_err(|err| {
                    format!("{} worker validation failed: {err}", mode.worker_kind())
                });
            set_ready(&result, value);
        };
        #[cfg(not(target_arch = "wasm32"))]
        drop(state.runtime.spawn(validation));
        #[cfg(target_arch = "wasm32")]
        WORKFLOW_LOCAL_SET.with(|local| drop(local.spawn_local(validation)));
        Ok(())
    });
    if let Err(error) = preparation {
        let result = write_result(error_ptr, error_capacity, || Err(error));
        reset_bridge_state();
        return result;
    }
    let result = take_operation_result(error_ptr, error_capacity, |state| {
        &state.initialization_result
    });
    if unpack_result_code(result) == BRIDGE_ERROR {
        reset_bridge_state();
    }
    result
}

/// Start an asynchronous Core activity poll.
#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_start_poll_activity(
    error_ptr: *mut u8,
    error_capacity: usize,
) -> i64 {
    write_result(error_ptr, error_capacity, || {
        with_state(|state| {
            ensure_mode(state, BridgeInitMode::ActivityOnly)?;
            mark_pending(&state.activity_poll_result, "activity poll")?;
            let worker = initialized_worker(state)?;
            let result = state.activity_poll_result.clone();
            state.runtime.spawn(async move {
                let value = worker
                    .poll_activity_task()
                    .await
                    .map(|task| task.encode_to_vec())
                    .map_err(|err| format!("activity poll failed: {err}"));
                set_ready(&result, value);
            });
            Ok(Vec::new())
        })
    })
}

/// Take a completed activity poll result, or return the pending result code.
#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_take_poll_activity(
    output_ptr: *mut u8,
    output_capacity: usize,
) -> i64 {
    take_operation_result(output_ptr, output_capacity, |state| {
        &state.activity_poll_result
    })
}

/// Start submitting a protobuf-encoded activity completion to Core.
#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_start_complete_activity(
    completion_ptr: *const u8,
    completion_len: usize,
    error_ptr: *mut u8,
    error_capacity: usize,
) -> i64 {
    write_result(error_ptr, error_capacity, || {
        let completion = ActivityTaskCompletion::decode(read_bytes(completion_ptr, completion_len))
            .map_err(|err| format!("invalid ActivityTaskCompletion protobuf: {err}"))?;
        with_state(|state| {
            ensure_mode(state, BridgeInitMode::ActivityOnly)?;
            mark_pending(&state.activity_completion_result, "activity completion")?;
            let worker = initialized_worker(state)?;
            let result = state.activity_completion_result.clone();
            state.runtime.spawn(async move {
                let value = worker
                    .complete_activity_task(completion)
                    .await
                    .map(|_| Vec::new())
                    .map_err(|err| format!("activity completion failed: {err}"));
                set_ready(&result, value);
            });
            Ok(Vec::new())
        })
    })
}

/// Take the result of a started activity completion, or return the pending result code.
#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_take_complete_activity(
    error_ptr: *mut u8,
    error_capacity: usize,
) -> i64 {
    take_operation_result(error_ptr, error_capacity, |state| {
        &state.activity_completion_result
    })
}

/// Start an asynchronous Core workflow activation poll.
#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_start_poll_workflow_activation(
    error_ptr: *mut u8,
    error_capacity: usize,
) -> i64 {
    write_result(error_ptr, error_capacity, || {
        with_state(|state| {
            ensure_mode(state, BridgeInitMode::WorkflowOnly)?;
            mark_pending(&state.workflow_poll_result, "workflow activation poll")?;
            let worker = initialized_worker(state)?;
            let result = state.workflow_poll_result.clone();
            let poll = async move {
                let value = worker
                    .poll_workflow_activation()
                    .await
                    .map(|activation| activation.encode_to_vec())
                    .map_err(|err| format!("workflow activation poll failed: {err}"));
                set_ready(&result, value);
            };
            #[cfg(not(target_arch = "wasm32"))]
            drop(state.runtime.spawn(poll));
            #[cfg(target_arch = "wasm32")]
            WORKFLOW_LOCAL_SET.with(|local| drop(local.spawn_local(poll)));
            Ok(Vec::new())
        })
    })
}

/// Take a completed workflow activation poll result, or return the pending result code.
#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_take_poll_workflow_activation(
    output_ptr: *mut u8,
    output_capacity: usize,
) -> i64 {
    take_operation_result(output_ptr, output_capacity, |state| {
        &state.workflow_poll_result
    })
}

/// Start submitting a protobuf-encoded workflow activation completion to Core.
#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_start_complete_workflow_activation(
    completion_ptr: *const u8,
    completion_len: usize,
    error_ptr: *mut u8,
    error_capacity: usize,
) -> i64 {
    write_result(error_ptr, error_capacity, || {
        let completion =
            WorkflowActivationCompletion::decode(read_bytes(completion_ptr, completion_len))
                .map_err(|err| format!("invalid WorkflowActivationCompletion protobuf: {err}"))?;
        with_state(|state| {
            ensure_mode(state, BridgeInitMode::WorkflowOnly)?;
            mark_pending(
                &state.workflow_completion_result,
                "workflow activation completion",
            )?;
            let worker = initialized_worker(state)?;
            let result = state.workflow_completion_result.clone();
            let completion = async move {
                let value = worker
                    .complete_workflow_activation(completion)
                    .await
                    .map(|_| Vec::new())
                    .map_err(|err| format!("workflow activation completion failed: {err}"));
                set_ready(&result, value);
            };
            #[cfg(not(target_arch = "wasm32"))]
            drop(state.runtime.spawn(completion));
            #[cfg(target_arch = "wasm32")]
            WORKFLOW_LOCAL_SET.with(|local| drop(local.spawn_local(completion)));
            Ok(Vec::new())
        })
    })
}

/// Take the result of a started workflow activation completion, or return the pending result code.
#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_take_complete_workflow_activation(
    error_ptr: *mut u8,
    error_capacity: usize,
) -> i64 {
    take_operation_result(error_ptr, error_capacity, |state| {
        &state.workflow_completion_result
    })
}

/// Record a protobuf-encoded activity heartbeat through Core.
#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_record_activity_heartbeat(
    heartbeat_ptr: *const u8,
    heartbeat_len: usize,
    error_ptr: *mut u8,
    error_capacity: usize,
) -> i64 {
    write_result(error_ptr, error_capacity, || {
        let heartbeat = ActivityHeartbeat::decode(read_bytes(heartbeat_ptr, heartbeat_len))
            .map_err(|err| format!("invalid ActivityHeartbeat protobuf: {err}"))?;
        with_state(|state| {
            initialized_worker(state)?.record_activity_heartbeat(heartbeat);
            Ok(Vec::new())
        })
    })
}

/// Advance Core's current-thread Tokio runtime without blocking on a long poll.
#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_tick(error_ptr: *mut u8, error_capacity: usize) -> i64 {
    write_result(error_ptr, error_capacity, || {
        with_state(|state| {
            #[cfg(not(target_arch = "wasm32"))]
            state
                .runtime
                .block_on(async { tokio::time::sleep(Duration::from_millis(1)).await });
            #[cfg(target_arch = "wasm32")]
            WORKFLOW_LOCAL_SET.with(|local| {
                local.block_on(&state.runtime, async {
                    tokio::time::sleep(Duration::from_millis(1)).await
                });
            });
            Ok(Vec::new())
        })
    })
}

/// Take the next queued Core gRPC request as a length-delimited binary envelope.
#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_take_grpc_request(
    output_ptr: *mut u8,
    output_capacity: usize,
) -> i64 {
    let Some(request) = HOST_TRANSPORT
        .get_or_init(HostTransport::default)
        .take_request()
    else {
        return pack_result(BRIDGE_PENDING, 0);
    };
    write_result(output_ptr, output_capacity, || {
        Ok(encode_grpc_request(request))
    })
}

/// Return a host gRPC response to the waiting Core transport future.
#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_complete_grpc_request(
    id: u64,
    status_code: i32,
    response_ptr: *const u8,
    response_len: usize,
    error_ptr: *mut u8,
    error_capacity: usize,
) -> i64 {
    write_result(error_ptr, error_capacity, || {
        let response = read_bytes(response_ptr, response_len).to_vec();
        let response = if status_code == Code::Ok as i32 {
            Ok(GrpcSuccessResponse {
                headers: Default::default(),
                proto: response,
            })
        } else {
            Err(Status::new(
                Code::from_i32(status_code),
                String::from_utf8_lossy(&response).into_owned(),
            ))
        };
        HOST_TRANSPORT
            .get_or_init(HostTransport::default)
            .complete(id, response)?;
        Ok(Vec::new())
    })
}

/// Start Temporal Core's asynchronous graceful worker shutdown.
#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_start_shutdown(error_ptr: *mut u8, error_capacity: usize) -> i64 {
    write_result(error_ptr, error_capacity, || {
        with_state(|state| {
            mark_pending(&state.shutdown_result, "worker shutdown")?;
            let worker = initialized_worker(state)?;
            let result = state.shutdown_result.clone();
            state.runtime.spawn(async move {
                worker.shutdown().await;
                set_ready(&result, Ok(Vec::new()));
            });
            Ok(Vec::new())
        })
    })
}

/// Take the result of graceful worker shutdown, or return the pending result code.
#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_take_shutdown(error_ptr: *mut u8, error_capacity: usize) -> i64 {
    let result = take_operation_result(error_ptr, error_capacity, |state| &state.shutdown_result);
    if unpack_result_code(result) == 0 {
        reset_bridge_state();
    }
    result
}

fn operation_slot() -> OperationSlot {
    Arc::new(Mutex::new(OperationState::Idle))
}

fn mark_pending(slot: &OperationSlot, name: &str) -> Result<(), String> {
    let mut state = slot
        .lock()
        .map_err(|_| format!("{name} result lock is poisoned"))?;
    if !matches!(*state, OperationState::Idle) {
        return Err(format!("{name} is already in progress"));
    }
    *state = OperationState::Pending;
    Ok(())
}

fn set_ready(slot: &OperationSlot, value: Result<Vec<u8>, String>) {
    *slot.lock().expect("operation result lock is not poisoned") = OperationState::Ready(value);
}

fn take_operation_result(
    output_ptr: *mut u8,
    output_capacity: usize,
    select: impl FnOnce(&BridgeState) -> &OperationSlot,
) -> i64 {
    let result = with_state(|state| {
        let mut operation = select(state)
            .lock()
            .map_err(|_| "operation result lock is poisoned".to_owned())?;
        match std::mem::replace(&mut *operation, OperationState::Idle) {
            OperationState::Ready(result) => result.map(Some),
            OperationState::Pending => {
                *operation = OperationState::Pending;
                Ok(None)
            }
            OperationState::Idle => Err("operation has not been started".to_owned()),
        }
    });
    match result {
        Ok(None) => pack_result(BRIDGE_PENDING, 0),
        Ok(Some(output)) => write_result(output_ptr, output_capacity, || Ok(output)),
        Err(error) => write_result(output_ptr, output_capacity, || Err(error)),
    }
}

fn with_state<T>(
    operation: impl FnOnce(&mut BridgeState) -> Result<T, String>,
) -> Result<T, String> {
    let state_lock = STATE.get_or_init(|| Mutex::new(None));
    let mut state = state_lock
        .lock()
        .map_err(|_| "Core bridge state lock is poisoned".to_owned())?;
    let state = state
        .as_mut()
        .ok_or_else(|| "Core worker is not initialized".to_owned())?;
    operation(state)
}

fn initialized_worker(state: &BridgeState) -> Result<Arc<Worker>, String> {
    state
        .worker
        .clone()
        .ok_or_else(|| "Core worker initialization is not complete".to_owned())
}

fn ensure_mode(state: &BridgeState, expected: BridgeInitMode) -> Result<(), String> {
    if state.mode == expected {
        Ok(())
    } else {
        Err(format!(
            "{} bridge operation requires {} mode",
            state.mode.worker_kind(),
            expected.worker_kind()
        ))
    }
}

fn reset_bridge_state() {
    if let Ok(mut state) = STATE.get_or_init(|| Mutex::new(None)).lock() {
        state.take();
    }
    HOST_TRANSPORT.get_or_init(HostTransport::default).reset();
}

fn unpack_result_code(result: i64) -> i32 {
    (result as u64 >> 32) as i32
}

fn encode_headers(headers: &http::HeaderMap) -> Vec<u8> {
    let mut output = Vec::new();
    for (name, value) in headers {
        let name = name.as_str().as_bytes();
        let value = value.as_bytes();
        output.extend_from_slice(&(name.len() as u32).to_le_bytes());
        output.extend_from_slice(&(value.len() as u32).to_le_bytes());
        output.extend_from_slice(name);
        output.extend_from_slice(value);
    }
    output
}

fn encode_grpc_request(request: PendingGrpcRequest) -> Vec<u8> {
    let mut output = Vec::with_capacity(
        24 + request.service.len()
            + request.rpc.len()
            + request.headers.len()
            + request.proto.len(),
    );
    output.extend_from_slice(&request.id.to_le_bytes());
    output.extend_from_slice(&(request.service.len() as u32).to_le_bytes());
    output.extend_from_slice(&(request.rpc.len() as u32).to_le_bytes());
    output.extend_from_slice(&(request.headers.len() as u32).to_le_bytes());
    output.extend_from_slice(&(request.proto.len() as u32).to_le_bytes());
    output.extend_from_slice(request.service.as_bytes());
    output.extend_from_slice(request.rpc.as_bytes());
    output.extend_from_slice(&request.headers);
    output.extend_from_slice(&request.proto);
    output
}

fn write_result(
    output_ptr: *mut u8,
    output_capacity: usize,
    operation: impl FnOnce() -> Result<Vec<u8>, String>,
) -> i64 {
    match operation() {
        Ok(output) if output.len() <= output_capacity => {
            copy_to_guest(output_ptr, &output);
            pack_result(0, output.len() as u32)
        }
        Ok(output) => write_error(
            output_ptr,
            output_capacity,
            format!(
                "bridge output is {} bytes but the host provided {} bytes",
                output.len(),
                output_capacity
            ),
        ),
        Err(error) => write_error(output_ptr, output_capacity, error),
    }
}

fn write_error(output_ptr: *mut u8, output_capacity: usize, error: String) -> i64 {
    let bytes = error.as_bytes();
    let len = bytes.len().min(output_capacity);
    copy_to_guest(output_ptr, &bytes[..len]);
    pack_result(BRIDGE_ERROR, len as u32)
}

fn copy_to_guest(output_ptr: *mut u8, bytes: &[u8]) {
    if bytes.is_empty() || output_ptr.is_null() {
        return;
    }
    // SAFETY: Export callers provide writable guest memory of at least bytes.len().
    unsafe { std::ptr::copy_nonoverlapping(bytes.as_ptr(), output_ptr, bytes.len()) };
}

fn read_bytes<'a>(ptr: *const u8, len: usize) -> &'a [u8] {
    if len == 0 {
        return &[];
    }
    // SAFETY: Export callers provide readable guest memory for the duration of the call.
    unsafe { std::slice::from_raw_parts(ptr, len) }
}

fn read_string(ptr: *const u8, len: usize) -> Result<String, String> {
    String::from_utf8(read_bytes(ptr, len).to_vec())
        .map_err(|err| format!("bridge string parameter is not UTF-8: {err}"))
}

fn pack_result(code: i32, len: u32) -> i64 {
    (((code as u32 as u64) << 32) | len as u64) as i64
}

#[cfg(test)]
fn unpack_result(result: i64) -> (i32, usize) {
    let result = result as u64;
    ((result >> 32) as u32 as i32, result as u32 as usize)
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderValue;

    #[test]
    fn worker_config_uses_core_activity_concurrency() {
        let config = activity_worker_config("namespace".to_owned(), "queue".to_owned(), 12, 3)
            .expect("worker config is valid");
        assert_eq!(config.max_outstanding_activities, Some(12));
        assert_eq!(
            config.activity_task_poller_behavior,
            Some(PollerBehavior::SimpleMaximum(3))
        );
    }

    #[test]
    fn worker_config_leaves_default_poller_behavior_to_core() {
        let config = activity_worker_config("namespace".to_owned(), "queue".to_owned(), 12, 0)
            .expect("worker config is valid");
        assert_eq!(config.activity_task_poller_behavior, None);
    }

    #[test]
    fn workflow_worker_config_enables_cached_state_and_evictions() {
        let config = workflow_worker_config("namespace".to_owned(), "queue".to_owned())
            .expect("worker config is valid");
        assert_eq!(config.task_types, WorkerTaskTypes::workflow_only());
        assert_eq!(config.max_cached_workflows, 1);
        assert_eq!(config.max_outstanding_workflow_tasks, Some(2));
        assert_eq!(
            config.workflow_task_poller_behavior,
            Some(PollerBehavior::SimpleMaximum(2))
        );
    }

    #[test]
    fn init_modes_are_stable_for_hosts() {
        assert_eq!(
            BridgeInitMode::try_from(BridgeInitMode::ActivityOnly as u32).expect("mode is valid"),
            BridgeInitMode::ActivityOnly
        );
        assert_eq!(
            BridgeInitMode::try_from(BridgeInitMode::WorkflowOnly as u32).expect("mode is valid"),
            BridgeInitMode::WorkflowOnly
        );
        assert!(BridgeInitMode::try_from(99).is_err());
    }

    #[test]
    fn bridge_uses_independent_workflow_and_activity_slots() {
        let workflow_poll = operation_slot();
        let workflow_completion = operation_slot();
        let activity_poll = operation_slot();
        let activity_completion = operation_slot();
        assert!(!Arc::ptr_eq(&workflow_poll, &workflow_completion));
        assert!(!Arc::ptr_eq(&workflow_poll, &activity_poll));
        assert!(!Arc::ptr_eq(&workflow_poll, &activity_completion));
        assert!(!Arc::ptr_eq(&workflow_completion, &activity_poll));
        assert!(!Arc::ptr_eq(&workflow_completion, &activity_completion));
        assert!(!Arc::ptr_eq(&activity_poll, &activity_completion));
    }

    #[test]
    fn result_word_preserves_signed_code_and_length() {
        assert_eq!(unpack_result(pack_result(-1, 42)), (-1, 42));
        assert_eq!(
            unpack_result(pack_result(Code::Unavailable as i32, 7)),
            (14, 7)
        );
    }

    #[test]
    fn header_encoding_is_length_delimited() {
        let mut headers = http::HeaderMap::new();
        headers.insert("client-name", HeaderValue::from_static("go-wasm"));
        let encoded = encode_headers(&headers);
        assert_eq!(&encoded[0..4], &11_u32.to_le_bytes());
        assert_eq!(&encoded[4..8], &7_u32.to_le_bytes());
        assert_eq!(&encoded[8..19], b"client-name");
        assert_eq!(&encoded[19..26], b"go-wasm");
    }

    #[test]
    fn grpc_request_encoding_includes_id_and_lengths() {
        let encoded = encode_grpc_request(PendingGrpcRequest {
            id: 9,
            service: "service".to_owned(),
            rpc: "rpc".to_owned(),
            headers: vec![1, 2],
            proto: vec![3, 4, 5],
        });
        assert_eq!(&encoded[0..8], &9_u64.to_le_bytes());
        assert_eq!(&encoded[8..12], &7_u32.to_le_bytes());
        assert_eq!(&encoded[12..16], &3_u32.to_le_bytes());
        assert_eq!(&encoded[16..20], &2_u32.to_le_bytes());
        assert_eq!(&encoded[20..24], &3_u32.to_le_bytes());
        assert_eq!(&encoded[24..], b"servicerpc\x01\x02\x03\x04\x05");
    }

    #[test]
    fn late_grpc_response_is_ignored_after_core_cancels_request() {
        let transport = HostTransport::default();
        let (sender, receiver) = oneshot::channel();
        transport
            .responders
            .lock()
            .expect("responders lock is not poisoned")
            .insert(7, sender);
        drop(receiver);

        assert!(
            transport
                .complete(7, Err(Status::cancelled("late response")))
                .is_ok()
        );
    }
}
