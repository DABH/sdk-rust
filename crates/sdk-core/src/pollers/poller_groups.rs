use crate::worker::client::WorkerClient;
use parking_lot::Mutex;
use std::{collections::HashMap, sync::Arc};
use temporalio_client::worker::PollerGroupInfoStore;

#[derive(Clone)]
pub(crate) struct PollerGroupInfoStoreResolver {
    client: Arc<dyn WorkerClient>,
    namespace: Arc<str>,
}

impl PollerGroupInfoStoreResolver {
    pub(crate) fn new(client: Arc<dyn WorkerClient>, namespace: String) -> Self {
        Self {
            client,
            namespace: namespace.into(),
        }
    }

    pub(crate) fn current(&self) -> Arc<PollerGroupInfoStore> {
        self.client
            .workers()
            .poller_group_info_store(self.namespace.as_ref())
    }
}

/// Owned by each poller
struct PollerGroupManager {
    store_resolver: PollerGroupInfoStoreResolver,
    tracker: Mutex<PollerGroupTracker>,
}

/// One in-flight RPC
struct PollerGroupLease {
    manager: Arc<PollerGroupManager>,
    request_group_id: Option<Arc<str>>,
    queue_kind: Option<WorkflowQueueKind>,
}

enum WorkflowQueueKind {
    Normal,
    Sticky,
}

enum PollerGroupTracker {
    Standard(HashMap<Arc<str>, usize>),
    Workflow(HashMap<Arc<str>, WorkflowGroupState>),
}

struct WorkflowGroupState {
    pending_normal_polls: usize,
    pending_sticky_polls: usize,
    sticky_backlog: usize,
}
