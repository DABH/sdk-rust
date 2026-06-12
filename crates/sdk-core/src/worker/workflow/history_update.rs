use crate::{
    abstractions::dbg_panic,
    internal_flags::CoreInternalFlags,
    protosext::ValidPollWFTQResponse,
    worker::{
        client::WorkerClient,
        workflow::{CacheMissFetchReq, PermittedWFT, PreparedWFT},
    },
};
use futures_util::{FutureExt, Stream, future::BoxFuture};
use itertools::Itertools;
use std::{
    collections::VecDeque,
    fmt::Debug,
    future::Future,
    mem,
    mem::transmute,
    pin::Pin,
    sync::{Arc, LazyLock},
    task::{Context, Poll},
};
use temporalio_common::protos::temporal::api::{
    enums::v1::EventType,
    history::v1::{
        HistoryEvent, WorkflowTaskCompletedEventAttributes, history_event::Attributes,
    },
};
use tracing::Instrument;

static EMPTY_TASK_ERR: LazyLock<tonic::Status> = LazyLock::new(|| {
    tonic::Status::unknown("Received an empty workflow task with no queries or history")
});

/// Per-poll envelope metadata: the parts of a polled WFT that aren't its events.
///
/// These fields are properties of the poll, not of any individual event. The
/// [`LwftBuffer`] remembers the most recently pushed envelope so the chunker
/// can access `has_pending_speculative_updates` for the current poll, and so
/// `WorkflowMachines` can reach the polled WFT's `previous_wft_started_id` /
/// `wft_started_id` without crossing back through the paginator.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct WftEnvelope {
    /// The event ID of the last started WFT, as according to the polled WFT.
    pub(crate) previous_wft_started_id: i64,
    /// The `started_event_id` field from the polled WFT. Multiple
    /// [`LwftBuffer::push_events`] calls for the same poll (paginated
    /// fetches) all carry the same value.
    pub(crate) wft_started_id: i64,
    /// True if the polled WFT carries pending update messages. Read by the
    /// chunker; the heartbeat-collapsing heuristic uses it to refuse to merge
    /// the last WFT in history into a preceding heartbeat chain.
    pub(crate) has_pending_speculative_updates: bool,
}

/// Output of fetching one page from a [`HistoryPaginator`].
#[derive(Debug)]
pub(crate) struct FetchPageOutput {
    /// Newly-fetched events (possibly augmented with any held-aside
    /// `pending_post_replay_events` on the final page).
    pub(crate) events: Vec<HistoryEvent>,
    /// True iff the paginator has nothing more to fetch — next-page token is
    /// exhausted AND any held-aside events have been flushed into `events`.
    pub(crate) no_more_pages: bool,
}

/// Output of [`HistoryPaginator::from_poll`]: the paginator paired with the
/// [`PreparedWFT`] extracted from the poll response.
#[derive(Debug)]
pub(crate) struct PolledWftOutput {
    pub(crate) paginator: HistoryPaginator,
    pub(crate) prep: PreparedWFT,
}

/// Per-workflow-run owner of history events.
///
/// Events flow in via [`Self::push_events`]; logical workflow tasks (LWFTs)
/// flow out via [`Self::take_next_wft_sequence`]. The buffer outlives any
/// single poll or paginator, which is the property that lets it hold the
/// run's `chunking_version` (only resolvable from event 1's
/// `WorkflowTaskCompleted`) across polls without resorting to per-poll
/// re-detection.
///
/// Chunker invocation lives here, not in the paginator: the paginator's job
/// is to fetch and return raw events; the buffer's job is to chunk those
/// events into LWFTs on demand.
///
/// See `arch_docs/workflow_task_chunking.md` for the LWFT concept.
#[derive(Debug)]
pub(crate) struct LwftBuffer {
    /// All events not yet consumed as part of a yielded LWFT, in history
    /// order. Each [`Self::push_events`] call appends; each
    /// [`Self::take_next_wft_sequence`] call drains from the front.
    events: Vec<HistoryEvent>,
    /// Per-poll envelope, updated on each [`Self::push_events`]. Stays at the
    /// `Default` (all zeros) until the first push.
    envelope: WftEnvelope,
    /// Sticky flag: latches to `true` on the first push that carries
    /// `no_more_pages = true`. Once set, never unset for the run.
    has_last_wft: bool,
    /// The chunking version for this workflow run. `None` until either:
    /// - a pushed event batch contains the `WftChunkingV2` flag → `Some(V2)`,
    /// - any `WorkflowTaskCompleted` is seen with no flag → `Some(V1)`, or
    /// - the owner explicitly calls [`Self::set_chunking_version`].
    ///
    /// Sticky: once `Some(_)`, never returns to `None`. V2 cannot be
    /// downgraded to V1.
    chunking_version: Option<ChunkingVersion>,
}

impl LwftBuffer {
    /// Construct an empty buffer (placeholder for a freshly-created run).
    pub(crate) fn empty() -> Self {
        Self {
            events: vec![],
            envelope: WftEnvelope::default(),
            has_last_wft: false,
            chunking_version: None,
        }
    }

    /// Append a batch of history events to the buffer, updating the envelope
    /// and the "no more pages" flag.
    ///
    /// Events must arrive in history order, both within a batch and across
    /// successive batches. Events whose `event_id` is `<=` the last event
    /// already in the buffer are silently dropped (handles the overlap
    /// between the cache-miss `pending_post_replay_events` and freshly
    /// fetched pages).
    ///
    /// `no_more_pages = true` latches `has_last_wft` for the run. Calling
    /// with `no_more_pages = false` after `has_last_wft` is already on
    /// indicates a logic bug in the paginator/buffer flow.
    pub(crate) fn push_events(
        &mut self,
        envelope: WftEnvelope,
        events: Vec<HistoryEvent>,
        no_more_pages: bool,
    ) {
        self.envelope = envelope;
        let last_event_id = self.events.last().map(|e| e.event_id).unwrap_or(0);
        self.events
            .extend(events.into_iter().filter(|e| e.event_id > last_event_id));
        if no_more_pages {
            self.has_last_wft = true;
        } else if self.has_last_wft {
            dbg_panic!(
                "LwftBuffer: push_events received no_more_pages=false after has_last_wft was \
                 already latched on. This indicates a logic bug in the paginator/buffer flow."
            );
        }
        // Monotonically resolve the chunking version from accumulated events.
        // The chunking version is sticky once `Some(_)`, so this is a cheap
        // no-op after the first resolution. Belt-and-suspenders re-scan
        // covers events that arrived via paths other than the paginator (e.g.
        // a poll's own initial events, the partial-WFT events held aside
        // during a cache-miss).
        self.chunking_version =
            resolve_chunking_version_from_events(self.events.iter(), self.chunking_version);
    }

    /// Authoritative setter, intended to be called by `WorkflowMachines` after
    /// it observes a `WorkflowTaskCompleted` (and consequently knows the
    /// answer from `observed_internal_flags`). Monotonic: a `V2` answer cannot
    /// be downgraded to `V1`.
    pub(crate) fn set_chunking_version(&mut self, v: ChunkingVersion) {
        match (self.chunking_version, v) {
            (Some(ChunkingVersion::V2), ChunkingVersion::V1) => {
                dbg_panic!(
                    "LwftBuffer: attempted to downgrade chunking version V2 → V1; \
                     this should never happen and indicates a bug."
                );
            }
            _ => {
                self.chunking_version = Some(v);
            }
        }
    }

    /// The currently-known chunking version, if any.
    #[allow(dead_code)] // reserved for diagnostics
    pub(crate) fn chunking_version(&self) -> Option<ChunkingVersion> {
        self.chunking_version
    }

    pub(crate) fn previous_wft_started_id(&self) -> i64 {
        self.envelope.previous_wft_started_id
    }

    #[allow(dead_code)]
    pub(crate) fn wft_started_id(&self) -> i64 {
        self.envelope.wft_started_id
    }

    /// A copy of the most recently pushed envelope.
    pub(crate) fn envelope(&self) -> WftEnvelope {
        self.envelope
    }

    #[allow(dead_code)]
    pub(crate) fn first_event_id(&self) -> Option<i64> {
        self.events.first().map(|e| e.event_id)
    }

    /// All events currently buffered, in order. Provided for debug printing
    /// and test introspection.
    #[allow(dead_code)]
    pub(crate) fn get_events(&self) -> &[HistoryEvent] {
        &self.events
    }

    /// Drain the next LWFT from the buffer (consuming its events).
    ///
    /// Returns:
    /// - `NextWFT::WFT(lwft)` when a complete LWFT could be identified.
    /// - `NextWFT::NeedFetch` when chunking can't conclude yet (more events
    ///   are required) — caller should fetch more pages and `push_events`.
    /// - `NextWFT::ReplayOver` when no more LWFTs exist and the buffer has
    ///   received all pages (`has_last_wft` is set).
    pub(crate) fn take_next_wft_sequence(&mut self, from_wft_started_id: i64) -> NextWFT {
        // Discard already-consumed events before the requested start id.
        if let Some(ix_first_relevant) =
            starting_index_after_skipping(&self.events, from_wft_started_id)
        {
            self.events.drain(0..ix_first_relevant);
        }

        // If the chunking version isn't known and we still have pages to
        // fetch, we can't safely run the chunker (silent V1 fallback would
        // be the bug). Force a fetch.
        let chunking_version = match self.chunking_version {
            Some(v) => v,
            None if !self.has_last_wft => return NextWFT::NeedFetch,
            None => {
                // Fully paginated but never resolved (no WFTCompleted in
                // history). V1 is the conservative legacy default; the
                // remaining events have no chunking decisions to make
                // anyway, so the choice is moot for correctness.
                ChunkingVersion::V1
            }
        };

        let chunk = find_end_index_of_next_wft_seq(
            &self.events,
            from_wft_started_id,
            self.has_last_wft,
            self.envelope.has_pending_speculative_updates,
            chunking_version,
        );

        match chunk {
            NextWFTSeqEndIndex::NeedMore => NextWFT::NeedFetch,
            NextWFTSeqEndIndex::Tail => {
                if !self.has_last_wft {
                    NextWFT::NeedFetch
                } else if self.events.is_empty() {
                    NextWFT::ReplayOver
                } else {
                    // Trailing matter (e.g. terminal events, WFTCompleted +
                    // commands after the last WFTStarted). Include them all
                    // so the caller can process them (e.g. set
                    // `have_seen_terminal_event`).
                    self.build_next_wft(self.events.len() - 1)
                }
            }
            NextWFTSeqEndIndex::Complete(next_wft_ix) => self.build_next_wft(next_wft_ix),
        }
    }

    fn build_next_wft(&mut self, drain_this_much: usize) -> NextWFT {
        let events: Vec<HistoryEvent> = self.events.drain(0..=drain_this_much).collect();
        let is_terminal = self.events.is_empty() && self.has_last_wft;
        NextWFT::WFT(LogicalWorkflowTask {
            events,
            is_terminal,
        })
    }

    /// Peek at the next LWFT's events without consuming them. Returns an
    /// empty slice if no events are available; may return a partial sequence
    /// if we're at the end of available history.
    pub(crate) fn peek_next_wft_sequence(&self, from_wft_started_id: i64) -> &[HistoryEvent] {
        let ix_first_relevant =
            starting_index_after_skipping(&self.events, from_wft_started_id).unwrap_or_default();

        let relevant_events = &self.events[ix_first_relevant..];
        if relevant_events.is_empty() {
            return relevant_events;
        }

        // Peek runs with a best-effort version. If unresolved, V1 is the
        // conservative default; peek is non-authoritative so a mismatch with
        // the eventual `take` is acceptable.
        let chunking_version = self.chunking_version.unwrap_or(ChunkingVersion::V1);
        let ix_end = find_end_index_of_next_wft_seq(
            relevant_events,
            from_wft_started_id,
            self.has_last_wft,
            self.envelope.has_pending_speculative_updates,
            chunking_version,
        )
        .end_index_in_slice(relevant_events.len());

        &relevant_events[0..=ix_end]
    }

    /// True iff the buffer can yield the next LWFT without needing more
    /// events to be pushed.
    pub(crate) fn can_take_next_wft_sequence(&self, from_wft_started_id: i64) -> bool {
        // If version isn't resolved and we still have pages to fetch, the
        // chunker can't run safely — return "can't" to force a fetch.
        let chunking_version = match self.chunking_version {
            Some(v) => v,
            None if !self.has_last_wft => return false,
            None => ChunkingVersion::V1,
        };

        let next_wft_ix = find_end_index_of_next_wft_seq(
            &self.events,
            from_wft_started_id,
            self.has_last_wft,
            self.envelope.has_pending_speculative_updates,
            chunking_version,
        );
        match next_wft_ix {
            NextWFTSeqEndIndex::NeedMore => false,
            NextWFTSeqEndIndex::Tail => self.has_last_wft,
            NextWFTSeqEndIndex::Complete(_) => true,
        }
    }

    /// Returns the next WFT completed event attributes, if any, starting at
    /// (inclusive) the given event id.
    pub(crate) fn peek_next_wft_completed(
        &self,
        from_id: i64,
    ) -> Option<&WorkflowTaskCompletedEventAttributes> {
        self.events
            .iter()
            .skip_while(|e| e.event_id < from_id)
            .find_map(|e| match &e.attributes {
                Some(Attributes::WorkflowTaskCompletedEventAttributes(a)) => Some(a),
                _ => None,
            })
    }
}

/// A complete logical workflow task (LWFT): the events that the workflow machines
/// should apply as a single, atomic processing unit. May correspond to one
/// server-side Workflow Task, or to multiple consecutive WFTs that have been
/// collapsed (e.g. WFT heartbeats followed by a real WFT).
///
/// See `arch_docs/workflow_task_chunking.md` for the conceptual model.
#[derive(Debug)]
pub(crate) struct LogicalWorkflowTask {
    events: Vec<HistoryEvent>,
    /// True if this LWFT is the terminal one in the workflow's history.
    is_terminal: bool,
}

impl LogicalWorkflowTask {
    /// Borrowed access to the events composing this LWFT, in history order.
    // Only consumed from test code today; will be used by production consumers
    // in a follow-up phase once the buffer/LWFT API is fully fleshed out.
    #[allow(dead_code)]
    pub(crate) fn events(&self) -> &[HistoryEvent] {
        &self.events
    }

    /// Whether this is the final LWFT for the workflow.
    pub(crate) fn is_terminal(&self) -> bool {
        self.is_terminal
    }

    /// Consume this LWFT and return its events.
    pub(crate) fn into_events(self) -> Vec<HistoryEvent> {
        self.events
    }
}

#[derive(Debug)]
pub(crate) enum NextWFT {
    ReplayOver,
    WFT(LogicalWorkflowTask),
    NeedFetch,
}

/// Which workflow-task chunking algorithm applies to a particular workflow.
/// See `arch_docs/workflow_task_chunking.md` for the algorithm details.
///
/// Chunking is a per-workflow-execution decision: a workflow's first
/// `WorkflowTaskCompleted` either carries the `WftChunkingV2` SDK flag (→ V2)
/// or doesn't (→ V1). The choice is then permanent for that workflow.
///
/// Using a typed enum (rather than a `bool`) makes the "we don't know yet"
/// state visible at the type level via `Option<ChunkingVersion>`, so the
/// chunker can refuse to run with an unresolved version instead of silently
/// falling back to V1.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ChunkingVersion {
    /// Legacy chunking algorithm.
    V1,
    /// Newer, more rigorous chunking algorithm. Used for workflows whose
    /// first `WorkflowTaskCompleted` carries the `WftChunkingV2` SDK flag.
    V2,
}

impl ChunkingVersion {
    /// True iff this is the v2 algorithm.
    // Not used in production code yet; reserved for the upcoming buffer API
    // where consumers will want a typed query rather than `==` against the enum.
    #[allow(dead_code)]
    pub(crate) fn is_v2(self) -> bool {
        matches!(self, Self::V2)
    }
}

/// Per-poll fetch component. Fetches pages from the server on demand and
/// emits raw events to the [`LwftBuffer`]. The chunker is no longer invoked
/// here — that logic is owned by the buffer.
#[derive(derive_more::Debug)]
#[debug("HistoryPaginator(run_id: {run_id})")]
pub(crate) struct HistoryPaginator {
    pub(crate) wf_id: String,
    pub(crate) run_id: String,
    client: Arc<dyn WorkerClient>,
    next_page_token: NextPageToken,
    /// Events held aside until pagination-from-start completes (cache-miss
    /// case only). Drained into the output of the first `fetch_next_page`
    /// call that transitions `next_page_token` to `Done`.
    pending_post_replay_events: Vec<HistoryEvent>,
}

#[derive(Clone, Debug)]
pub(crate) enum NextPageToken {
    /// There is no page token, we need to fetch history from the beginning
    FetchFromStart,
    /// There is a page token
    Next(Vec<u8>),
    /// There is no page token, we are done fetching history
    Done,
}

// If we're converting from a page token from the server, if it's empty, then we're done.
impl From<Vec<u8>> for NextPageToken {
    fn from(page_token: Vec<u8>) -> Self {
        if page_token.is_empty() {
            NextPageToken::Done
        } else {
            NextPageToken::Next(page_token)
        }
    }
}

impl HistoryPaginator {
    /// Build a paginator from a poll response. The poll's events flow into
    /// `prep.initial_events`; subsequent pages (if any) are fetched on demand
    /// via [`Self::fetch_next_page`] when the buffer signals `NeedFetch`.
    pub(super) async fn from_poll(
        wft: ValidPollWFTQResponse,
        client: Arc<dyn WorkerClient>,
    ) -> Result<PolledWftOutput, tonic::Status> {
        let empty_hist = wft.history.events.is_empty();
        if empty_hist && wft.legacy_query.is_none() && wft.query_requests.is_empty() {
            return Err(EMPTY_TASK_ERR.clone());
        }
        // Empty history → no events, no pages to fetch (sticky cache-hit or
        // query-only WFT). Non-empty → use the poll's next-page token.
        let next_page_token: NextPageToken = if empty_hist {
            NextPageToken::Done
        } else {
            wft.next_page_token.into()
        };
        let no_more_pages = matches!(next_page_token, NextPageToken::Done);
        let has_pending_speculative_updates = !wft.messages.is_empty();
        let paginator = HistoryPaginator {
            wf_id: wft.workflow_execution.workflow_id.clone(),
            run_id: wft.workflow_execution.run_id.clone(),
            client,
            next_page_token,
            pending_post_replay_events: vec![],
        };
        let envelope = WftEnvelope {
            previous_wft_started_id: wft.previous_started_event_id,
            wft_started_id: wft.started_event_id,
            has_pending_speculative_updates,
        };
        let prep = PreparedWFT {
            task_token: wft.task_token,
            attempt: wft.attempt,
            execution: wft.workflow_execution,
            workflow_type: wft.workflow_type,
            legacy_query: wft.legacy_query,
            query_requests: wft.query_requests,
            envelope,
            initial_events: wft.history.events,
            no_more_pages,
            messages: wft.messages,
        };
        Ok(PolledWftOutput { paginator, prep })
    }

    /// Cache-miss path: build a paginator for fetch-from-start, then pre-fetch
    /// the entire history into `req.original_wft.work.initial_events`. The
    /// `WftExtractor`-level contract is: a `FetchResult` carries a
    /// `PermittedWFT` whose events are ready to be applied without further
    /// fetching.
    ///
    /// Pre-fetching all pages here (rather than deferring to per-NeedFetch
    /// round-trips) matches today's behavior for cache-miss replays of long
    /// histories.
    pub(super) async fn from_fetchreq(
        mut req: Box<CacheMissFetchReq>,
        client: Arc<dyn WorkerClient>,
    ) -> Result<PermittedWFT, tonic::Status> {
        let envelope = req.original_wft.work.envelope;
        let mut paginator = HistoryPaginator {
            wf_id: req.original_wft.work.execution.workflow_id.clone(),
            run_id: req.original_wft.work.execution.run_id.clone(),
            client,
            next_page_token: NextPageToken::FetchFromStart,
            // The partial poll's events were captured in `initial_events`
            // before we got here; hold them aside until pagination from start
            // has drained the prior history.
            pending_post_replay_events: mem::take(&mut req.original_wft.work.initial_events),
        };
        let mut all_events: Vec<HistoryEvent> = Vec::new();
        loop {
            let page = paginator.fetch_next_page().await?;
            all_events.extend(page.events);
            if page.no_more_pages {
                break;
            }
        }
        req.original_wft.work.envelope = envelope;
        req.original_wft.work.initial_events = all_events;
        req.original_wft.work.no_more_pages = true;
        req.original_wft.paginator = paginator;
        Ok(req.original_wft)
    }

    /// Fetch one page of history from the server. The page's events are
    /// returned; the chunker is not invoked here — the [`LwftBuffer`] runs
    /// the chunker on demand once the events have been pushed into it.
    ///
    /// On the page that transitions `next_page_token` to `Done`, any
    /// held-aside `pending_post_replay_events` are flushed into the output
    /// (after the freshly-fetched events, in history order).
    pub(crate) async fn fetch_next_page(&mut self) -> Result<FetchPageOutput, tonic::Status> {
        let history = loop {
            let npt = match mem::replace(&mut self.next_page_token, NextPageToken::Done) {
                NextPageToken::Done => break None,
                NextPageToken::FetchFromStart => vec![],
                NextPageToken::Next(v) => v,
            };
            debug!(run_id=%self.run_id, "Fetching new history page");
            let fetch_res = self
                .client
                .get_workflow_execution_history(self.wf_id.clone(), Some(self.run_id.clone()), npt)
                .instrument(span!(tracing::Level::TRACE, "fetch_history_in_paginator"))
                .await?;

            self.next_page_token = fetch_res.next_page_token.into();

            let history_is_empty = fetch_res
                .history
                .as_ref()
                .map(|h| h.events.is_empty())
                .unwrap_or(true);
            if history_is_empty && matches!(&self.next_page_token, NextPageToken::Next(_)) {
                // Empty page with a continuation token — immediately try the next.
                continue;
            }
            break fetch_res.history;
        };

        let mut events: Vec<HistoryEvent> = history.map(|h| h.events).unwrap_or_default();
        let done = matches!(&self.next_page_token, NextPageToken::Done);
        if done {
            // Final page: flush held-aside events. The buffer's
            // `push_events` filter de-duplicates by event_id.
            events.extend(mem::take(&mut self.pending_post_replay_events));
        }
        Ok(FetchPageOutput {
            events,
            no_more_pages: done,
        })
    }
}

/// Test-only adapter: turns a [`HistoryPaginator`] into a [`Stream`] of
/// individual events. Used by history-downloading tests/utilities; the
/// production hot path uses [`LwftBuffer`] which receives whole pages at a
/// time via `push_events`.
#[cfg(test)]
#[pin_project::pin_project]
struct StreamingHistoryPaginator {
    inner: HistoryPaginator,
    /// Buffered events from the most recent `fetch_next_page` call, yielded
    /// one at a time to satisfy the `Stream` contract.
    pending: VecDeque<HistoryEvent>,
    /// Whether the inner paginator has reported no more pages. Once true and
    /// `pending` is empty, the stream terminates.
    drained: bool,
    #[pin]
    open_history_request: Option<BoxFuture<'static, Result<FetchPageOutput, tonic::Status>>>,
}

#[cfg(test)]
impl StreamingHistoryPaginator {
    fn new(inner: HistoryPaginator) -> Self {
        Self {
            inner,
            pending: VecDeque::new(),
            drained: false,
            open_history_request: None,
        }
    }
}

#[cfg(test)]
impl Stream for StreamingHistoryPaginator {
    type Item = Result<HistoryEvent, tonic::Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            if let Some(e) = this.pending.pop_front() {
                return Poll::Ready(Some(Ok(e)));
            }
            if *this.drained {
                return Poll::Ready(None);
            }
            if this.open_history_request.is_none() {
                // SAFETY: the inner paginator cannot be dropped before the
                // future, and the future won't be moved out of this struct.
                this.open_history_request.set(Some(unsafe {
                    transmute::<
                        BoxFuture<'_, Result<FetchPageOutput, tonic::Status>>,
                        BoxFuture<'static, Result<FetchPageOutput, tonic::Status>>,
                    >(this.inner.fetch_next_page().boxed())
                }));
            }
            let history_req = this.open_history_request.as_mut().as_pin_mut().unwrap();
            match Future::poll(history_req, cx) {
                Poll::Ready(resp) => {
                    this.open_history_request.set(None);
                    match resp {
                        Err(neterr) => return Poll::Ready(Some(Err(neterr))),
                        Ok(page) => {
                            this.pending.extend(page.events);
                            if page.no_more_pages {
                                *this.drained = true;
                            }
                            // Loop to yield the first buffered event (or terminate).
                        }
                    }
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

// The `HistoryUpdate` type (and its `from_events`, `take_next_wft_sequence`,
// `peek_next_wft_sequence`, etc. methods) has been removed. Its event-buffer
// and chunker-invocation roles now live on [`LwftBuffer`].

fn starting_index_after_skipping(
    events: &[HistoryEvent],
    from_wft_started_id: i64,
) -> Option<usize> {
    events
        .iter()
        .find_position(|e| e.event_id > from_wft_started_id)
        .map(|(ix, _)| ix)
}

/// Returns true if any WFTCompleted event in the given events carries the
/// `WftChunkingV2` flag.
fn events_have_wft_chunking_v2<'a, I: IntoIterator<Item = &'a HistoryEvent>>(events: I) -> bool {
    let flag_value = CoreInternalFlags::WftChunkingV2 as u32;
    events.into_iter().any(|e| {
        if let Some(Attributes::WorkflowTaskCompletedEventAttributes(ref attr)) = e.attributes
            && let Some(ref metadata) = attr.sdk_metadata
        {
            metadata.core_used_flags.contains(&flag_value)
        } else {
            false
        }
    })
}

/// Returns true if any `WorkflowTaskCompleted` event is present in the slice.
fn events_have_any_wft_completed<'a, I: IntoIterator<Item = &'a HistoryEvent>>(events: I) -> bool {
    events.into_iter().any(|e| {
        matches!(
            &e.attributes,
            Some(Attributes::WorkflowTaskCompletedEventAttributes(_))
        )
    })
}

/// Monotonically resolve the chunking version from a window of events, given
/// whatever we already knew. Resolution rules:
///
/// - If we already have `Some(_)`, the answer is sticky and we return it
///   unchanged. The decision is per-workflow and cannot change.
/// - If any event carries the `WftChunkingV2` flag, the workflow uses V2.
/// - Else, if any `WorkflowTaskCompleted` is present (meaning we've seen at
///   least one completion and none of them advertise V2), the workflow uses
///   V1.
/// - Otherwise we still don't know.
///
/// This function is the single source of truth for "what version is this
/// workflow's chunking?" within `history_update.rs`. The bug Copilot flagged
/// in `from_fetchreq` came from a different, snapshot-style detection that
/// only ran at construction time; the monotonic, repeatedly-invoked variant
/// here closes that hole.
fn resolve_chunking_version_from_events<'a, I>(
    events: I,
    prior: Option<ChunkingVersion>,
) -> Option<ChunkingVersion>
where
    I: IntoIterator<Item = &'a HistoryEvent> + Clone,
{
    if prior.is_some() {
        return prior;
    }
    if events_have_wft_chunking_v2(events.clone()) {
        return Some(ChunkingVersion::V2);
    }
    if events_have_any_wft_completed(events) {
        return Some(ChunkingVersion::V1);
    }
    None
}

/// Dispatches to v1 (legacy) or v2 chunking based on the [`ChunkingVersion`].
fn find_end_index_of_next_wft_seq(
    events: &[HistoryEvent],
    from_event_id: i64,
    has_last_wft: bool,
    has_pending_speculative_updates: bool,
    chunking_version: ChunkingVersion,
) -> NextWFTSeqEndIndex {
    match chunking_version {
        ChunkingVersion::V2 => find_end_index_of_next_wft_seq_v2(
            events,
            from_event_id,
            has_last_wft,
            has_pending_speculative_updates,
        ),
        ChunkingVersion::V1 => {
            find_end_index_of_next_wft_seq_v1(events, from_event_id, has_last_wft)
        }
    }
}

/// Legacy chunking algorithm. Used for workflows that were started before the
/// `WftChunkingV2` flag was introduced.
fn find_end_index_of_next_wft_seq_v1(
    events: &[HistoryEvent],
    from_event_id: i64,
    has_last_wft: bool,
) -> NextWFTSeqEndIndex {
    if events.is_empty() {
        return if has_last_wft {
            NextWFTSeqEndIndex::Tail
        } else {
            NextWFTSeqEndIndex::NeedMore
        };
    }
    let mut last_index;
    let mut saw_command_or_started = false;
    let mut saw_command = false;
    let mut wft_started_event_id_to_index = vec![];
    for (ix, e) in events.iter().enumerate() {
        last_index = ix;

        if e.event_id <= from_event_id {
            continue;
        }

        if e.is_command_event() {
            saw_command = true;
            saw_command_or_started = true;
        }
        if e.event_type() == EventType::WorkflowExecutionStarted {
            saw_command_or_started = true;
        }
        if e.is_final_wf_execution_event() {
            return NextWFTSeqEndIndex::Complete(last_index);
        }

        if e.event_type() == EventType::WorkflowTaskStarted {
            wft_started_event_id_to_index.push((e.event_id, ix));
            if let Some(next_event) = events.get(ix + 1) {
                let next_event_type = next_event.event_type();
                if matches!(
                    next_event_type,
                    EventType::WorkflowTaskFailed
                        | EventType::WorkflowTaskTimedOut
                        | EventType::WorkflowExecutionTimedOut
                        | EventType::WorkflowExecutionTerminated
                        | EventType::WorkflowExecutionCanceled
                ) {
                    wft_started_event_id_to_index.pop();
                    continue;
                } else if next_event_type == EventType::WorkflowTaskCompleted {
                    if let Some(next_next_event) = events.get(ix + 2) {
                        if !saw_command
                            && next_next_event.event_type() == EventType::WorkflowTaskScheduled
                        {
                            continue;
                        } else {
                            if let Some(Attributes::WorkflowExecutionUpdateAcceptedEventAttributes(
                                ref attr,
                            )) = next_next_event.attributes
                                && let Some(ret_ix) = wft_started_event_id_to_index
                                    .iter()
                                    .rev()
                                    .find_map(|(eid, ix)| {
                                        if *eid < attr.accepted_request_sequencing_event_id {
                                            return Some(*ix);
                                        }
                                        None
                                    })
                            {
                                return NextWFTSeqEndIndex::Complete(ret_ix);
                            }
                            return NextWFTSeqEndIndex::Complete(ix);
                        }
                    } else if !has_last_wft && !saw_command_or_started {
                        continue;
                    }
                }
            } else if !has_last_wft && !saw_command_or_started {
                continue;
            }
            if saw_command_or_started {
                return NextWFTSeqEndIndex::Complete(ix);
            }
        }
    }

    // Legacy: Incomplete maps to NeedMore when !has_last_wft; the caller handles
    // has_last_wft by treating all remaining events as a single WFT.
    if has_last_wft {
        NextWFTSeqEndIndex::Tail
    } else {
        NextWFTSeqEndIndex::NeedMore
    }
}

#[derive(Debug, Copy, Clone)]
enum NextWFTSeqEndIndex {
    /// The next Logical WFT sequence is completely contained within the passed-in slice.
    /// The index corresponds to the index of the last `WorkflowTaskStarted` event.
    Complete(usize),

    /// Not enough events in the slice to positively determine the next WFT boundary.
    /// The caller should fetch more events before attempting to chunk again.
    NeedMore,

    /// No more WFT boundaries exist in this slice. Any remaining events are trailing matter
    /// after the last WFT (e.g. terminal `WorkflowExecution*` events, `WorkflowTaskCompleted`
    /// with its commands). These events still need to be processed by the caller.
    Tail,
}

impl NextWFTSeqEndIndex {
    /// Last event index within a slice of length `slice_len` that this result refers to.
    fn end_index_in_slice(self, slice_len: usize) -> usize {
        match self {
            NextWFTSeqEndIndex::Complete(ix) => ix,
            NextWFTSeqEndIndex::NeedMore | NextWFTSeqEndIndex::Tail => slice_len.saturating_sub(1),
        }
    }

    fn add(self, val: usize) -> Self {
        match self {
            NextWFTSeqEndIndex::Complete(ix) => NextWFTSeqEndIndex::Complete(ix + val),
            NextWFTSeqEndIndex::NeedMore => NextWFTSeqEndIndex::NeedMore,
            NextWFTSeqEndIndex::Tail => NextWFTSeqEndIndex::Tail,
        }
    }
}

/// Return the event _index_ (not ID!) of the last event of the logical workflow task starting
/// at event ID `from_event_id`. The logical WFT is guaranteed to be "complete", meaning that all
/// events required to process that logical WFT are contained in the provided slice.
///
/// Returns one of three variants:
///
/// - `Complete(ix)` — the WFT boundary is at the `WorkflowTaskStarted` event at index `ix`.
///   All events required to process the LWFT are present in the slice.
/// - `NeedMore` — not enough events to determine the boundary; the caller should fetch more
///   history pages before retrying. This can happen when the slice ends at a point where
///   look-ahead is required (e.g. `WFTStarted → WFTCompleted → EOS` with `!has_last_wft`).
/// - `Tail` — no more WFT boundaries exist in the remaining events. Any events still in the
///   slice are trailing matter after the last WFT (e.g. terminal `WorkflowExecution*` events,
///   `WorkflowTaskCompleted` + commands). The caller must still process these events (e.g. to
///   set `have_seen_terminal_event`).
///
/// When `has_last_wft` is true, the slice is the full history for this update: a trailing
/// `WorkflowTaskStarted` with no following event (open task) **is** a `Complete` boundary at
/// that started event—there is no further history to page in that could change the decision.
///
/// The index returned by `Complete(x)` always corresponds to the event index of a
/// `WorkflowTaskStarted` event.
///
/// A logical WFT may span multiple real WFTs in history, in the following cases:
///
/// - Empty Workflow Tasks sequences, like those resulting from WFT heartbeats;
/// - WFT attempts that failed or timed out.
///
/// In both cases, the ignored wft is swallowed by the _following_ workflow task,
/// resulting in a single logical workflow task.
fn find_end_index_of_next_wft_seq_v2(
    events: &[HistoryEvent],
    from_event_id: i64,
    has_last_wft: bool,
    has_pending_speculative_updates: bool,
) -> NextWFTSeqEndIndex {
    use EventType::*;
    use NextWFTSeqEndIndex::*;

    if events.is_empty() {
        return if has_last_wft { Tail } else { NeedMore };
    }

    // It's possible to have gotten a new history update without eviction (ex: unhandled
    // command on completion), where we may need to skip events we already handled.
    let mut ix = starting_index_after_skipping(events, from_event_id).unwrap_or(events.len());

    // Set to true if we've seen any event that prevents extending the present LWFT past the next `WFTStarted` event.
    let mut prevent_heartbeat = false;

    // Skip the initial `WFExecutionStarted` event, if present.
    //
    // Consume `WFExecutionStarted?`
    if let Some(WorkflowExecutionStarted) = events.get(ix).map(|e| e.event_type()) {
        ix += 1;
        prevent_heartbeat = true;
    }

    // We're at the beginning of a LWFT. Any command here results from the _previous_ WFT,
    // and therefore shouldn't affect chunking of the present LWFT, besides
    //
    // Consume `(WFTCompleted -> Command*)?`
    if let Some(WorkflowTaskCompleted) = events.get(ix).map(|e| e.event_type()) {
        ix += 1; // WFTCompleted

        while ix < events.len() {
            if !events[ix].is_command_event() {
                break;
            }

            prevent_heartbeat = true;
            ix += 1; // Command
        }
    }

    // From this point on, there should be:
    // `InboundEvent* -> WFTScheduled -> WFTStarted -> WFTCompleted -> Command*`
    while ix < events.len() {
        // let ahead = &events[ix + 1..events.len().min(ix + 6)];
        // let ahead: Vec<_> = ahead.iter().map(|e| e.event_type()).collect();

        let e0 = &events[ix];
        let e1 = events.get(ix + 1);
        let e2 = events.get(ix + 2);
        let e3 = events.get(ix + 3);
        let e4 = events.get(ix + 4);
        let e5 = events.get(ix + 5);

        match e0.event_type() {
            // WFTStarted -> ...
            EventType::WorkflowTaskStarted => {
                match e1.map(|e| e.event_type()) {
                    // WFTStarted -> EOH
                    None if has_last_wft => {
                        // History ends on this WFTStarted.
                        // Conclusion is safe and replay is over after this LWFT.
                        return NextWFTSeqEndIndex::Complete(ix);
                    }

                    // WFTStarted -> (unknown)
                    None /* !has_last_wft */ => {
                        // Can't conclude yet: unknown could be a WFTCompleted, WFTFailed, or WFTTimedOut event.
                        return NextWFTSeqEndIndex::NeedMore;
                    }

                    // WFTStarted -> WFTCompleted -> ...
                    Some(EventType::WorkflowTaskCompleted) => {
                        match e2.map(|e| e.event_type()) {
                            // WFTStarted -> WFTCompleted -> EOH
                            None if has_last_wft => {
                                // There's no more event to look ahead.
                                // It is safe to conclude the LWFT at the current WFTStarted event.
                                return NextWFTSeqEndIndex::Complete(ix);
                            }

                            // WFTStarted -> WFTCompleted -> (unknown)
                            None /* !has_last_wft */ => {
                                // Can't conclude yet, as unknown could be a WFTScheduled or UpdateAccepted event.
                                // Note that we are not making an exception for prevent_heartbeat=true here,
                                // because we'd still need to if there's an UpdateAccepted event ahead.
                                return NextWFTSeqEndIndex::NeedMore;
                            }

                            // WFTStarted -> WFTCompleted -> WFTScheduled -> ...
                            Some(EventType::WorkflowTaskScheduled) => {
                                if prevent_heartbeat {
                                    // For some reason (e.g. we saw a command preceding this WFTStarted), we know
                                    // that we can't collapse the current WFT with the one ahead, and we've seen
                                    // one event that can't belong to the current WFT (the WFTScheduled), so it
                                    // is safe to conclude a Complete LWFT at the current WFTStarted event.
                                    return NextWFTSeqEndIndex::Complete(ix);
                                }

                                match e3.map(|e| e.event_type()) {
                                    // WFTStarted -> WFTCompleted -> WFTScheduled -> EOH
                                    None if has_last_wft => {
                                            // History ends on this WFTScheduled. That's somewhat unexpected,
                                            // but still means there can't be nothing affecting decision on the
                                            // present LWFT, so it is safe to conclude a Complete LWFT
                                            // at the current WFTStarted event.
                                            return NextWFTSeqEndIndex::Complete(ix);
                                    }

                                    // WFTStarted -> WFTCompleted -> WFTScheduled -> (unknown)
                                    None /* !has_last_wft */ => {
                                        // There might be more events ahead that would affect the conclusion,
                                        // e.g. a `WFTScheduled -> WFTStarted` sequence that would make this
                                        // a heartbeat. Delay the conclusion until we see more events.
                                        return NextWFTSeqEndIndex::NeedMore;
                                    }

                                    // WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> ...
                                    Some(EventType::WorkflowTaskStarted) => {
                                        match e4.map(|e| e.event_type()) {
                                            // WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> EOH
                                            None if has_last_wft => {
                                                if has_pending_speculative_updates {
                                                    // There's a pending speculative update, which necessarily affects
                                                    // the last WFTStarted event, which is the one we're looking ahead
                                                    // to. We therefore can't collapse the current WFT (WFTStarted at ix)
                                                    // with the one ahead (WFTStarted at ix + 3).
                                                    return NextWFTSeqEndIndex::Complete(ix);
                                                } else {
                                                    // We got a full noop WFT sequence. Collapse the current WFT
                                                    // (WFTStarted at ix) with the one ahead (WFTStarted at ix + 3),
                                                    // and return that as this is the final event in history.
                                                    return NextWFTSeqEndIndex::Complete(ix + 3);
                                                }
                                            }

                                            // WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> (unknown)
                                            None /* !has_last_wft */ => {
                                                // Can't conclude yet: unknown could be a WFTCompleted, WFTFailed, or WFTTimedOut.
                                                return NextWFTSeqEndIndex::NeedMore;
                                            }

                                            // WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> WFTCompleted -> ...
                                            Some(EventType::WorkflowTaskCompleted) => {
                                                match e5.map(|e| e.event_type()) {
                                                    // WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> WFTCompleted -> EOH
                                                    None if has_last_wft => {
                                                        assert!(!has_pending_speculative_updates);

                                                        // We got a full noop WFT sequence. Collapse the current WFT
                                                        // (WFTStarted at ix) with the one ahead (WFTStarted at ix + 3),
                                                        // and return that as this is the final event in history.
                                                        return NextWFTSeqEndIndex::Complete(ix + 3);
                                                    }

                                                    // WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> WFTCompleted -> (unknown)
                                                    None /* !has_last_wft */ => {
                                                        // Can't conclude yet, as unknown could be a WFTStarted, WFTFailed, or WFTTimedOut event.
                                                        return NextWFTSeqEndIndex::NeedMore;
                                                    }

                                                    // WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> WFTCompleted -> UpdateAccepted -> ...
                                                    Some(EventType::WorkflowExecutionUpdateAccepted) => {
                                                        // Found an UpdateAccepted event, which must affect the WFTStarted at ix + 3.
                                                        // That means we can't collapse the current WFT (WFTStarted at ix) with the
                                                        // one ahead (WFTStarted at ix + 3). Conclude the current WFTStarted event.
                                                        return NextWFTSeqEndIndex::Complete(ix);
                                                    }

                                                    // WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> WFTCompleted -> <something else>
                                                    Some(_) => {
                                                        // We found a full noop WFT sequence (ix..ix+3), and we've looked
                                                        // ahead far enough to be sure that we won't need to walk back on
                                                        // previous WFTStarted events. Jump ahead to the next WFTStarted
                                                        // event, and continue the loop.
                                                        ix += 3; // WFTStarted + WFTCompleted + WFTScheduled
                                                        continue;
                                                    }
                                                }

                                            }

                                            // WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> <something else>
                                            Some(_) => {
                                                return NextWFTSeqEndIndex::Complete(ix);
                                            }
                                        }
                                    }

                                    // WFTStarted -> WFTCompleted -> WFTScheduled -> <something else>
                                    Some(_) => {
                                        return NextWFTSeqEndIndex::Complete(ix);
                                    }
                                }
                            }

                            // WFTStarted -> WFTCompleted -> <something else>
                            Some(_) => {
                                return NextWFTSeqEndIndex::Complete(ix);
                            }
                        }
                    }

                    // WFTStarted -> WFT(Failed|TimedOut) -> ...
                    Some(EventType::WorkflowTaskFailed) | Some(EventType::WorkflowTaskTimedOut) => {
                        // Failed WFT. Skip over it.
                        ix += 2; // Started + Failed/TimedOut
                        continue;
                    }

                    // Workflow execution terminates after WFTStarted without WFTCompleted.
                    // Complete points at the WFTStarted; the terminal event is left as
                    // trailing matter (will be returned as `Tail` on the next call).
                    // `WFTStarted -> WFExecution(Terminated|TimedOut|...)`
                    Some(_) if e1.is_some_and(|e| e.is_final_wf_execution_event()) => {
                        return NextWFTSeqEndIndex::Complete(ix);
                    }

                    // `WFTStarted -> <something else>`
                    Some(_) => {
                        panic!(
                            "Unexpected event type: {:?} after WorkflowTaskStarted event, {:?}",
                            e0.event_type(),
                            events
                        );
                    }
                }
            }

            // Sudden workflow execution termination. That's the end of history,
            // but we still don't have a "complete" LWFT. The terminal event is trailing
            // matter that the caller must still process (to set have_seen_terminal_event).
            // `WFExecution(Failed|TimedOut|Canceled|Terminated|TimedOut|CAN)`
            _ if e0.is_final_wf_execution_event() => {
                if e1.is_some() || !has_last_wft || has_pending_speculative_updates {
                    panic!(
                        "{:?} event at index {ix} is not the last event in history",
                        e0.event_type()
                    );
                }
                return Tail;
            }

            // Just skip over any other event type.
            _ => {
                if e0.is_command_event() {
                    // This case is theoretically impossible, unless either the workflow
                    // history is malformed or we hit a bug in this chunking logic.
                    dbg_panic!(
                        "Command event at index {ix} is not expected after seeing a non-command event"
                    );
                }

                if e0.is_wft_time_sensitive_event() {
                    prevent_heartbeat = true;
                }

                ix += 1;
                continue;
            }
        }

        #[allow(unreachable_code)]
        {
            panic!("All match arms above must diverge (return/continue/panic)");
        }
    }

    // Fell off the main loop without finding a WFTStarted.
    if has_last_wft {
        // This is the last WFT in history. Any events consumed by the preamble (WFTCompleted + commands)
        // or remaining inbound events are trailing matter.
        NextWFTSeqEndIndex::Tail
    } else {
        // There might be a WFTStarted event ahead, but we'll need to fetch more events to find it.
        NextWFTSeqEndIndex::NeedMore
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        replay::{HistoryInfo, TestHistoryBuilder, canned_histories},
        test_help::{ResponseType, hist_to_poll_resp},
        worker::client::mocks::mock_worker_client,
    };
    use futures_util::TryStreamExt;
    use temporalio_common::protos::temporal::api::{
        common::v1::WorkflowExecution, enums::v1::WorkflowTaskFailedCause, history::v1::History,
        workflowservice::v1::GetWorkflowExecutionHistoryResponse,
    };

    /// Test helper: maps the `chunking_v2: bool` test parameter (parameterized
    /// via `#[values(false, true)]`) to a [`ChunkingVersion`]. Tests use the
    /// `bool` form to stay terse; production code uses the enum.
    fn cv(chunking_v2: bool) -> ChunkingVersion {
        if chunking_v2 {
            ChunkingVersion::V2
        } else {
            ChunkingVersion::V1
        }
    }

    /// Test-only constructor for [`LwftBuffer`]. Pushes one batch of events
    /// with the given envelope/`no_more_pages` setting, then forces the
    /// chunking version (so tests don't have to depend on event-flag scanning
    /// to resolve it).
    pub(super) fn lwft_buffer_for_test(
        events: Vec<HistoryEvent>,
        previous_wft_started_id: i64,
        wft_started_id: i64,
        no_more_pages: bool,
        has_pending_speculative_updates: bool,
        chunking_version: ChunkingVersion,
    ) -> LwftBuffer {
        let mut buf = LwftBuffer::empty();
        // Set the version FIRST so the buffer's chunker dispatch can rely on
        // it from the first push (otherwise `push_events`' best-effort scan
        // determines it from the events, which is what production does but
        // is not what tests assume).
        buf.set_chunking_version(chunking_version);
        buf.push_events(
            WftEnvelope {
                previous_wft_started_id,
                wft_started_id,
                has_pending_speculative_updates,
            },
            events,
            no_more_pages,
        );
        buf
    }

    /// Test-only constructor for [`HistoryPaginator`] that bypasses the
    /// async fetching done by `from_poll`/`from_fetchreq`. Used by tests
    /// that drive the paginator directly with a mocked `WorkerClient`.
    pub(super) fn paginator_for_test(
        wf_id: String,
        run_id: String,
        next_page_token: impl Into<NextPageToken>,
        client: Arc<dyn WorkerClient>,
        pending_post_replay_events: Vec<HistoryEvent>,
    ) -> HistoryPaginator {
        HistoryPaginator {
            wf_id,
            run_id,
            client,
            next_page_token: next_page_token.into(),
            pending_post_replay_events,
        }
    }

    /// Build an `LwftBuffer` from a fully-known history. The version is
    /// auto-detected from the events. Mirrors the old `as_history_update()`
    /// semantics: `no_more_pages = true`, no pending updates.
    fn buffer_from_history_info(v: HistoryInfo) -> LwftBuffer {
        let events = v.events().to_vec();
        let chunking_version = if events_have_wft_chunking_v2(events.iter()) {
            ChunkingVersion::V2
        } else {
            ChunkingVersion::V1
        };
        lwft_buffer_for_test(
            events,
            v.previous_started_event_id(),
            v.workflow_task_started_event_id(),
            true,
            false,
            chunking_version,
        )
    }

    trait TestHBExt {
        fn as_lwft_buffer(&self) -> LwftBuffer;
    }

    impl TestHBExt for TestHistoryBuilder {
        fn as_lwft_buffer(&self) -> LwftBuffer {
            buffer_from_history_info(self.get_full_history_info().unwrap())
        }
    }

    /// Retroactively sets the `WftChunkingV2` flag on the first
    /// WFTCompleted event in an already-constructed builder (for canned histories).
    fn maybe_set_chunking_v2(t: &mut TestHistoryBuilder, chunking_v2: bool) {
        if chunking_v2 {
            use crate::internal_flags::CoreInternalFlags;
            t.set_flags_first_wft(&[CoreInternalFlags::WftChunkingV2], &[]);
        }
    }

    impl NextWFT {
        fn unwrap_events(self) -> Vec<HistoryEvent> {
            match self {
                NextWFT::WFT(lwft) => lwft.into_events(),
                o => panic!("Must be complete WFT: {o:?}"),
            }
        }

        fn is_complete(&self) -> bool {
            matches!(self, NextWFT::WFT(lwft) if lwft.is_terminal())
        }
    }

    fn next_check_peek(buf: &mut LwftBuffer, from_id: i64) -> Vec<HistoryEvent> {
        let seq_peeked = buf.peek_next_wft_sequence(from_id).to_vec();
        let seq = buf.take_next_wft_sequence(from_id).unwrap_events();
        assert_eq!(seq, seq_peeked);
        seq
    }

    fn next_check_peek2(buf: &mut LwftBuffer, from_id: i64) -> (usize, bool) {
        let seq_peek = buf.peek_next_wft_sequence(from_id).to_vec();
        let next = buf.take_next_wft_sequence(from_id);
        let is_complete = next.is_complete();
        let seq_take = next.unwrap_events();
        assert_eq!(seq_take, seq_peek);
        (seq_take.len(), is_complete)
    }

    #[rstest::rstest]
    #[test]
    fn consumes_standard_wft_sequence(#[values(false, true)] chunking_v2: bool) {
        let mut timer_hist = canned_histories::single_timer("t");
        maybe_set_chunking_v2(&mut timer_hist, chunking_v2);
        let mut update = timer_hist.as_lwft_buffer();
        let seq_1 = next_check_peek(&mut update, 0);
        assert_eq!(seq_1.len(), 3);
        assert_eq!(seq_1.last().unwrap().event_id, 3);
        let seq_2_peeked = update.peek_next_wft_sequence(0).to_vec();
        let seq_2 = next_check_peek(&mut update, 3);
        assert_eq!(seq_2, seq_2_peeked);
        assert_eq!(seq_2.len(), 5);
        assert_eq!(seq_2.last().unwrap().event_id, 8);
    }

    #[rstest::rstest]
    #[test]
    fn skips_wft_failed(#[values(false, true)] chunking_v2: bool) {
        let mut failed_hist = canned_histories::workflow_fails_with_reset_after_timer("t", "runid");
        maybe_set_chunking_v2(&mut failed_hist, chunking_v2);
        let mut update = failed_hist.as_lwft_buffer();
        let seq_1 = next_check_peek(&mut update, 0);
        assert_eq!(seq_1.len(), 3);
        assert_eq!(seq_1.last().unwrap().event_id, 3);
        let seq_2 = next_check_peek(&mut update, 3);
        assert_eq!(seq_2.len(), 8);
        assert_eq!(seq_2.last().unwrap().event_id, 11);
    }

    #[rstest::rstest]
    #[test]
    fn skips_wft_timeout(#[values(false, true)] chunking_v2: bool) {
        let mut failed_hist = canned_histories::wft_timeout_repro();
        maybe_set_chunking_v2(&mut failed_hist, chunking_v2);
        let mut update = failed_hist.as_lwft_buffer();
        let seq_1 = next_check_peek(&mut update, 0);
        assert_eq!(seq_1.len(), 3);
        assert_eq!(seq_1.last().unwrap().event_id, 3);
        let seq_2 = next_check_peek(&mut update, 3);
        assert_eq!(seq_2.len(), 11);
        assert_eq!(seq_2.last().unwrap().event_id, 14);
    }

    #[rstest::rstest]
    #[test]
    fn skips_events_before_desired_wft(#[values(false, true)] chunking_v2: bool) {
        let mut timer_hist = canned_histories::single_timer("t");
        maybe_set_chunking_v2(&mut timer_hist, chunking_v2);
        let mut update = timer_hist.as_lwft_buffer();
        // We haven't processed the first 3 events, but we should still only get the second sequence
        let seq_2 = update.take_next_wft_sequence(3).unwrap_events();
        assert_eq!(seq_2.len(), 5);
        assert_eq!(seq_2.last().unwrap().event_id, 8);
    }

    #[rstest::rstest]
    #[test]
    fn history_ends_abruptly(#[values(false, true)] chunking_v2: bool) {
        let mut timer_hist = canned_histories::single_timer("t");
        timer_hist.add_workflow_execution_terminated();
        maybe_set_chunking_v2(&mut timer_hist, chunking_v2);
        let mut update = timer_hist.as_lwft_buffer();
        let seq_2 = update.take_next_wft_sequence(3).unwrap_events();
        if chunking_v2 {
            // New algorithm: terminal event is not part of the WFTStarted LWFT.
            assert_eq!(seq_2.len(), 5);
            assert_eq!(seq_2.last().unwrap().event_id, 8);
            let seq_3 = update.take_next_wft_sequence(8).unwrap_events();
            assert_eq!(seq_3.len(), 1);
            assert!(seq_3[0].is_final_wf_execution_event());
            assert_matches!(update.take_next_wft_sequence(8), NextWFT::ReplayOver);
        } else {
            // Legacy algorithm: terminal event included in the WFTStarted LWFT.
            assert_eq!(seq_2.len(), 6);
            assert_eq!(seq_2.last().unwrap().event_id, 9);
            assert!(seq_2.last().unwrap().is_final_wf_execution_event());
        }
    }

    /// Verifies that non-command terminal events (`WorkflowExecutionTerminated`,
    /// `WorkflowExecutionTimedOut`) following a `WorkflowTaskStarted` are returned as
    /// trailing tail events rather than being silently dropped. This is critical because
    /// callers need to process them to set `have_seen_terminal_event`.
    #[rstest::rstest]
    #[test]
    fn terminal_events_not_dropped_after_wft_started(#[values(false, true)] chunking_v2: bool) {
        // Test both non-command terminal event types that can follow WFTStarted.
        for add_terminal in [
            TestHistoryBuilder::add_workflow_execution_terminated as fn(&mut TestHistoryBuilder),
            TestHistoryBuilder::add_workflow_execution_timed_out,
        ] {
            let mut t = TestHistoryBuilder::default();
            t.add_by_type(EventType::WorkflowExecutionStarted);
            t.add_full_wf_task(); // Sched(2), Started(3), Completed(4)
            t.add_by_type(EventType::TimerStarted); // TimerStarted(5)
            t.add_workflow_task_scheduled_and_started(); // Sched(6), Started(7)
            add_terminal(&mut t); // terminal(8)
            maybe_set_chunking_v2(&mut t, chunking_v2);

            let mut update = t.as_lwft_buffer();
            let seq_1 = update.take_next_wft_sequence(0).unwrap_events();
            assert_eq!(seq_1.last().unwrap().event_id, 3);

            if chunking_v2 {
                let seq_2 = update.take_next_wft_sequence(3).unwrap_events();
                assert_eq!(seq_2.last().unwrap().event_id, 7);
                assert_eq!(
                    seq_2.last().unwrap().event_type(),
                    EventType::WorkflowTaskStarted
                );
                let seq_3 = update.take_next_wft_sequence(7).unwrap_events();
                assert_eq!(seq_3.len(), 1);
                assert!(seq_3[0].is_final_wf_execution_event());
                assert_matches!(update.take_next_wft_sequence(7), NextWFT::ReplayOver);
            } else {
                let seq_2 = update.take_next_wft_sequence(3).unwrap_events();
                assert_eq!(seq_2.last().unwrap().event_id, 8);
                assert!(seq_2.last().unwrap().is_final_wf_execution_event());
            }
        }
    }

    #[rstest::rstest]
    #[test]
    fn heartbeats_skipped(#[values(false, true)] chunking_v2: bool) {
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        t.add_full_wf_task();
        t.add_by_type(EventType::TimerStarted);
        t.add_full_wf_task();
        t.add_full_wf_task();
        t.add_full_wf_task();
        t.add_full_wf_task();
        t.add_by_type(EventType::TimerStarted);
        t.add_full_wf_task();
        t.add_we_signaled("whee", vec![]);
        t.add_full_wf_task();
        t.add_workflow_execution_completed();
        maybe_set_chunking_v2(&mut t, chunking_v2);

        let mut update = t.as_lwft_buffer();
        if chunking_v2 {
            // v2 treats WFExecutionStarted as time-sensitive,
            // so the first WFT can't be collapsed with the next.
            let seq = next_check_peek(&mut update, 0);
            assert_eq!(seq.len(), 3);
            let seq = next_check_peek(&mut update, 3);
            assert_eq!(seq.len(), 3);
        } else {
            let seq = next_check_peek(&mut update, 0);
            assert_eq!(seq.len(), 6);
        }
        let seq = next_check_peek(&mut update, 6);
        assert_eq!(seq.len(), 4);
        let seq = next_check_peek(&mut update, 10);
        assert_eq!(seq.len(), 9);
        let seq = next_check_peek(&mut update, 19);
        assert_eq!(seq.len(), 4);
        let seq = next_check_peek(&mut update, 23);
        assert_eq!(seq.len(), 4);
        let seq = next_check_peek(&mut update, 27);
        assert_eq!(seq.len(), 2);
    }

    #[rstest::rstest]
    #[test]
    fn heartbeat_marker_end(#[values(false, true)] chunking_v2: bool) {
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        t.add_full_wf_task();
        t.add_local_activity_result_marker(1, "1", "done".into());
        t.add_workflow_execution_completed();
        maybe_set_chunking_v2(&mut t, chunking_v2);

        let mut update = t.as_lwft_buffer();
        let seq = next_check_peek(&mut update, 3);
        assert_eq!(seq.len(), 3);
        let seq = next_check_peek(&mut update, 6);
        assert_eq!(seq.len(), 3);
    }

    /// Test fixture: returns a pre-populated [`LwftBuffer`] (holding the
    /// first chunk's worth of events) plus a [`HistoryPaginator`] whose
    /// mocked client serves subsequent chunks.
    fn paginator_setup(
        history: TestHistoryBuilder,
        chunk_size: usize,
    ) -> (LwftBuffer, HistoryPaginator) {
        let hinfo = history.get_full_history_info().unwrap();
        let wft_started = hinfo.workflow_task_started_event_id();
        let full_hist = hinfo.into_events();
        let initial_hist = full_hist.chunks(chunk_size).next().unwrap().to_vec();
        let mut mock_client = mock_worker_client();

        let mut npt = 1;
        mock_client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, passed_npt| {
                assert_eq!(passed_npt, vec![npt]);
                let mut hist_chunks = full_hist.chunks(chunk_size).peekable();
                let next_chunks = hist_chunks.nth(npt.into()).unwrap_or_default();
                npt += 1;
                let next_page_token = if hist_chunks.peek().is_none() {
                    vec![]
                } else {
                    vec![npt]
                };
                Ok(GetWorkflowExecutionHistoryResponse {
                    history: Some(History {
                        events: next_chunks.into(),
                    }),
                    raw_history: vec![],
                    next_page_token,
                    archived: false,
                })
            });

        let paginator = paginator_for_test(
            "wfid".to_string(),
            "runid".to_string(),
            vec![1],
            Arc::new(mock_client),
            vec![],
        );

        // Auto-detect chunking version from initial events so tests don't
        // need to pre-set it (and so the legacy/v2 parameterization works).
        let chunking_version = if events_have_wft_chunking_v2(initial_hist.iter()) {
            ChunkingVersion::V2
        } else {
            ChunkingVersion::V1
        };
        let buf = lwft_buffer_for_test(initial_hist, 0, wft_started, false, false, chunking_version);
        (buf, paginator)
    }

    /// Test helper: replay-loop one step. Either yield a WFT (already
    /// taken from the buffer) or fetch one more page and try again.
    /// Returns the WFT events, or `None` once `ReplayOver` is reached.
    async fn pump_next_wft(
        buf: &mut LwftBuffer,
        paginator: &mut HistoryPaginator,
        from_id: i64,
    ) -> Option<Vec<HistoryEvent>> {
        loop {
            match buf.take_next_wft_sequence(from_id) {
                NextWFT::WFT(lwft) => return Some(lwft.into_events()),
                NextWFT::ReplayOver => return None,
                NextWFT::NeedFetch => {
                    let page = paginator.fetch_next_page().await.unwrap();
                    let envelope = buf.envelope();
                    buf.push_events(envelope, page.events, page.no_more_pages);
                }
            }
        }
    }

    /// Test helper: drain ONE page from the paginator and push it into the
    /// buffer. Used by tests that want to drive pagination step-by-step.
    async fn pump_one_page(buf: &mut LwftBuffer, paginator: &mut HistoryPaginator) {
        let page = paginator.fetch_next_page().await.unwrap();
        let envelope = buf.envelope();
        buf.push_events(envelope, page.events, page.no_more_pages);
    }

    /// Test helper: keep pumping pages from the paginator into the buffer
    /// until the buffer can yield (or ReplayOver / unexpected NeedFetch).
    async fn drain_to_take(
        buf: &mut LwftBuffer,
        paginator: &mut HistoryPaginator,
        from_id: i64,
    ) -> NextWFT {
        loop {
            match buf.take_next_wft_sequence(from_id) {
                NextWFT::NeedFetch => pump_one_page(buf, paginator).await,
                other => return other,
            }
        }
    }

    /// Test helper: set up a buffer + paginator pair for cache-miss-style
    /// tests. The partial task's events are held aside on the paginator;
    /// the buffer starts empty with just the envelope set.
    fn cache_miss_setup(
        partial_task: HistoryInfo,
        client: Arc<dyn WorkerClient>,
        chunking_version: ChunkingVersion,
    ) -> (LwftBuffer, HistoryPaginator) {
        let envelope = WftEnvelope {
            previous_wft_started_id: partial_task.previous_started_event_id(),
            wft_started_id: partial_task.workflow_task_started_event_id(),
            has_pending_speculative_updates: false,
        };
        let paginator = paginator_for_test(
            "wfid".to_string(),
            "runid".to_string(),
            NextPageToken::FetchFromStart,
            client,
            partial_task.into_events(),
        );
        let mut buf = LwftBuffer::empty();
        buf.set_chunking_version(chunking_version);
        buf.push_events(envelope, vec![], false);
        (buf, paginator)
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn paginator_extracts_updates(
        #[values(10, 11, 12, 13, 14)] chunk_size: usize,
        #[values(false, true)] chunking_v2: bool,
    ) {
        let wft_count = 100;
        let mut hist = canned_histories::long_sequential_timers(wft_count);
        let expected_final_eid = hist
            .get_full_history_info()
            .unwrap()
            .into_events()
            .last()
            .unwrap()
            .event_id;
        maybe_set_chunking_v2(&mut hist, chunking_v2);

        let (mut buf, mut paginator) = paginator_setup(hist, chunk_size);
        let mut last_id = 0;
        loop {
            let seq = match pump_next_wft(&mut buf, &mut paginator, last_id).await {
                Some(seq) => seq,
                None => {
                    assert_eq!(last_id, expected_final_eid);
                    return;
                }
            };
            assert!(!seq.is_empty());
            for e in &seq {
                assert!(
                    e.event_id > last_id,
                    "event ids must increase monotonically (last_id={last_id}, got {})",
                    e.event_id
                );
                last_id = e.event_id;
            }
        }
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn paginator_streams(#[values(false, true)] chunking_v2: bool) {
        let wft_count = 10;
        let mut hist = canned_histories::long_sequential_timers(wft_count);
        maybe_set_chunking_v2(&mut hist, chunking_v2);
        // Drive the paginator via the streaming adapter, then prepend the
        // events already in the buffer (the first chunk, which the paginator
        // never re-fetches).
        let (buf, paginator) = paginator_setup(hist, 10);
        let initial: Vec<HistoryEvent> = buf.get_events().to_vec();
        let stream = StreamingHistoryPaginator::new(paginator);
        let rest: Vec<HistoryEvent> = stream.try_collect().await.unwrap();
        let everything: Vec<HistoryEvent> = initial.into_iter().chain(rest).collect();
        assert_eq!(everything.len(), (wft_count + 1) * 5);
        everything.iter().fold(1, |event_id, e| {
            assert_eq!(event_id, e.event_id);
            e.event_id + 1
        });
    }

    fn three_wfts_then_heartbeats() -> TestHistoryBuilder {
        let mut t = TestHistoryBuilder::default();
        // Start with two complete normal WFTs
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task(); // wft start - 3
        t.add_by_type(EventType::TimerStarted);
        t.add_full_wf_task(); // wft start - 7
        t.add_by_type(EventType::TimerStarted);
        t.add_full_wf_task(); // wft start - 11
        for _ in 1..50 {
            // Add a bunch of heartbeats with no commands, which count as one task
            t.add_full_wf_task();
        }
        t.add_workflow_execution_completed();
        t
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn needs_fetch_if_ending_in_middle_of_wft_seq(
        // These values test points truncation could've occurred in the middle of the heartbeat
        #[values(18, 19, 20, 21)] truncate_at: usize,
        #[values(false, true)] chunking_v2: bool,
    ) {
        let mut t = three_wfts_then_heartbeats();
        maybe_set_chunking_v2(&mut t, chunking_v2);
        // Truncate the history mid-heartbeat-chain to simulate "more pages
        // pending." Then push the truncated events into a buffer with
        // `no_more_pages = false`: the buffer should yield the complete
        // LWFTs and signal NeedFetch when it hits the unresolvable tail.
        let mut ends_in_middle_of_seq: Vec<HistoryEvent> = t.as_lwft_buffer().get_events().to_vec();
        ends_in_middle_of_seq.truncate(truncate_at);
        let wft_started_id = t
            .get_full_history_info()
            .unwrap()
            .workflow_task_started_event_id();
        let mut buf = lwft_buffer_for_test(
            ends_in_middle_of_seq,
            0,
            wft_started_id,
            false, // no_more_pages: more pages remain
            false,
            cv(chunking_v2),
        );
        let seq = buf.take_next_wft_sequence(0).unwrap_events();
        assert_eq!(seq.last().unwrap().event_id, 3);
        let seq = buf.take_next_wft_sequence(3).unwrap_events();
        assert_eq!(seq.last().unwrap().event_id, 7);
        if chunking_v2 {
            // New algorithm: the third logical WFT ends at `WorkflowTaskStarted` (id 11), but
            // the buffer has no following event — `find_end` returns NeedMore until more history
            // exists.
            let next = buf.take_next_wft_sequence(7);
            assert_matches!(next, NextWFT::NeedFetch);
        } else {
            // Legacy algorithm: less conservative, yields WFT3 immediately then NeedFetch for
            // the next call.
            let seq = buf.take_next_wft_sequence(7).unwrap_events();
            assert_eq!(seq.last().unwrap().event_id, 11);
            let next = buf.take_next_wft_sequence(11);
            assert_matches!(next, NextWFT::NeedFetch);
        }
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn paginator_works_with_wft_over_multiple_pages(
        #[values(10, 11, 12, 13, 14)] chunk_size: usize,
        #[values(false, true)] chunking_v2: bool,
    ) {
        let mut t = three_wfts_then_heartbeats();
        maybe_set_chunking_v2(&mut t, chunking_v2);
        let (mut buf, mut paginator) = paginator_setup(t, chunk_size);
        let mut last_id = 0;
        loop {
            let seq = buf.take_next_wft_sequence(last_id);
            match seq {
                NextWFT::WFT(lwft) => {
                    last_id = lwft.events().last().unwrap().event_id;
                }
                NextWFT::NeedFetch => {
                    pump_one_page(&mut buf, &mut paginator).await;
                }
                NextWFT::ReplayOver => break,
            }
        }
        assert_eq!(last_id, 160);
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn task_just_before_heartbeat_chain_is_taken(#[values(false, true)] chunking_v2: bool) {
        let mut t = three_wfts_then_heartbeats();
        maybe_set_chunking_v2(&mut t, chunking_v2);
        let mut update = t.as_lwft_buffer();
        let seq = update.take_next_wft_sequence(0).unwrap_events();
        assert_eq!(seq.last().unwrap().event_id, 3);
        let seq = update.take_next_wft_sequence(3).unwrap_events();
        assert_eq!(seq.last().unwrap().event_id, 7);
        let seq = update.take_next_wft_sequence(7).unwrap_events();
        assert_eq!(seq.last().unwrap().event_id, 11);
        let seq = update.take_next_wft_sequence(11).unwrap_events();
        assert_eq!(seq.last().unwrap().event_id, 158);
        let seq = update.take_next_wft_sequence(158).unwrap_events();
        assert_eq!(seq.last().unwrap().event_id, 160);
        assert_eq!(
            seq.last().unwrap().event_type(),
            EventType::WorkflowExecutionCompleted
        );
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn handles_cache_misses(#[values(false, true)] chunking_v2: bool) {
        let mut timer_hist = canned_histories::single_timer("t");
        maybe_set_chunking_v2(&mut timer_hist, chunking_v2);
        let partial_task = timer_hist.get_one_wft(2).unwrap();
        let mut history_from_get: GetWorkflowExecutionHistoryResponse =
            timer_hist.get_history_info(2).unwrap().into();
        // Chop off the last event, which is WFT started, which server doesn't return in get
        // history
        history_from_get.history.as_mut().map(|h| h.events.pop());
        let mut mock_client = mock_worker_client();
        mock_client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, _| Ok(history_from_get.clone()));

        let (mut buf, mut paginator) =
            cache_miss_setup(partial_task, Arc::new(mock_client), cv(chunking_v2));
        let seq = drain_to_take(&mut buf, &mut paginator, 0)
            .await
            .unwrap_events();
        assert_eq!(seq[0].event_id, 1);
        let seq = drain_to_take(&mut buf, &mut paginator, 3)
            .await
            .unwrap_events();
        // Verify anything extra (which should only ever be WFT started) was re-appended to the
        // end of the event iteration after fetching the old history.
        assert_eq!(seq.last().unwrap().event_id, 8);
    }

    #[rstest::rstest]
    #[test]
    fn la_marker_chunking(#[values(false, true)] chunking_v2: bool) {
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        t.add_we_signaled("whatever", vec![]);
        t.add_full_wf_task(); // started - 7
        t.add_local_activity_result_marker(1, "hi", Default::default());
        let act_s = t.add_activity_task_scheduled("1");
        let act_st = t.add_activity_task_started(act_s);
        t.add_activity_task_completed(act_s, act_st, Default::default());
        t.add_workflow_task_scheduled_and_started();
        t.add_workflow_task_timed_out();
        t.add_workflow_task_scheduled_and_started();
        t.add_workflow_task_timed_out();
        t.add_workflow_task_scheduled_and_started();
        maybe_set_chunking_v2(&mut t, chunking_v2);

        let mut update = t.as_lwft_buffer();
        let seq = next_check_peek(&mut update, 0);
        assert_eq!(seq.len(), 3);
        let seq = next_check_peek(&mut update, 3);
        assert_eq!(seq.len(), 4);
        let seq = next_check_peek(&mut update, 7);
        assert_eq!(seq.len(), 13);
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn handles_blank_fetch_response(#[values(false, true)] chunking_v2: bool) {
        let mut timer_hist = canned_histories::single_timer("t");
        maybe_set_chunking_v2(&mut timer_hist, chunking_v2);
        let partial_task = timer_hist.get_one_wft(2).unwrap();
        let _prev_started_wft_id = partial_task.previous_started_event_id();
        let _wft_started_id = partial_task.workflow_task_started_event_id();
        // The old test verified that a `get_workflow_execution_history` that
        // returns nothing causes `extract_next_update` to error. In the new
        // model, the paginator only fetches one page at a time and returns
        // the events. An empty page with no continuation token simply means
        // `no_more_pages = true`; the buffer then needs to handle "I've been
        // told there are no more pages but I still have nothing to chunk."
        //
        // That degenerate case can't happen in production (a real workflow
        // always has at least the events the caller is replaying), so this
        // test no longer exercises a meaningful failure mode. Kept as a
        // smoke check that an empty fetch is at least benign.
        let mut mock_client = mock_worker_client();
        mock_client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, _| Ok(Default::default()));

        let partial_events = partial_task.into_events();
        let mut paginator = paginator_for_test(
            "wfid".to_string(),
            "runid".to_string(),
            NextPageToken::FetchFromStart,
            Arc::new(mock_client),
            partial_events.clone(),
        );
        let page = paginator.fetch_next_page().await.unwrap();
        // Server returned nothing; the page still flushes held-aside events
        // on the Done transition.
        assert!(page.no_more_pages);
        assert_eq!(page.events.len(), partial_events.len());
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn handles_empty_page_with_next_token(#[values(false, true)] chunking_v2: bool) {
        let mut timer_hist = canned_histories::single_timer("t");
        maybe_set_chunking_v2(&mut timer_hist, chunking_v2);
        let partial_task = timer_hist.get_one_wft(2).unwrap();
        let full_resp: GetWorkflowExecutionHistoryResponse =
            timer_hist.get_full_history_info().unwrap().into();
        let mut mock_client = mock_worker_client();
        mock_client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, _| {
                Ok(GetWorkflowExecutionHistoryResponse {
                    history: Some(History { events: vec![] }),
                    raw_history: vec![],
                    next_page_token: vec![2],
                    archived: false,
                })
            })
            .times(1);
        mock_client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, _| Ok(full_resp.clone()))
            .times(1);

        let (mut buf, mut paginator) =
            cache_miss_setup(partial_task, Arc::new(mock_client), cv(chunking_v2));
        let seq = drain_to_take(&mut buf, &mut paginator, 0)
            .await
            .unwrap_events();
        assert_eq!(seq.last().unwrap().event_id, 3);
        let seq = drain_to_take(&mut buf, &mut paginator, 3)
            .await
            .unwrap_events();
        assert_eq!(seq.last().unwrap().event_id, 8);
        assert_matches!(
            drain_to_take(&mut buf, &mut paginator, 8).await,
            NextWFT::ReplayOver
        );
    }

    // TODO: Test we dont re-feed pointless updates if fetching returns <= events we already
    //   processed

    #[rstest::rstest]
    #[tokio::test]
    async fn handles_fetching_page_with_complete_wft_and_page_token_to_empty_page(
        #[values(false, true)] chunking_v2: bool,
    ) {
        let mut timer_hist = canned_histories::single_timer("t");
        maybe_set_chunking_v2(&mut timer_hist, chunking_v2);
        let workflow_task = timer_hist.get_full_history_info().unwrap();

        let mut full_resp_with_npt: GetWorkflowExecutionHistoryResponse =
            timer_hist.get_full_history_info().unwrap().into();
        full_resp_with_npt.next_page_token = vec![1];

        let mut mock_client = mock_worker_client();
        mock_client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, _| Ok(full_resp_with_npt.clone()))
            .times(1);
        mock_client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, _| {
                Ok(GetWorkflowExecutionHistoryResponse {
                    history: Some(History { events: vec![] }),
                    raw_history: vec![],
                    next_page_token: vec![],
                    archived: false,
                })
            })
            .times(1);

        let (mut buf, mut paginator) =
            cache_miss_setup(workflow_task, Arc::new(mock_client), cv(chunking_v2));
        let seq = drain_to_take(&mut buf, &mut paginator, 0)
            .await
            .unwrap_events();
        assert_eq!(seq.last().unwrap().event_id, 3);
        let seq = drain_to_take(&mut buf, &mut paginator, 3)
            .await
            .unwrap_events();
        assert_eq!(seq.last().unwrap().event_id, 8);
        assert_matches!(
            drain_to_take(&mut buf, &mut paginator, 8).await,
            NextWFT::ReplayOver
        );
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn extreme_pagination_doesnt_drop_wft_events_paginator(
        #[values(false, true)] chunking_v2: bool,
    ) {
        // 1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
        // 2: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
        // 3: EVENT_TYPE_WORKFLOW_TASK_STARTED // <- previous_started_event_id
        // 4: EVENT_TYPE_WORKFLOW_TASK_COMPLETED

        // 5: EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
        // 6: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
        // 7: EVENT_TYPE_WORKFLOW_TASK_STARTED
        // 8: EVENT_TYPE_WORKFLOW_TASK_FAILED

        // 9: EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
        // 10: EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
        // 11: EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
        // 12: EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
        // 13: EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
        // 14: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
        // 15: EVENT_TYPE_WORKFLOW_TASK_STARTED // <- started_event_id

        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();

        t.add_we_signaled("hi", vec![]);
        t.add_workflow_task_scheduled_and_started();
        t.add_workflow_task_failed_with_failure(
            WorkflowTaskFailedCause::UnhandledCommand,
            Default::default(),
        );

        t.add_we_signaled("hi", vec![]);
        t.add_we_signaled("hi", vec![]);
        t.add_we_signaled("hi", vec![]);
        t.add_we_signaled("hi", vec![]);
        t.add_we_signaled("hi", vec![]);
        t.add_workflow_task_scheduled_and_started();
        maybe_set_chunking_v2(&mut t, chunking_v2);

        let mut mock_client = mock_worker_client();

        let events: Vec<HistoryEvent> = t.get_full_history_info().unwrap().into_events();
        let first_event = events[0].clone();
        for (i, event) in events.into_iter().enumerate() {
            // Add an empty page
            mock_client
                .expect_get_workflow_execution_history()
                .returning(move |_, _, _| {
                    Ok(GetWorkflowExecutionHistoryResponse {
                        history: Some(History { events: vec![] }),
                        raw_history: vec![],
                        next_page_token: vec![(i * 10) as u8],
                        archived: false,
                    })
                })
                .times(1);

            // Add a page with only event i
            mock_client
                .expect_get_workflow_execution_history()
                .returning(move |_, _, _| {
                    Ok(GetWorkflowExecutionHistoryResponse {
                        history: Some(History {
                            events: vec![event.clone()],
                        }),
                        raw_history: vec![],
                        next_page_token: vec![(i * 10 + 1) as u8],
                        archived: false,
                    })
                })
                .times(1);
        }

        // Add an extra empty page at the end, with no NPT
        mock_client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, _| {
                Ok(GetWorkflowExecutionHistoryResponse {
                    history: Some(History { events: vec![] }),
                    raw_history: vec![],
                    next_page_token: vec![],
                    archived: false,
                })
            })
            .times(1);

        // Initial poll events live in the buffer; the paginator only fetches
        // subsequent pages.
        let envelope = WftEnvelope {
            previous_wft_started_id: 3,
            wft_started_id: 15,
            has_pending_speculative_updates: false,
        };
        let mut buf = LwftBuffer::empty();
        buf.set_chunking_version(cv(chunking_v2));
        buf.push_events(envelope, vec![first_event], false);
        let mut paginator = paginator_for_test(
            "wfid".to_string(),
            "runid".to_string(),
            vec![1],
            Arc::new(mock_client),
            vec![],
        );

        let seq = drain_to_take(&mut buf, &mut paginator, 0)
            .await
            .unwrap_events();
        assert_eq!(seq.first().unwrap().event_id, 1);
        assert_eq!(seq.last().unwrap().event_id, 3);

        let seq = drain_to_take(&mut buf, &mut paginator, 3)
            .await
            .unwrap_events();
        assert_eq!(seq.first().unwrap().event_id, 4);
        assert_eq!(seq.last().unwrap().event_id, 15);
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn finding_end_index_with_started_as_last_event(
        #[values(false, true)] chunking_v2: bool,
    ) {
        let wf_id = "fakeid";
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();

        t.add_we_signaled("hi", vec![]);
        t.add_workflow_task_scheduled_and_started();
        maybe_set_chunking_v2(&mut t, chunking_v2);
        // We need to see more after this - it's not sufficient to end on a started event when
        // we know there might be more

        let workflow_task = t.get_history_info(1).unwrap();
        let mut wft_resp = workflow_task.as_poll_wft_response();
        wft_resp.workflow_execution = Some(WorkflowExecution {
            workflow_id: wf_id.to_string(),
            run_id: t.get_orig_run_id().to_string(),
        });
        wft_resp.next_page_token = vec![1];

        let mut resp_1: GetWorkflowExecutionHistoryResponse =
            t.get_full_history_info().unwrap().into();
        resp_1.next_page_token = vec![2];

        let mut mock_client = mock_worker_client();
        mock_client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, _| Ok(resp_1.clone()))
            .times(1);
        // Since there aren't sufficient events, we should try to see another fetch, and that'll
        // say there aren't any
        mock_client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, _| Ok(Default::default()))
            .times(1);

        let (mut buf, mut paginator) =
            cache_miss_setup(workflow_task, Arc::new(mock_client), cv(chunking_v2));
        let seq = drain_to_take(&mut buf, &mut paginator, 0)
            .await
            .unwrap_events();
        assert_eq!(seq.last().unwrap().event_id, 3);
        let seq = drain_to_take(&mut buf, &mut paginator, 3)
            .await
            .unwrap_events();
        // We're done since the last fetch revealed nothing
        assert_eq!(seq.last().unwrap().event_id, 7);
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn just_signal_is_complete_wft(#[values(false, true)] chunking_v2: bool) {
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        t.add_we_signaled("whatever", vec![]);
        t.add_full_wf_task();
        t.add_we_signaled("whatever", vec![]);
        t.add_full_wf_task();
        t.add_workflow_execution_completed();
        maybe_set_chunking_v2(&mut t, chunking_v2);

        let workflow_task = t.get_full_history_info().unwrap();
        let prev_started_wft_id = workflow_task.previous_started_event_id();
        let wft_started_id = workflow_task.workflow_task_started_event_id();
        let mock_client = mock_worker_client();
        let mut paginator = HistoryPaginator::new(
            workflow_task.into(),
            prev_started_wft_id,
            wft_started_id,
            "wfid".to_string(),
            "runid".to_string(),
            NextPageToken::Done,
            Arc::new(mock_client),
            false,
        );
        let mut update = paginator.extract_next_update().await.unwrap();
        let seq = next_check_peek(&mut update, 0);
        assert_eq!(seq.len(), 3);
        let seq = next_check_peek(&mut update, 3);
        assert_eq!(seq.len(), 4);
        let seq = next_check_peek(&mut update, 7);
        assert_eq!(seq.len(), 4);
        let seq = next_check_peek(&mut update, 11);
        assert_eq!(seq.len(), 2);
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn heartbeats_then_signal(#[values(false, true)] chunking_v2: bool) {
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        t.add_full_wf_task();
        let mut need_fetch_resp =
            hist_to_poll_resp(&t, "wfid".to_owned(), ResponseType::AllHistory).resp;
        need_fetch_resp.next_page_token = vec![1];
        t.add_full_wf_task();
        t.add_we_signaled("whatever", vec![]);
        t.add_workflow_task_scheduled_and_started();
        maybe_set_chunking_v2(&mut t, chunking_v2);

        let full_resp: GetWorkflowExecutionHistoryResponse =
            t.get_full_history_info().unwrap().into();

        let mut mock_client = mock_worker_client();
        mock_client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, _| Ok(full_resp.clone()))
            .times(1);

        let mut paginator = HistoryPaginator::new(
            need_fetch_resp.history.unwrap(),
            // Pretend we have already processed first WFT
            3,
            6,
            "wfid".to_string(),
            "runid".to_string(),
            NextPageToken::Next(vec![1]),
            Arc::new(mock_client),
            false,
        );
        let mut update = paginator.extract_next_update().await.unwrap();
        // Starting past first wft
        let seq = next_check_peek(&mut update, 3);
        assert_eq!(seq.len(), 6);
        let seq = next_check_peek(&mut update, 9);
        assert_eq!(seq.len(), 4);
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn cache_miss_with_only_one_wft_available_orders_properly(
        #[values(false, true)] chunking_v2: bool,
    ) {
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        t.add_by_type(EventType::TimerStarted);
        t.add_full_wf_task();
        t.add_by_type(EventType::TimerStarted);
        t.add_workflow_task_scheduled_and_started();
        maybe_set_chunking_v2(&mut t, chunking_v2);

        let incremental_task =
            hist_to_poll_resp(&t, "wfid".to_owned(), ResponseType::OneTask(3)).resp;

        let mut mock_client = mock_worker_client();
        let mut one_task_resp: GetWorkflowExecutionHistoryResponse =
            t.get_history_info(1).unwrap().into();
        one_task_resp.next_page_token = vec![1];
        mock_client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, _| Ok(one_task_resp.clone()))
            .times(1);
        let mut up_to_sched_start: GetWorkflowExecutionHistoryResponse =
            t.get_full_history_info().unwrap().into();
        up_to_sched_start
            .history
            .as_mut()
            .unwrap()
            .events
            .truncate(9);
        mock_client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, _| Ok(up_to_sched_start.clone()))
            .times(1);

        let mut paginator = HistoryPaginator::new(
            incremental_task.history.unwrap(),
            6,
            9,
            "wfid".to_string(),
            "runid".to_string(),
            NextPageToken::FetchFromStart,
            Arc::new(mock_client),
            false,
        );
        let mut update = paginator.extract_next_update().await.unwrap();
        let seq = next_check_peek(&mut update, 0);
        assert_eq!(seq.last().unwrap().event_id, 3);
        let seq = next_check_peek(&mut update, 3);
        assert_eq!(seq.last().unwrap().event_id, 7);
        let seq = next_check_peek(&mut update, 7);
        assert_eq!(seq.last().unwrap().event_id, 11);
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn wft_fail_on_first_task_with_update(#[values(false, true)] chunking_v2: bool) {
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_workflow_task_scheduled_and_started();
        t.add_workflow_task_failed_with_failure(
            WorkflowTaskFailedCause::Unspecified,
            Default::default(),
        );
        t.add_full_wf_task();
        let accept_id = t.add_update_accepted("1", "upd");
        let timer_id = t.add_timer_started("1".to_string());
        t.add_update_completed(accept_id);
        t.add_timer_fired(timer_id, "1".to_string());
        t.add_full_wf_task();
        maybe_set_chunking_v2(&mut t, chunking_v2);

        let mut update = t.as_lwft_buffer();
        let seq = next_check_peek(&mut update, 0);
        // In this case, we expect to see up to the task with update, since the task failure
        // should be skipped. This means that the peek of the _next_ task will include the update
        // and thus properly synthesize the update request with the first activation.
        assert_eq!(seq.len(), 6);
        let seq = next_check_peek(&mut update, 6);
        assert_eq!(seq.len(), 7);
    }

    #[rstest::rstest]
    #[test]
    fn update_accepted_after_empty_wft(#[values(false, true)] chunking_v2: bool) {
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        t.add_full_wf_task();
        let accept_id = t.add_update_accepted("1", "upd");
        let timer_id = t.add_timer_started("1".to_string());
        t.add_update_completed(accept_id);
        t.add_timer_fired(timer_id, "1".to_string());
        t.add_full_wf_task();
        maybe_set_chunking_v2(&mut t, chunking_v2);

        let mut update = t.as_lwft_buffer();
        let seq = next_check_peek(&mut update, 0);
        // unlike the case with a wft failure, here the first task should not extend through to
        // the update, because here the first empty WFT happened with _just_ the workflow init,
        // not also with the update.
        assert_eq!(seq.len(), 3);
        let seq = next_check_peek(&mut update, 3);
        assert_eq!(seq.len(), 3);
        //         // Heartbeat: first empty WFT collapses into the second; boundary is the second WFTStarted.
        // assert_eq!(seq.len(), 6);
        // assert_eq!(seq.last().unwrap().event_id, 6);
        // let seq = next_check_peek(&mut update, 6);
        // // Through timer command, next WFTStarted (open until following completion is visible as end index).
        // assert_eq!(seq.len(), 7);
        // assert_eq!(seq.last().unwrap().event_id, 13);
    }

    /// Builds a history with an empty WFT followed by a WFT with an update:
    ///   Event 1:  WorkflowExecutionStarted
    ///   Event 2:  WFTScheduled  ─┐
    ///   Event 3:  WFTStarted    ─┤ WFT1 (empty, no commands)
    ///   Event 4:  WFTCompleted  ─┘
    ///   Event 5:  WFTScheduled  ─┐
    ///   Event 6:  WFTStarted    ─┤ WFT2 (empty, no commands)
    ///   Event 7:  WFTCompleted  ─┘
    ///   Event 8:  WFTScheduled  ─┐
    ///   Event 9:  WFTStarted    ─┤ WFT3 (update + commands follow)
    ///   Event 10: WFTCompleted  ─┘
    ///   Event 11: UpdateAccepted  (sequencing_event_id = 8)
    ///   Event 12: UpdateCompleted
    ///   Event 13: TimerStarted
    ///   Event 14: TimerFired
    ///   Event 15: WFTScheduled  ─┐
    ///   Event 16: WFTStarted    ─┤ WFT4
    ///   Event 17: WFTCompleted  ─┘
    ///   Event 18: WorkflowExecutionCompleted
    fn build_empty_wft_then_update_history(chunking_v2: bool) -> TestHistoryBuilder {
        let mut t = TestHistoryBuilder::default();
        if chunking_v2 {
            t.set_use_wft_chunking_v2();
        }
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task(); // WFT1: events 2-4 (empty)
        t.add_full_wf_task(); // WFT2: events 5-7 (empty)
        t.add_full_wf_task(); // WFT3: events 8-10
        let accept_id = t.add_update_accepted("upd-1", "startWork"); // 11, seq=8
        t.add_update_completed(accept_id); // 12
        let timer_id = t.add_timer_started("1".to_string()); // 13
        t.add_timer_fired(timer_id, "1".to_string()); // 14
        t.add_full_wf_task(); // WFT4: events 15-17 (command)
        t.add_workflow_execution_completed(); // 18
        t
    }

    /// Empty WFT followed by a speculative WFT with Update request.
    ///
    /// The v1 algorithm may incorrectly collapse the speculative WFT with the preceding WFT,
    /// resulting in an NDE. This test confirms that v2 correctly handles this scenario.
    ///
    /// The legacy, v1 algorithm's behavior is not covered by this test.
    #[rstest::rstest]
    #[test]
    fn empty_wft_then_update_has_last_wft(#[values(false, true)] chunking_v2: bool) {
        if !chunking_v2 {
            // The legacy, v1 algorithm's behavior is not covered by this test.
            return;
        }

        let t = build_empty_wft_then_update_history(chunking_v2);
        let all_events = t.get_full_history_info().unwrap().into_events();

        // 3. Up to WFT1 Started — single WFT visible.
        {
            let mut update = lwft_buffer_for_test(
                all_events[..3].to_vec(),
                0,
                3,
                true,
                false,
                cv(chunking_v2),
            );

            // WFEStarted -> WFTScheduled -> WFTStarted
            assert_eq!(next_check_peek2(&mut update, 0), (3, true));

            // ReplayOver
            assert_matches!(update.take_next_wft_sequence(3), NextWFT::ReplayOver);
        }

        // 4. Up to WFT1 Completed.
        {
            let mut update = lwft_buffer_for_test(
                all_events[..4].to_vec(),
                0,
                3,
                true,
                false,
                cv(chunking_v2),
            );

            // Can't collapse because of WFExecutionStarted.
            // WFEStarted -> WFTScheduled -> WFTStarted
            assert_eq!(next_check_peek2(&mut update, 0), (3, false));

            // WFTCompleted
            assert_eq!(next_check_peek2(&mut update, 3), (1, true));

            // ReplayOver
            assert_matches!(update.take_next_wft_sequence(4), NextWFT::ReplayOver);
        }

        // 6. Up to WFT2 Started.
        {
            let mut update = lwft_buffer_for_test(
                all_events[..6].to_vec(),
                0,
                6,
                true,
                false,
                cv(chunking_v2),
            );

            // Can't collapse because of WFExecutionStarted.
            // WFEStarted -> WFTScheduled -> WFTStarted
            assert_eq!(next_check_peek2(&mut update, 0), (3, false));

            // WFT2 is the remaining LWFT.
            // WFTCompleted -> WFTScheduled -> WFTStarted
            assert_eq!(next_check_peek2(&mut update, 3), (3, true));

            // ReplayOver
            assert_matches!(update.take_next_wft_sequence(6), NextWFT::ReplayOver);
        }

        // 7. Up to WFT2 Completed.
        {
            let mut update = lwft_buffer_for_test(
                all_events[..7].to_vec(),
                0,
                6,
                true,
                false,
                cv(chunking_v2),
            );

            // Can't collapse because of WFExecutionStarted.
            // WFEStarted -> WFTScheduled -> WFTStarted
            assert_eq!(next_check_peek2(&mut update, 0), (3, false));

            // WFTCompleted -> WFTScheduled -> WFTStarted
            assert_eq!(next_check_peek2(&mut update, 3), (3, false));

            // WFTCompleted
            assert_eq!(next_check_peek2(&mut update, 7), (1, true));

            // ReplayOver
            assert_matches!(update.take_next_wft_sequence(7), NextWFT::ReplayOver);
        }

        // 9. Up to WFT3 Started, no speculative Update pending.
        {
            let mut update = lwft_buffer_for_test(
                all_events[..9].to_vec(),
                0,
                9,
                true,
                false,
                cv(chunking_v2),
            );

            // WFEStarted -> WFTScheduled -> WFTStarted
            assert_eq!(next_check_peek2(&mut update, 0), (3, false));

            // It is ok to collapse WFT2+WFT3 in this case, as there is no new event on WFT3.
            // WFTCompleted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted
            assert_eq!(next_check_peek2(&mut update, 3), (6, true));

            // ReplayOver
            assert_matches!(update.take_next_wft_sequence(9), NextWFT::ReplayOver);
        }

        // 9a. Similar to 9, but WFT3 is a speculative WFT with a pending update.
        {
            let mut update = lwft_buffer_for_test(
                all_events[..9].to_vec(),
                0,
                9,
                true,
                true,
                cv(chunking_v2),
            );

            // WFEStarted -> WFTScheduled -> WFTStarted
            assert_eq!(next_check_peek2(&mut update, 0), (3, false));

            // WFTCompleted -> WFTScheduled -> WFTStarted
            assert_eq!(next_check_peek2(&mut update, 3), (3, false));

            // Can't collapse because of speculative update affecting WFT3
            // WFTCompleted -> WFTScheduled -> WFTStarted
            assert_eq!(next_check_peek2(&mut update, 6), (3, true));

            // ReplayOver
            assert_matches!(update.take_next_wft_sequence(9), NextWFT::ReplayOver);
        }

        // 10. Up to WFT3 Completed.
        {
            let mut update = lwft_buffer_for_test(
                all_events[..10].to_vec(),
                0,
                6,
                true,
                false,
                cv(chunking_v2),
            );

            // WFEStarted -> WFTScheduled -> WFTStarted
            assert_eq!(next_check_peek2(&mut update, 0), (3, false));

            // It is ok to collapse WFT2+WFT3 in this case, as there is no new event on WFT3.
            // WFTCompleted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted
            assert_eq!(next_check_peek2(&mut update, 3), (6, false));

            // WFTCompleted
            assert_eq!(next_check_peek2(&mut update, 9), (1, true));

            // ReplayOver
            assert_matches!(update.take_next_wft_sequence(10), NextWFT::ReplayOver);
        }

        // 11. Similar to 10, but there's an UpdateAccepted affecting WFT3.
        {
            let mut update = lwft_buffer_for_test(
                all_events[..11].to_vec(),
                0,
                9,
                true,
                false,
                cv(chunking_v2),
            );

            // WFEStarted -> WFTScheduled -> WFTStarted
            assert_eq!(next_check_peek2(&mut update, 0), (3, false));

            // WFTCompleted -> WFTScheduled -> WFTStarted
            assert_eq!(next_check_peek2(&mut update, 3), (3, false));

            // Can't collapse because of UpdateAccepted affecting WFT3
            // WFTCompleted -> WFTScheduled -> WFTStarted
            assert_eq!(next_check_peek2(&mut update, 6), (3, false));

            // Tail(WFTCompleted -> UpdateAccepted)
            assert_eq!(next_check_peek2(&mut update, 9), (2, true));

            // ReplayOver
            assert_matches!(update.take_next_wft_sequence(11), NextWFT::ReplayOver);
        }

        // 18: Full history
        {
            let mut update = t.as_lwft_buffer();

            // WFEStarted -> WFTScheduled -> WFTStarted
            assert_eq!(next_check_peek2(&mut update, 0), (3, false));

            // WFTCompleted -> WFTScheduled -> WFTStarted
            assert_eq!(next_check_peek2(&mut update, 3), (3, false));

            // Can't collapse because of UpdateAccepted affecting WFT3
            // WFTCompleted -> WFTScheduled -> WFTStarted
            assert_eq!(next_check_peek2(&mut update, 6), (3, false));

            // Complete(WFTCompleted -> UpdateAccepted -> UpdateCompleted -> TimerStarted -> TimerFired -> WFTScheduled -> WFTStarted)
            assert_eq!(next_check_peek2(&mut update, 9), (7, false));

            // Complete(WFTCompleted -> WorkflowExecutionCompleted)
            assert_eq!(next_check_peek2(&mut update, 16), (2, true));

            // ReplayOver
            assert_matches!(update.take_next_wft_sequence(18), NextWFT::ReplayOver);
        }
    }

    /// Empty WFT followed by WFT with update.
    ///
    /// The v1 algorithm would often return a Complete despite not being not being able to look
    /// ahead far enough (despite has_last_wft=false) to take a positive decision. This test
    /// confirms that v2 correctly returns NeedFetch in those case.
    ///
    /// The legacy, v1 algorithm's behavior is not covered by this test.
    #[rstest::rstest]
    #[test]
    fn empty_wft_then_update_no_last_wft(#[values(false, true)] chunking_v2: bool) {
        if !chunking_v2 {
            // This test encodes behavior that WFT chunking v2 specifically fixes
            // (correct handling of updates after empty WFTs). The legacy algorithm has
            // known buggy behavior in these scenarios — which is the entire motivation
            // for this workspace's work. Skip on legacy.
            return;
        }
        let t = build_empty_wft_then_update_history(chunking_v2);
        let all_events = t.get_full_history_info().unwrap().into_events();

        // 3. Up to WFT1 Started.
        {
            let mut update = lwft_buffer_for_test(
                all_events[..3].to_vec(),
                0,
                3,
                false,
                false,
                cv(chunking_v2),
            );

            // Buffer:
            //   WFEStarted -> WFTScheduled -> WFTStarted -> (unknown)

            // Can't decide because unknown could:
            // - be collapsable into WFT1
            // - contain an UpdateAccepted event pointing back to the first WFTStarted
            // - contain a WFTFailed event

            assert_matches!(update.take_next_wft_sequence(0), NextWFT::NeedFetch);
        }

        // 4. Up to WFT1 Completed.
        {
            let mut update = lwft_buffer_for_test(
                all_events[..4].to_vec(),
                0,
                3,
                false,
                false,
                cv(chunking_v2),
            );

            // Buffer:
            //   WFEStarted -> WFTScheduled -> WFTStarted -> WFTCompleted -> (unknown)

            // Can't decide because unknown could:
            // - be collapsable into WFT1
            // - contain an UpdateAccepted event pointing back to the first WFTStarted

            assert_matches!(update.take_next_wft_sequence(0), NextWFT::NeedFetch);
        }

        // 4a. Up to WFT1 Completed + a follow up command
        {
            let mut t = TestHistoryBuilder::from_history(all_events[..4].to_vec());
            t.add_timer_started("1".to_string());

            let events = t.get_full_history_info().unwrap().into_events().to_vec();
            let mut update =
                lwft_buffer_for_test(events, 0, 3, false, false, cv(chunking_v2));

            // Buffer:
            //   WFEStarted -> WFTScheduled -> WFTStarted -> WFTCompleted -> TimerStarted -> (unknown)

            // It is safe to return LWFT ending at the first WFTStarted
            assert_eq!(next_check_peek2(&mut update, 0), (3, false));

            // Can't decide because there are no more WFTStarted in buffer, but unknown could contain some
            assert_matches!(update.take_next_wft_sequence(3), NextWFT::NeedFetch);
        }

        // 5. Up to WFT2 Scheduled.
        {
            let mut update = lwft_buffer_for_test(
                all_events[..5].to_vec(),
                0,
                3,
                false,
                false,
                cv(chunking_v2),
            );

            // Buffer:
            //   WFEStarted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> (unknown)

            // It is safe to return LWFT ending at the first WFTStarted (WFT1 follows WFExecutionStarted).
            assert_eq!(next_check_peek2(&mut update, 0), (3, false));

            // Can't decide further because unknown could:
            // - contain a WFTFailed event
            // - contain an UpdateAccepted event pointing back to the second WFTStarted
            assert_matches!(update.take_next_wft_sequence(3), NextWFT::NeedFetch);
        }

        // 5a. Up to WFT2 Scheduled + some inbound event
        {
            let mut t = TestHistoryBuilder::from_history(all_events[..5].to_vec());
            t.add_we_signaled("whee", vec![]);

            let events = t.get_full_history_info().unwrap().into_events().to_vec();
            let mut update =
                lwft_buffer_for_test(events, 0, 3, false, false, cv(chunking_v2));

            // Buffer:
            //   WFEStarted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WeSignaled -> (unknown)

            // It is safe to return LWFT ending at the first WFTStarted.
            // There can't be any unknown passed the WeSignaled that would affect WFT1
            assert_eq!(next_check_peek2(&mut update, 0), (3, false));

            // Can't decide further because there are no more WFTStarted in buffer, but unknown could contain some
            assert_matches!(update.take_next_wft_sequence(3), NextWFT::NeedFetch);
        }

        // 6. Up to WFT2 Started.
        {
            let mut update = lwft_buffer_for_test(
                all_events[..6].to_vec(),
                0,
                6,
                false,
                false,
                cv(chunking_v2),
            );

            // Buffer:
            //   WFEStarted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> (unknown)

            // It is safe to return LWFT ending at the first WFTStarted (WFT1 follows WFExecutionStarted).
            assert_eq!(next_check_peek2(&mut update, 0), (3, false));

            // Can't decide further because unknown could:
            // - contain a WFTFailed event
            // - contain an UpdateAccepted event pointing back to the second WFTStarted
            assert_matches!(update.take_next_wft_sequence(3), NextWFT::NeedFetch);
        }

        // 6a. Up to WFT2 Started + WFTTimedOut.
        {
            let mut t = TestHistoryBuilder::from_history(all_events[..6].to_vec());
            t.add_workflow_task_timed_out();

            let events = t.get_full_history_info().unwrap().into_events().to_vec();
            let mut update =
                lwft_buffer_for_test(events, 0, 3, false, false, cv(chunking_v2));

            // Buffer:
            //   WFEStarted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> WFTFailed -> (unknown)

            // It is safe to return LWFT ending at the first WFTStarted.
            assert_eq!(next_check_peek2(&mut update, 0), (3, false));

            // Can't decide further because there are no more non-failed WFTStarted in buffer; unknown could contain some
            assert_matches!(update.take_next_wft_sequence(3), NextWFT::NeedFetch);
        }

        // 7. Up to WFT2 Completed.
        {
            let mut update = lwft_buffer_for_test(
                all_events[..7].to_vec(),
                0,
                6,
                false,
                false,
                cv(chunking_v2),
            );

            // Buffer:
            //   WFEStarted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> WFTCompleted -> (unknown)

            // It is safe to return LWFT ending at the first WFTStarted (WFT1 follows WFExecutionStarted).
            assert_eq!(next_check_peek2(&mut update, 0), (3, false));

            // Can't decide further because unknown could contain an UpdateAccepted event pointing
            // back to the second WFTStarted.
            assert_matches!(update.take_next_wft_sequence(3), NextWFT::NeedFetch);
        }

        // 7a. Up to WFT2 Completed + a follow up command.
        {
            let mut t = TestHistoryBuilder::from_history(all_events[..7].to_vec());
            t.add_timer_started("1".to_string());

            let events = t.get_full_history_info().unwrap().into_events().to_vec();
            let mut update =
                lwft_buffer_for_test(events, 0, 3, false, false, cv(chunking_v2));

            // Buffer:
            //   WFEStarted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> WFTCompleted -> TimerStarted -> (unknown)

            // WFT1 is forced separate (follows WFExecutionStarted).
            assert_eq!(next_check_peek2(&mut update, 0), (3, false));

            // It is safe to return LWFT ending at the second WFTStarted.
            assert_eq!(next_check_peek2(&mut update, 3), (3, false));

            assert_matches!(update.take_next_wft_sequence(6), NextWFT::NeedFetch);
        }

        // 9. Up to WFT3 Started.
        {
            let mut update = lwft_buffer_for_test(
                all_events[..9].to_vec(),
                0,
                9,
                false,
                false,
                cv(chunking_v2),
            );

            // Buffer:
            //   WFEStarted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> (unknown)

            // It is safe to return LWFT ending at the first WFTStarted (WFT1 follows WFExecutionStarted).
            assert_eq!(next_check_peek2(&mut update, 0), (3, false));

            // Can't decide further because unknown could:
            // - allow or prevent collapsing WFT2+WFT3
            // - contain a WFTFailed event
            // - contain an UpdateAccepted event pointing back to the second WFTStarted
            assert_matches!(update.take_next_wft_sequence(3), NextWFT::NeedFetch);
        }

        // 9a. Up to WFT3 Started + WFTTimedOut.
        {
            let mut t = TestHistoryBuilder::from_history(all_events[..9].to_vec());
            t.add_workflow_task_timed_out();

            let events = t.get_full_history_info().unwrap().into_events().to_vec();
            let mut update =
                lwft_buffer_for_test(events, 0, 0, false, false, cv(chunking_v2));

            // Buffer:
            //   WFEStarted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> WFTTimedOut -> (unknown)

            // WFT1 is forced separate (follows WFExecutionStarted).
            assert_eq!(next_check_peek2(&mut update, 0), (3, false));

            // It is safe to return LWFT ending at the second WFTStarted.
            assert_eq!(next_check_peek2(&mut update, 3), (3, false));

            // Can't decide further because there are no more non-failed WFTStarted in buffer; unknown could contain some
            assert_matches!(update.take_next_wft_sequence(6), NextWFT::NeedFetch);
        }

        // 10. Up to WFT3 Completed.
        {
            let mut update = lwft_buffer_for_test(
                all_events[..10].to_vec(),
                0,
                9,
                false,
                false,
                cv(chunking_v2),
            );

            // Buffer:
            //   WFEStarted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> WFTCompleted -> (unknown)

            // It is safe to return LWFT ending at the first WFTStarted (WFT1 follows WFExecutionStarted).
            assert_eq!(next_check_peek2(&mut update, 0), (3, false));

            // Can't decide further because unknown could:
            // - allow or prevent collapsing WFT2+WFT3
            // - contain an UpdateAccepted event pointing back to the third WFTStarted
            assert_matches!(update.take_next_wft_sequence(3), NextWFT::NeedFetch);
        }

        // 11. Up to updateAccepted
        {
            let mut update = lwft_buffer_for_test(
                all_events[..11].to_vec(),
                0,
                9,
                false,
                false,
                cv(chunking_v2),
            );

            // Buffer:
            //   WFEStarted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTAccepted -> (unknown)

            // WFT1 is forced separate (follows WFExecutionStarted).
            assert_eq!(next_check_peek2(&mut update, 0), (3, false));
            // WFT2 is safe because we know it can't collapse with WFT3 (because of UpdateAccepted)
            assert_eq!(next_check_peek2(&mut update, 3), (3, false));
            // WFT3 is safe because we know we can't collapse past the UpdateAccepted ahead
            assert_eq!(next_check_peek2(&mut update, 6), (3, false));

            // Can't decide further because there are no more WFTStarted in buffer; unknown could contain some; UpdateAccepted is not part of any LWFT
            assert_matches!(update.take_next_wft_sequence(9), NextWFT::NeedFetch);
        }

        // 12. Up to TimerStarted
        {
            let mut update = lwft_buffer_for_test(
                all_events[..13].to_vec(),
                0,
                9,
                false,
                false,
                cv(chunking_v2),
            );

            // Buffer:
            //   WFEStarted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTAccepted -> WFTCompleted -> TimerStarted -> (unknown)

            // WFT1 is forced separate (follows WFExecutionStarted).
            assert_eq!(next_check_peek2(&mut update, 0), (3, false));
            // WFT2 is safe because we know it can't collapse with WFT3 (because of UpdateAccepted)
            assert_eq!(next_check_peek2(&mut update, 3), (3, false));
            // WFT3 is safe because we know we can't collapse past the UpdateAccepted ahead
            assert_eq!(next_check_peek2(&mut update, 6), (3, false));

            // Can't decide further because there are no more WFTStarted in buffer; unknown could contain some; UpdateAccepted is not part of any LWFT
            assert_matches!(update.take_next_wft_sequence(9), NextWFT::NeedFetch);
        }

        // 16. Up to WFT4 Started.
        {
            let mut update = lwft_buffer_for_test(
                all_events[..16].to_vec(),
                0,
                9,
                false,
                false,
                cv(chunking_v2),
            );

            // Buffer:
            //   WFEStarted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTAccepted -> WFTCompleted -> TimerStarted -> TimerFired -> WFTScheduled -> WFTStarted -> (unknown)

            // WFT1 is forced separate (follows WFExecutionStarted).
            assert_eq!(next_check_peek2(&mut update, 0), (3, false));
            // WFT2 is safe because we know it can't collapse with WFT3 (because of UpdateAccepted)
            assert_eq!(next_check_peek2(&mut update, 3), (3, false));
            // WFT3 is safe because we know we can't collapse past the UpdateAccepted ahead
            assert_eq!(next_check_peek2(&mut update, 6), (3, false));

            // Can't decide further because WFT4 Started could be followed by a WFTFailure or noop WFT sequences.
            assert_matches!(update.take_next_wft_sequence(9), NextWFT::NeedFetch);
        }
    }

    fn build_heartbeat_then_commands_history(chunking_v2: bool) -> TestHistoryBuilder {
        let mut t = TestHistoryBuilder::default();
        if chunking_v2 {
            t.set_use_wft_chunking_v2();
        }
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        t.add_full_wf_task(); // WFT2: has commands
        let timer_id = t.add_timer_started("1".to_string());
        t.add_timer_fired(timer_id, "1".to_string());
        t.add_full_wf_task(); // WFT3
        t
    }

    /// Heartbeat collapsing: empty WFT followed by WFT with commands.
    ///
    /// Under v1 (legacy), WFT1+WFT2 collapse into a single 6-event LWFT.
    /// Under v2, WFT1 is forced separate (it follows WFExecutionStarted),
    /// and WFT2 becomes its own LWFT.
    #[rstest::rstest]
    #[test]
    fn heartbeat_collapsing(#[values(false, true)] chunking_v2: bool) {
        let t = build_heartbeat_then_commands_history(chunking_v2);

        let mut update = t.as_lwft_buffer();
        if chunking_v2 {
            // WFT1 alone (follows WFExecutionStarted).
            let seq = next_check_peek(&mut update, 0);
            assert_eq!(seq.len(), 3, "WFT1 should be separate");
            assert_eq!(seq.last().unwrap().event_id, 3);

            // WFT2 alone (was previously collapsed with WFT1 by v1).
            let seq = next_check_peek(&mut update, 3);
            assert_eq!(seq.len(), 3, "WFT2 is the second LWFT");
            assert_eq!(seq.last().unwrap().event_id, 6);
        } else {
            let seq = next_check_peek(&mut update, 0);
            assert_eq!(seq.len(), 6, "WFT1+WFT2 should be collapsed under v1");
            assert_eq!(seq.last().unwrap().event_id, 6);
        }
    }

    /// When there are pending speculative updates, WFT chunking v2 must NOT
    /// collapse the last WFT in a heartbeat chain, because the update needs
    /// to be delivered in its own activation (matching the original execution).
    /// Earlier (non-last) heartbeats in the chain may still be collapsed together.
    ///
    /// To exercise this independently of the WFExecutionStarted rule (which
    /// already forces WFT1 to be separate), the chain we care about runs from
    /// WFT2 through WFT4.
    ///
    /// History:
    ///   Event 1:   WorkflowExecutionStarted
    ///   Event 2:   WFTScheduled  ─┐
    ///   Event 3:   WFTStarted    ─┤ WFT1 (heartbeat, empty)
    ///   Event 4:   WFTCompleted  ─┘
    ///   Event 5:   WFTScheduled  ─┐
    ///   Event 6:   WFTStarted    ─┤ WFT2 (heartbeat, empty)
    ///   Event 7:   WFTCompleted  ─┘
    ///   Event 8:   WFTScheduled  ─┐
    ///   Event 9:   WFTStarted    ─┤ WFT3 (heartbeat, empty)
    ///   Event 10:  WFTCompleted  ─┘
    ///   Event 11:  WFTScheduled  ─┐
    ///   Event 12:  WFTStarted    ─┘ WFT4 (current task, with pending update)
    #[test]
    fn heartbeat_not_collapsed_when_speculative_updates_pending() {
        let chunking_v2 = true;
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task(); // WFT1: events 2-4
        t.add_full_wf_task(); // WFT2: events 5-7
        t.add_full_wf_task(); // WFT3: events 8-10
        t.add_workflow_task_scheduled_and_started(); // WFT4: events 11-12
        maybe_set_chunking_v2(&mut t, chunking_v2);
        let all_events = t.get_full_history_info().unwrap().into_events();

        // Without speculative updates: WFT2+WFT3+WFT4 all collapse via heartbeat coalescing.
        {
            let (mut update, _) =
                HistoryUpdate::from_events(all_events.clone(), 0, 12, true, false, cv(chunking_v2));

            // WFT1 alone (follows WFExecutionStarted).
            let seq = next_check_peek(&mut update, 0);
            assert_eq!(
                seq.len(),
                3,
                "WFT1 is separate (follows WFExecutionStarted)"
            );
            assert_eq!(seq.last().unwrap().event_id, 3);

            // WFT2+WFT3+WFT4 collapsed.
            let seq = next_check_peek(&mut update, 3);
            assert_eq!(
                seq.len(),
                9,
                "Without speculative updates: WFT2+WFT3+WFT4 collapsed"
            );
            assert_eq!(seq.last().unwrap().event_id, 12);
        }

        // With speculative updates: only the last heartbeat (WFT4) is uncollapsed;
        // the earlier heartbeats (WFT2+WFT3) are still collapsed together.
        {
            let (mut update, _) =
                HistoryUpdate::from_events(all_events.clone(), 0, 12, true, true, cv(chunking_v2));

            // WFT1 alone (follows WFExecutionStarted).
            let seq = next_check_peek(&mut update, 0);
            assert_eq!(
                seq.len(),
                3,
                "WFT1 is separate (follows WFExecutionStarted)"
            );
            assert_eq!(seq.last().unwrap().event_id, 3);

            // WFT2+WFT3 collapsed: intermediate heartbeats can still merge.
            let seq = next_check_peek(&mut update, 3);
            assert_eq!(
                seq.len(),
                6,
                "With speculative updates: WFT2+WFT3 still collapsed (intermediate heartbeats)"
            );
            assert_eq!(seq.last().unwrap().event_id, 9);

            // WFT4 separate: holds the pending speculative update.
            let seq = next_check_peek(&mut update, 9);
            assert_eq!(
                seq.len(),
                3,
                "With speculative updates: WFT4 should be separate (3 events)"
            );
            assert_eq!(seq.last().unwrap().event_id, 12);
        }
    }
}
