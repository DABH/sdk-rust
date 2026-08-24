use super::{
    GetChunkingVersionMsg,
    workflow_stream::{LocalInput, LocalInputs},
};
use crate::{
    abstractions::OwnedMeteredSemPermit,
    protosext::ValidPollWFTQResponse,
    worker::{
        WorkflowSlotKind,
        client::WorkerClient,
        workflow::{
            AutoReplyTask, CacheMissFetchReq, HistoryUpdate, NextPageReq, PermittedWFT,
            history_update::{HistoryPaginator, WFTChunkingVersion},
        },
    },
};
use futures_util::{FutureExt, Stream, StreamExt, stream, stream::PollNext};
use std::{future, sync::Arc};
use temporalio_common::protos::coresdk::WorkflowSlotInfo;
use tokio::sync::{mpsc::UnboundedSender, oneshot};
use tracing::Span;

/// Transforms incoming validated WFTs and history fetching requests into [PermittedWFT]s ready
/// for application to workflow state
pub(super) struct WFTExtractor {}

pub(super) enum WFTExtractorOutput {
    NewWFT(PermittedWFT),
    FetchResult(
        PermittedWFT,
        // Field isn't read, but we need to hold on to it.
        #[allow(dead_code)] Arc<HistfetchRC>,
    ),
    NextPage {
        paginator: HistoryPaginator,
        update: HistoryUpdate,
        span: Span,
        rc: Arc<HistfetchRC>,
    },
    FailedFetch {
        run_id: String,
        err: tonic::Status,
        auto_reply_fail_task: Option<AutoReplyTask>,
    },
    PollerDead,
}

pub(crate) type WFTStreamIn = Result<
    (
        ValidPollWFTQResponse,
        OwnedMeteredSemPermit<WorkflowSlotKind>,
    ),
    tonic::Status,
>;
#[derive(derive_more::From, Debug)]
pub(super) enum HistoryFetchReq {
    Full(Box<CacheMissFetchReq>, Arc<HistfetchRC>),
    NextPage(Box<NextPageReq>, Arc<HistfetchRC>),
}
/// Used inside of `Arc`s to ensure we don't shutdown while there are outstanding fetches.
#[derive(Debug)]
pub(super) struct HistfetchRC {}

impl WFTExtractor {
    /// Querying through the workflow stream keeps cache lookup ordered with eviction and
    /// completion processing. A missing response is treated as a cache miss so the normal replay
    /// path remains the source of truth if workflow processing has already shut down.
    pub(super) async fn get_chunking_version(
        local_tx: &UnboundedSender<LocalInput>,
        run_id: String,
    ) -> Option<WFTChunkingVersion> {
        let (response_tx, response_rx) = oneshot::channel();
        local_tx
            .send(LocalInput {
                input: LocalInputs::GetChunkingVersion(GetChunkingVersionMsg {
                    run_id,
                    response_tx,
                }),
                span: Span::current(),
            })
            .ok()?;
        response_rx.await.ok().flatten()
    }

    pub(super) fn build(
        client: Arc<dyn WorkerClient>,
        max_fetch_concurrency: usize,
        wft_stream: impl Stream<Item = WFTStreamIn> + Send + 'static,
        fetch_stream: impl Stream<Item = HistoryFetchReq> + Send + 'static,
        local_tx: UnboundedSender<LocalInput>,
    ) -> impl Stream<Item = Result<WFTExtractorOutput, tonic::Status>> + Send + 'static {
        let fetch_client = client.clone();
        let wft_stream = wft_stream
            .map(move |stream_in| {
                let client = client.clone();
                let local_tx = local_tx.clone();
                async move {
                    match stream_in {
                        Ok((wft, permit)) => {
                            let run_id = wft.workflow_execution.run_id.clone();
                            let auto_reply_fail_task = if wft.legacy_query.is_some() {
                                AutoReplyTask::LegacyQuery(wft.task_token.clone())
                            } else {
                                AutoReplyTask::Workflow(wft.task_token.clone())
                            };
                            let chunking_version =
                                Self::get_chunking_version(&local_tx, run_id.clone()).await;
                            let page =
                                HistoryPaginator::from_poll(wft, client, chunking_version).await;
                            Ok(match page {
                                Ok((pag, prep)) => WFTExtractorOutput::NewWFT(PermittedWFT {
                                    permit: permit.into_used(WorkflowSlotInfo {
                                        workflow_type: prep.workflow_type.clone(),
                                        is_sticky: prep.is_incremental(),
                                    }),
                                    work: prep,
                                    paginator: pag,
                                }),
                                Err(err) => WFTExtractorOutput::FailedFetch {
                                    run_id,
                                    err,
                                    auto_reply_fail_task: Some(auto_reply_fail_task),
                                },
                            })
                        }
                        Err(e) => Err(e),
                    }
                }
                .boxed()
            })
            // The sentinel must wait for every WFT future. Cache lookup is itself asynchronous,
            // so placing it before the final buffer would let PollerDead shut down the workflow
            // stream while an already-polled WFT is still waiting for its lookup response.
            .buffer_unordered(max_fetch_concurrency)
            .map(|output| future::ready(output).boxed())
            .chain(stream::iter([future::ready(Ok(
                WFTExtractorOutput::PollerDead,
            ))
            .boxed()]));

        stream::select_with_strategy(
            wft_stream,
            fetch_stream.map(move |fetchreq: HistoryFetchReq| {
                let client = fetch_client.clone();
                async move {
                    Ok(match fetchreq {
                        // It's OK to simply drop the refcounters in the event of fetch
                        // failure. We'll just proceed with shutdown.
                        HistoryFetchReq::Full(req, rc) => {
                            let run_id = req.original_wft.work.execution.run_id.clone();
                            let auto_reply_fail_task = if req
                                .original_wft
                                .work
                                .legacy_query
                                .is_some()
                            {
                                AutoReplyTask::LegacyQuery(req.original_wft.work.task_token.clone())
                            } else {
                                AutoReplyTask::Workflow(req.original_wft.work.task_token.clone())
                            };
                            match HistoryPaginator::from_fetchreq(req, client).await {
                                Ok(r) => WFTExtractorOutput::FetchResult(r, rc),
                                Err(err) => WFTExtractorOutput::FailedFetch {
                                    run_id,
                                    err,
                                    auto_reply_fail_task: Some(auto_reply_fail_task),
                                },
                            }
                        }
                        HistoryFetchReq::NextPage(mut req, rc) => {
                            match req.paginator.extract_next_update().await {
                                Ok(update) => WFTExtractorOutput::NextPage {
                                    paginator: req.paginator,
                                    update,
                                    span: req.span,
                                    rc,
                                },
                                Err(err) => WFTExtractorOutput::FailedFetch {
                                    run_id: req.paginator.run_id,
                                    err,
                                    auto_reply_fail_task: None,
                                },
                            }
                        }
                    })
                }
                .boxed()
            }),
            // Priority always goes to the fetching stream
            |_: &mut ()| PollNext::Right,
        )
        .buffer_unordered(max_fetch_concurrency)
    }
}
