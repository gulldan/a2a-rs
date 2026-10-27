// Copyright AGNTCY Contributors (https://github.com/agntcy)
// Copyright A2A Contributors (https://github.com/a2aproject)
// SPDX-License-Identifier: Apache-2.0
//! Live (bidirectional) exchanges: `SendLiveMessage` (A2A 1.1 §3.1.12).
//!
//! A live exchange is a session bound to one task. Its uplink carries the
//! client's [`StreamRequest`]s and its downlink the task's events. The session
//! decides, per message, whether the task is running (the message is appended
//! under the task's writer lock and handed to the executor) or idle (the
//! message starts a new turn), and it keeps the downlink open across turns:
//! only a terminal task, a client that goes away, or an uplink that ends with
//! nothing left to say closes it.
//!
//! # Known limitations
//!
//! - A call another client routes into an exchange it did not open has no
//!   downlink: a rejection goes to the opener's downlink and the caller gets
//!   the task back, in which it can see its message is missing.
//! - A call lost to a panic is acknowledged when its message is in the task,
//!   even if no executor read it; an artifact-only call, or one with no
//!   message id, cannot be checked and is acknowledged.
//! - A message injected into a turn another client started reaches an idle
//!   exchange through the next snapshot, not as events.
//! - An executor that publishes more than the execution buffer holds
//!   (`EXECUTION_BUFFER_CAPACITY` events) faster than a session drains them
//!   ends that exchange with an error, as it does a streaming subscription.
//! - Without replay, `lastSeenGeneration` resumes from a snapshot.
//! - A push receiver that, before it answers a push, calls back into the task
//!   (a follow-up message, a cancel) waits until the push times out: the
//!   task's pushes are delivered one at a time, in order, and the follow-up's
//!   first push queues behind the one still unanswered.
//! - With `fail_on_error`, a failure to push the last event of a turn that
//!   ends without a terminal state is logged, not reported to the stream.
//! - Pushes of events published by different callers within one turn (a live
//!   input and the executor's reply) are ordered as the callers reach the
//!   push lock, which can differ from the order they were published in.
//! - A turn is closed ahead of the push of its last event only when the
//!   executor's stream ends right after that event; an executor that awaits
//!   something after its last yield keeps the input queue open for the push.

use super::*;
use crate::executor::{ClientInput, InputRefusal};
use futures::FutureExt;
use tokio_stream::wrappers::ReceiverStream;

/// What a live session's uplink carries: a request, and, for an HTTP uplink
/// call that is waiting for its acknowledgement, where to send the answer.
pub(crate) struct LiveItem {
    pub(crate) request: StreamRequest,
    /// Who made this call, when it is not who opened the exchange (a later
    /// HTTP call has its own credentials and version).
    pub(crate) params: Option<ServiceParams>,
    /// `Some(task)`: the request was handled, here is the task. `None`: the
    /// exchange ended before it got to this request, so nothing was done.
    /// A dropped sender: it was handled but the task could not be read back.
    pub(crate) ack: Option<AckGuard>,
}

/// The answer to an HTTP uplink call, which says "not reached" if the item is
/// dropped without being handled: when the session ends, panics, or never gets
/// to the call. The session takes the sender out (`into_sender`) when it starts
/// handling the call, after which a drop means "handled, outcome unknown".
pub(crate) struct AckGuard(Option<oneshot::Sender<Option<Task>>>);

impl AckGuard {
    pub(crate) fn new(sender: oneshot::Sender<Option<Task>>) -> Self {
        AckGuard(Some(sender))
    }

    fn into_sender(mut self) -> oneshot::Sender<Option<Task>> {
        self.0.take().expect("an ack guard holds its sender")
    }
}

impl Drop for AckGuard {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(None);
        }
    }
}

/// The client's side of an exchange as the session reads it.
pub(crate) enum Uplink {
    /// Calls arriving one at a time over HTTP, through a channel the session
    /// can close.
    Http(mpsc::Receiver<LiveItem>),
    /// A stream of requests (gRPC, SLIMRPC).
    Stream(BoxStream<'static, LiveItem>),
}

impl Uplink {
    async fn next(&mut self) -> Option<LiveItem> {
        match self {
            Uplink::Http(receiver) => receiver.recv().await,
            Uplink::Stream(stream) => stream.next().await,
        }
    }

    /// Refuses any further call and answers the ones queued but never reached,
    /// so a caller that got its call in just as the exchange ended is told it
    /// was not carried out (and may open a new exchange with it) instead of
    /// waiting for an answer that will not come.
    async fn close(&mut self) {
        match self {
            Uplink::Http(receiver) => {
                // After `close` nothing more can be sent, and what was sent
                // before it is still received here.
                receiver.close();
                // Dropping an item answers its caller "not reached".
                while receiver.recv().await.is_some() {}
            }
            Uplink::Stream(stream) => while let Some(Some(_)) = stream.next().now_or_never() {},
        }
    }
}

/// The open live sessions, by task, so a short-lived HTTP call can be routed to
/// the session whose downlink is already open.
#[derive(Default)]
pub(crate) struct LiveSessions {
    open: std::sync::Mutex<HashMap<TaskId, Vec<mpsc::Sender<LiveItem>>>>,
}

impl LiveSessions {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<TaskId, Vec<mpsc::Sender<LiveItem>>>> {
        self.open
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn register(&self, task_id: &str, sender: mpsc::Sender<LiveItem>) {
        self.lock()
            .entry(task_id.to_string())
            .or_default()
            .push(sender);
    }

    /// The newest session still open for the task.
    fn route(&self, task_id: &str) -> Option<mpsc::Sender<LiveItem>> {
        self.lock()
            .get(task_id)?
            .iter()
            .rev()
            .find(|sender| !sender.is_closed())
            .cloned()
    }

    fn unregister(&self, task_id: &str, sender: &mpsc::Sender<LiveItem>) {
        let mut open = self.lock();
        if let Some(senders) = open.get_mut(task_id) {
            senders.retain(|current| !current.same_channel(sender) && !current.is_closed());
            if senders.is_empty() {
                open.remove(task_id);
            }
        }
    }
}

/// The answer to one `SendLiveMessage` call over a transport that has no
/// native duplex (A2A 1.1 §3.1.12): the call that opens the exchange is the
/// downlink, and a later call for the same task only gets the task state back.
#[non_exhaustive]
pub enum LiveResponse {
    /// This call is the downlink: the events, until the exchange ends.
    Stream(BoxStream<'static, Result<StreamResponse, A2AError>>),
    /// This call was input for an exchange that is already open; here is the
    /// task as it stands. Its events arrive on the downlink, never here.
    Ack(Box<Task>),
}

/// Why a live request was not carried out.
pub(super) enum Rejection {
    /// The agent is not taking input now (not accepting it while it works, or
    /// its buffer is full). Not an error of the request: the client may retry.
    Declined(String),
    Failed(A2AError),
}

impl From<A2AError> for Rejection {
    fn from(error: A2AError) -> Self {
        Rejection::Failed(error)
    }
}

impl Rejection {
    /// The error to give a caller that is not on a stream yet.
    pub(super) fn into_error(self) -> A2AError {
        match self {
            Rejection::Declined(reason) => A2AError::unsupported_operation(reason),
            Rejection::Failed(error) => error,
        }
    }

    /// The non-fatal rejection of one request on a live stream: the code is the
    /// one the equivalent unary operation would report.
    fn into_stream_error(self, task_id: &str, reference_id: Option<String>) -> StreamResponse {
        let (code, message) = match self {
            Rejection::Declined(reason) => ("UNAVAILABLE".to_string(), reason),
            Rejection::Failed(error) => (stream_error_code(&error), error.message),
        };
        StreamResponse::Error(StreamError {
            task_id: task_id.to_string(),
            code,
            message,
            reference_id: reference_id.filter(|id| !id.is_empty()),
            metadata: None,
        })
    }
}

/// Why a message is declined when its turn has closed its input queue.
const INPUT_CLOSED: &str = "the agent is no longer taking input";

fn stream_error_code(error: &A2AError) -> String {
    match error.code {
        error_code::INVALID_PARAMS
        | error_code::INVALID_REQUEST
        | error_code::PARSE_ERROR
        | error_code::CONTENT_TYPE_NOT_SUPPORTED => "INVALID_ARGUMENT".to_string(),
        error_code::TASK_NOT_FOUND => "NOT_FOUND".to_string(),
        code => error_reason(code).to_string(),
    }
}

/// A message that reached a task, and what the session needs to carry on.
pub(super) enum Delivery {
    /// The message started a new turn.
    Started {
        task: Task,
        receiver: broadcast::Receiver<ExecutionEvent>,
        /// What the request recorded before the turn began, as events: the
        /// message as it was appended, then each artifact. A stream that
        /// already holds the task is told them; the first request's opening
        /// snapshot already reflects them.
        recorded: Vec<StreamResponse>,
    },
    /// The message went into the execution that was running.
    Injected {
        task: Task,
        receiver: broadcast::Receiver<ExecutionEvent>,
        /// The execution's sequence before the message's events were published,
        /// and after: a stream that already holds the task resumes after the
        /// latter, one that joins now takes them from the former.
        sequence_before: u64,
        sequence_after: u64,
    },
}

/// The generation an event reports, if it reports one.
fn generation_of(event: &StreamResponse) -> Option<i64> {
    match event {
        StreamResponse::Task(task) => Some(task.generation),
        StreamResponse::StatusUpdate(update) => Some(update.generation),
        StreamResponse::ArtifactUpdate(update) => Some(update.generation),
        StreamResponse::MessageUpdate(update) => Some(update.generation),
        _ => None,
    }
}

/// How soon an idle session looks at its task again, and how far it backs off
/// while nothing changes. (A server-wide wake-up for every change anywhere
/// would have every idle session reading the store each time.)
const IDLE_RECHECK: Duration = Duration::from_millis(250);
const IDLE_RECHECK_MAX: Duration = Duration::from_secs(1);

/// What looking at an idle task found.
enum Reattach {
    /// The exchange is over (the task ended, or is gone).
    End,
    /// The exchange follows a running execution now, or was told of a change.
    Changed,
    Unchanged,
}

/// Whether `task` holds the message `request` carries (always true for a
/// request that has no message id to look for).
fn records_message(task: &Task, request: &StreamRequest) -> bool {
    let StreamRequestPayload::SendMessage(send) = &request.payload else {
        return true;
    };
    let id = &send.message.message_id;
    id.is_empty()
        || task.history.iter().flatten().any(|m| &m.message_id == id)
        || task.timeline.iter().flatten().any(
            |entry| matches!(&entry.content, TimelineContent::Message(m) if &m.message_id == id),
        )
}

impl DefaultRequestHandler {
    /// The `RequestHandler::send_live_request` of the default handler: routes a
    /// call to the exchange that is open for its task, or opens one.
    pub(super) async fn live_request(
        &self,
        params: &ServiceParams,
        request: StreamRequest,
    ) -> Result<LiveResponse, A2AError> {
        self.require_streaming()?;
        if let StreamRequestPayload::SendMessage(message) = &request.payload {
            if let Some(task_id) = request_task_id(message) {
                if let Some(session) = self.live.route(&task_id) {
                    self.authorize(params, Some(&task_id))?;
                    let (ack, acknowledged) = oneshot::channel();
                    let item = LiveItem {
                        request: request.clone(),
                        params: Some(params.clone()),
                        ack: Some(AckGuard::new(ack)),
                    };
                    let sent = session.send(item).await.is_ok();
                    // Not held while waiting: the session's channel must be
                    // free to close when the exchange ends.
                    drop(session);
                    if sent {
                        match acknowledged.await {
                            Ok(Some(task)) => return Ok(LiveResponse::Ack(Box::new(task))),
                            // Handled, but the task could not be read back
                            // then: it is read now, and the request is not
                            // delivered a second time.
                            // Unless the exchange was lost midway, and the
                            // message is not in the task: then nothing says it
                            // was carried out, and the caller is told so.
                            Err(_) => {
                                let task = self.load_task(&task_id).await?;
                                if !records_message(&task, &request) {
                                    return Err(A2AError::internal(
                                        "the live exchange failed while handling the request",
                                    ));
                                }
                                return Ok(LiveResponse::Ack(Box::new(task)));
                            }
                            // The exchange ended before it got to this call,
                            // which opens a new one below.
                            Ok(None) => {}
                        }
                    }
                }
            }
        }

        let (sender, receiver) = mpsc::channel(LIVE_UPLINK_CAPACITY);
        let _ = sender
            .send(LiveItem {
                request,
                params: None,
                ack: None,
            })
            .await;
        let downlink = self
            .open_live(params, Uplink::Http(receiver), Some(sender))
            .await?;
        Ok(LiveResponse::Stream(downlink))
    }

    /// Opens an exchange whose uplink is `uplink` (its first item opens it),
    /// returning the downlink.
    pub(super) async fn open_live(
        &self,
        params: &ServiceParams,
        mut uplink: Uplink,
        registration: Option<mpsc::Sender<LiveItem>>,
    ) -> Result<BoxStream<'static, Result<StreamResponse, A2AError>>, A2AError> {
        self.require_streaming()?;
        let wants_message_updates = is_at_least_1_1(&requested_protocol_version(params));
        let first = uplink
            .next()
            .await
            .ok_or_else(|| A2AError::invalid_request("a live stream needs a first request"))?;

        // The generation the client has already been told of, when it said so.
        let mut seen_generation = 0;
        let (task_id, opening, inner) = match first.request.payload {
            StreamRequestPayload::SendMessage(request) => {
                self.authorize(params, request_task_id(&request).as_deref())?;
                match self
                    .live_deliver(params, request, StartMode::Live)
                    .await
                    .map_err(Rejection::into_error)?
                {
                    Delivery::Started { task, receiver, .. } => {
                        let events = subscription_stream(
                            receiver,
                            None,
                            0,
                            false,
                            wants_message_updates,
                            true,
                        );
                        (task.id.clone(), Some(task), Some(events))
                    }
                    Delivery::Injected {
                        task,
                        receiver,
                        sequence_after,
                        ..
                    } => {
                        // The snapshot already reflects the message: its own
                        // events are not sent a second time.
                        let events = subscription_stream(
                            receiver,
                            None,
                            sequence_after,
                            false,
                            wants_message_updates,
                            true,
                        );
                        (task.id.clone(), Some(task), Some(events))
                    }
                }
            }
            StreamRequestPayload::Subscribe(subscribe) => {
                self.authorize(params, Some(&subscribe.id))?;
                let task_id = subscribe.id.clone();
                let last_seen = subscribe.last_seen_generation;
                seen_generation = last_seen.unwrap_or(0);
                match self.subscribe_stream(params, subscribe, true).await {
                    Ok(events) => (task_id, None, Some(events)),
                    // An idle task that is not finished is still there to be
                    // observed: the exchange opens with it (unless the client
                    // already holds this generation) and waits.
                    Err(error) if error.code == error_code::TASK_NOT_FOUND => {
                        match self.task_store.get(&task_id).await? {
                            Some(task) if !task.status.state.is_terminal() => {
                                let current = last_seen == Some(task.generation);
                                (task_id, (!current).then_some(task), None)
                            }
                            _ => return Err(error),
                        }
                    }
                    Err(error) => return Err(error),
                }
            }
            _ => {
                return Err(A2AError::invalid_request(
                    "unrecognised StreamRequest payload",
                ));
            }
        };

        if let Some(registration) = &registration {
            self.live.register(&task_id, registration.clone());
        }
        let (out, downlink) = mpsc::channel(LIVE_DOWNLINK_CAPACITY);
        let session = LiveRun {
            handler: self.clone(),
            params: params.clone(),
            task_id,
            wants_message_updates,
            registration,
        };
        let (failed_out, handler, task_id, registration) = (
            out.clone(),
            self.clone(),
            session.task_id.clone(),
            session.registration.clone(),
        );
        tokio::spawn(async move {
            let run = session.run(opening, inner, seen_generation, uplink, out);
            if std::panic::AssertUnwindSafe(run)
                .catch_unwind()
                .await
                .is_err()
            {
                // The session is gone: no call is routed to it any more, and
                // the client is told its exchange failed instead of just ending.
                if let Some(registration) = &registration {
                    handler.live.unregister(&task_id, registration);
                }
                let _ = failed_out
                    .send(Err(A2AError::internal("the live exchange failed")))
                    .await;
            }
        });
        Ok(ReceiverStream::new(downlink).boxed())
    }

    /// Hands a message to a task: into the execution that is running, or as the
    /// start of a new turn.
    ///
    /// The choice and what follows from it happen under the task's writer, which
    /// the end of a turn also takes to close its input queue and deregister: the
    /// message finds either a running execution that is taking input, or none.
    pub(super) async fn live_deliver(
        &self,
        params: &ServiceParams,
        request: SendMessageRequest,
        mode: StartMode,
    ) -> Result<Delivery, Rejection> {
        self.validate_artifact_updates(&request)?;
        if !is_artifact_only(&request) {
            self.validate_content_types(&request.message)?;
        }
        // No task named: the message creates one, and there is nothing to join.
        let Some(task_id) = request_task_id(&request) else {
            return self.start_delivery(params, request, mode, None).await;
        };
        let writer = self
            .execution_manager
            .writer_for(&task_id)
            .lock_owned()
            .await;
        // An entry with no snapshot is a request that was dropped before it
        // began; it is no execution.
        let running = match self.execution_manager.get(&task_id).await {
            Some(active) if active.snapshot().await.is_some() => Some(active),
            _ => None,
        };
        match running {
            Some(active) => {
                self.inject(params, &request, &task_id, &active, writer)
                    .await
            }
            None => {
                self.start_delivery(params, request, mode, Some(writer))
                    .await
            }
        }
    }

    async fn start_delivery(
        &self,
        params: &ServiceParams,
        request: SendMessageRequest,
        mode: StartMode,
        writer: Option<tokio::sync::OwnedMutexGuard<()>>,
    ) -> Result<Delivery, Rejection> {
        let (task, receiver, recorded) = self
            .start_execution(params, request, mode, writer)
            .await
            .map_err(Rejection::Failed)?;
        Ok(Delivery::Started {
            task,
            receiver,
            recorded,
        })
    }

    /// Appends the message to the task and passes it to the running executor,
    /// under `writer`, which the caller holds for the task.
    async fn inject(
        &self,
        params: &ServiceParams,
        request: &SendMessageRequest,
        task_id: &str,
        active: &Arc<ActiveExecution>,
        writer: tokio::sync::OwnedMutexGuard<()>,
    ) -> Result<Delivery, Rejection> {
        // A task the executor has not created yet is not one a client can know.
        if active.uncreated.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(Rejection::Failed(A2AError::task_not_found(task_id)));
        }
        let Some(base) = active.snapshot().await else {
            return Err(Rejection::Failed(A2AError::internal(
                "a running execution has no task",
            )));
        };
        if base.status.state.is_terminal() {
            return Err(Rejection::Failed(A2AError::unsupported_operation(format!(
                "task {task_id} has already reached a terminal state"
            ))));
        }
        if let Some(expected) = request
            .configuration
            .as_ref()
            .and_then(|configuration| configuration.if_generation_match)
        {
            if base.generation != expected {
                return Err(Rejection::Failed(A2AError::task_generation_mismatch(
                    task_id,
                    expected,
                    base.generation,
                )));
            }
        }
        if let Some(requested) = request.message.context_id.as_deref() {
            if requested != base.context_id {
                return Err(Rejection::Failed(A2AError::invalid_params(format!(
                    "contextId {requested} does not match task {task_id}, \
                     which belongs to context {}",
                    base.context_id
                ))));
            }
        }

        let artifact_only = is_artifact_only(request);
        if !artifact_only
            && request.message.message_id.is_empty()
            && !request.artifact_updates.is_empty()
        {
            return Err(Rejection::Failed(A2AError::invalid_params(
                "a message sent with artifactUpdates must have a messageId",
            )));
        }
        // Room for the message is secured before anything is changed, so a
        // message the executor cannot take is declined with nothing recorded.
        let permit = if artifact_only {
            None
        } else {
            let inputs = active.inputs.get().ok_or_else(|| {
                Rejection::Declined("the agent does not take input while it works".to_string())
            })?;
            Some(inputs.try_reserve().map_err(|refusal| match refusal {
                InputRefusal::Full => {
                    Rejection::Declined("the agent is busy with earlier input".to_string())
                }
                InputRefusal::Closed => Rejection::Declined(INPUT_CLOSED.to_string()),
            })?)
        };

        // A push configuration sent with the message is registered like any
        // other (A2A 1.1 §3.1.12), once the message is certain to be taken.
        self.save_request_push_config(params, task_id, request)
            .await?;

        let mut task = base;
        let mut events = Vec::new();
        let mut message_generation = 0;
        if !artifact_only {
            task.history
                .get_or_insert_with(Vec::new)
                .push(request.message.clone());
            task.generation += 1;
            message_generation = task.generation;
            task.timeline
                .get_or_insert_with(Vec::new)
                .push(TimelineEntry {
                    content: TimelineContent::Message(request.message.clone()),
                    generation: message_generation,
                });
            events.push(StreamResponse::MessageUpdate(TaskMessageUpdateEvent {
                task_id: task.id.clone(),
                context_id: task.context_id.clone(),
                message: request.message.clone(),
                metadata: None,
                generation: message_generation,
            }));
        }
        for update in apply_client_artifacts(&mut task, request, false)? {
            events.push(StreamResponse::ArtifactUpdate(update));
        }
        let task = save_task(self.task_store.as_ref(), task).await?;

        let sequence_before = active.state.read().await.sequence;
        let receiver = active.subscribe();
        let last = events.len().saturating_sub(1);
        for (index, event) in events.iter().enumerate() {
            let snapshot = (index == last).then(|| task.clone());
            active.publish_marked(Ok(event), snapshot, false).await;
        }
        if events.is_empty() {
            active.set_snapshot(task.clone()).await;
        }
        let sequence_after = active.state.read().await.sequence;
        if let Some(permit) = permit {
            permit.send(ClientInput::new(
                request.message.clone(),
                message_generation,
            ));
        }
        // The webhooks are called in the background, in order, so a slow
        // receiver does not hold up the session that took the message. Their
        // place in line is taken here, before the executor can answer.
        spawn_push_notifications(
            &self.execution_manager,
            task_id,
            self.push_config_store.clone(),
            self.push_sender.clone(),
            events,
        )
        .await;
        drop(writer);
        self.task_changed.notify_waiters();

        Ok(Delivery::Injected {
            task,
            receiver,
            sequence_before,
            sequence_after,
        })
    }
}

impl DefaultRequestHandler {
    /// `SubscribeToTask`, for an ordinary stream or (`live`) for the downlink of
    /// a live exchange, which is also told what only a live exchange is.
    pub(super) async fn subscribe_stream(
        &self,
        params: &ServiceParams,
        req: SubscribeToTaskRequest,
        live: bool,
    ) -> Result<BoxStream<'static, Result<StreamResponse, A2AError>>, A2AError> {
        self.authorize(params, Some(&req.id))?;
        self.require_streaming()?;
        // An execution that is still being prepared has no snapshot to open
        // with yet. Wait for the writer -- the request preparing it holds it --
        // and look again, so the stream opens with the task, or, if the
        // request failed and released the task, falls through to the checks
        // below. The wait follows the lookup that found the execution, not one
        // before it: a registration made in between is waited for too.
        // A task the executor has not created yet is not one a client can know.
        if let Some(active) = self.execution_manager.get(&req.id).await {
            if active.uncreated.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(A2AError::task_not_found(&req.id));
            }
        }
        let mut resubscribed = self.execution_manager.resubscribe(&req.id).await;
        if matches!(resubscribed, Some((_, None, _))) {
            resubscribed = match self.execution_manager.get(&req.id).await {
                Some(active) => {
                    let _writer = active.writer.lock().await;
                    // With the writer held a reservation that is still empty
                    // belongs to a dropped request, so the task is not
                    // executing; nothing is left to wait for.
                    match active.snapshot().await {
                        Some(_) => self.execution_manager.resubscribe(&req.id).await,
                        None => None,
                    }
                }
                None => None,
            };
        }
        if let Some((receiver, snapshot_task, sequence)) = resubscribed {
            // A client that already holds the task's current generation has
            // missed nothing, so it resumes without a snapshot. One that is
            // behind cannot be replayed to -- events are not retained -- and
            // falls back to a snapshot, which the spec allows.
            let snapshot_task = match (req.last_seen_generation, snapshot_task) {
                (Some(last_seen), Some(task))
                    if task.generation == last_seen && !task.status.state.is_terminal() =>
                {
                    None
                }
                (_, snapshot) => snapshot,
            };
            return Ok(subscription_stream(
                receiver,
                snapshot_task,
                sequence,
                false,
                is_at_least_1_1(&requested_protocol_version(params)),
                live,
            ));
        }

        // A registry miss also happens once a task finishes and its
        // execution is cleaned up, so it doesn't by itself mean the task
        // never existed -- check the store to tell the two apart before
        // reporting TaskNotFoundError.
        if let Some(task) = self.task_store.get(&req.id).await? {
            if task.status.state.is_terminal() {
                return Err(A2AError::unsupported_operation(format!(
                    "task {} has already reached a terminal state",
                    req.id
                )));
            }
        }
        Err(A2AError::task_not_found(&req.id))
    }
}

/// One running exchange: the loop that owns the downlink.
struct LiveRun {
    handler: DefaultRequestHandler,
    /// Who opened the exchange; a request that carries its own are served as
    /// that caller.
    params: ServiceParams,
    /// The task the exchange is bound to, for its whole life.
    task_id: TaskId,
    wants_message_updates: bool,
    registration: Option<mpsc::Sender<LiveItem>>,
}

type Events = BoxStream<'static, Result<StreamResponse, A2AError>>;
type Downlink = mpsc::Sender<Result<StreamResponse, A2AError>>;

impl LiveRun {
    async fn run(
        self,
        opening: Option<Task>,
        mut inner: Option<Events>,
        seen_generation: i64,
        mut uplink: Uplink,
        out: Downlink,
    ) {
        // The generation the client has been told of, so a change it was not
        // there for (an idle task, then another client's turn) is noticed.
        let mut last_generation = seen_generation;
        if let Some(task) = opening {
            last_generation = task.generation;
            if out.send(Ok(StreamResponse::Task(task))).await.is_err() {
                return self.finish(uplink).await;
            }
        }
        let mut uplink_open = true;
        let mut idle_wait = IDLE_RECHECK;
        loop {
            tokio::select! {
                // The client went away: nothing is left to deliver to.
                () = out.closed() => break,
                event = async {
                    match inner.as_mut() {
                        Some(events) => events.next().await,
                        None => std::future::pending().await,
                    }
                } => match event {
                    Some(item) => {
                        let ends = match &item {
                            Ok(event) => is_terminal_event(event),
                            Err(_) => true,
                        };
                        if let Some(generation) = item.as_ref().ok().and_then(generation_of) {
                            last_generation = last_generation.max(generation);
                        }
                        if out.send(item).await.is_err() || ends {
                            break;
                        }
                    }
                    None => {
                        // The execution is over. The task may not be: the
                        // exchange stays open for the next message.
                        inner = None;
                        if !uplink_open {
                            break;
                        }
                    }
                },
                // Nothing is running that this exchange follows: it watches the
                // task, so another client's turn, or a cancel, reaches it too.
                () = tokio::time::sleep(idle_wait), if inner.is_none() => {
                    match self.reattach(&mut inner, &mut last_generation, &out).await {
                        Reattach::End => break,
                        // Something happened: look again soon.
                        Reattach::Changed => idle_wait = IDLE_RECHECK,
                        // Nothing did: look a little less often, up to a limit.
                        Reattach::Unchanged => {
                            idle_wait = (idle_wait * 2).min(IDLE_RECHECK_MAX);
                        }
                    }
                },
                item = uplink.next(), if uplink_open => match item {
                    Some(item) => {
                        if !self.handle(item, &mut inner, &mut last_generation, &out).await {
                            break;
                        }
                    }
                    None => {
                        uplink_open = false;
                        if inner.is_none() {
                            break;
                        }
                    }
                },
            }
        }
        self.finish(uplink).await;
    }

    /// Ends the exchange: no more calls are routed to it, and a call that was
    /// queued but never reached is told so (it is delivered nowhere, so the
    /// caller may open a new exchange with it).
    async fn finish(&self, mut uplink: Uplink) {
        if let Some(registration) = &self.registration {
            self.handler.live.unregister(&self.task_id, registration);
        }
        uplink.close().await;
    }

    /// Looks at the task while the exchange follows no execution. Follows a new
    /// one if it has started, tells the client of changes it was not there for
    /// (and that the task has ended). `false` when the exchange is over.
    async fn reattach(
        &self,
        inner: &mut Option<Events>,
        last_generation: &mut i64,
        out: &Downlink,
    ) -> Reattach {
        if let Some((receiver, Some(snapshot), sequence)) = self
            .handler
            .execution_manager
            .resubscribe(&self.task_id)
            .await
        {
            // Events carry on from the snapshot, which tells where things stand.
            *last_generation = (*last_generation).max(snapshot.generation);
            *inner = Some(subscription_stream(
                receiver,
                Some(snapshot),
                sequence,
                false,
                self.wants_message_updates,
                true,
            ));
            return Reattach::Changed;
        }
        let Ok(task) = self.handler.load_task(&self.task_id).await else {
            return Reattach::End;
        };
        let ended = task.status.state.is_terminal();
        let changed = task.generation > *last_generation;
        if ended || changed {
            *last_generation = task.generation;
            if out.send(Ok(StreamResponse::Task(task))).await.is_err() {
                return Reattach::End;
            }
        }
        if ended {
            Reattach::End
        } else if changed {
            Reattach::Changed
        } else {
            Reattach::Unchanged
        }
    }

    /// Carries out one request that arrived after the exchange opened. Returns
    /// `false` when the downlink is gone.
    async fn handle(
        &self,
        item: LiveItem,
        inner: &mut Option<Events>,
        last_generation: &mut i64,
        out: &Downlink,
    ) -> bool {
        let LiveItem {
            request,
            params,
            ack,
        } = item;
        // From here the call is being handled: if this task is lost midway, its
        // caller is told the outcome is unknown, not that nothing was done.
        let ack = ack.map(AckGuard::into_sender);
        let params = params.unwrap_or_else(|| self.params.clone());
        let mut reference_id = None;
        let result: Result<bool, Rejection> = match request.payload {
            StreamRequestPayload::SendMessage(mut request) => {
                reference_id = Some(request.message.message_id.clone())
                    .filter(|id| !id.is_empty())
                    .or_else(|| {
                        request
                            .artifact_updates
                            .first()
                            .map(|update| update.artifact.artifact_id.clone())
                    });
                match self.bind(&mut request) {
                    Ok(()) => {
                        self.deliver(&params, request, inner, last_generation, out)
                            .await
                    }
                    Err(rejection) => Err(rejection),
                }
            }
            StreamRequestPayload::Subscribe(subscribe) => {
                reference_id = Some(subscribe.id);
                Err(Rejection::Failed(A2AError::invalid_request(
                    "subscribe is only valid as the first request of a stream",
                )))
            }
            _ => Err(Rejection::Failed(A2AError::invalid_request(
                "unrecognised StreamRequest payload",
            ))),
        };
        let mut downlink_open = *result.as_ref().unwrap_or(&true);
        if let Err(rejection) = result {
            // Ordering is by arrival: what the exchange has already published
            // for earlier requests goes out before this one's rejection.
            downlink_open = self.flush(inner, last_generation, out).await;
            if downlink_open {
                let event = rejection.into_stream_error(&self.task_id, reference_id);
                downlink_open = out.send(Ok(event)).await.is_ok();
            }
        }
        if let Some(ack) = ack {
            // Answered whatever became of the downlink: the call was handled.
            // Dropped, not answered, only when the task cannot be read back;
            // the caller then reads it itself and does not send it again.
            if let Ok(task) = self.handler.load_task(&self.task_id).await {
                let _ = ack.send(Some(task));
            }
        }
        downlink_open
    }

    /// Forwards what the execution being followed has published already,
    /// without waiting for more. `false` when the downlink is gone or the
    /// exchange ended with a terminal event.
    async fn flush(
        &self,
        inner: &mut Option<Events>,
        last_generation: &mut i64,
        out: &Downlink,
    ) -> bool {
        while let Some(events) = inner.as_mut() {
            match events.next().now_or_never() {
                Some(Some(item)) => {
                    let ends = match &item {
                        Ok(event) => is_terminal_event(event),
                        Err(_) => true,
                    };
                    if let Some(generation) = item.as_ref().ok().and_then(generation_of) {
                        *last_generation = (*last_generation).max(generation);
                    }
                    if out.send(item).await.is_err() || ends {
                        return false;
                    }
                }
                Some(None) => *inner = None,
                None => break,
            }
        }
        true
    }

    /// A later request applies to the task the stream is bound to; one that
    /// names another is refused.
    fn bind(&self, request: &mut SendMessageRequest) -> Result<(), Rejection> {
        match request_task_id(request) {
            Some(named) if named != self.task_id => {
                Err(Rejection::Failed(A2AError::invalid_params(format!(
                    "this stream is bound to task {}, not {named}",
                    self.task_id
                ))))
            }
            Some(_) => Ok(()),
            None => {
                request.message.task_id = Some(self.task_id.clone());
                Ok(())
            }
        }
    }

    async fn deliver(
        &self,
        params: &ServiceParams,
        request: SendMessageRequest,
        inner: &mut Option<Events>,
        last_generation: &mut i64,
        out: &Downlink,
    ) -> Result<bool, Rejection> {
        self.handler
            .authorize(params, Some(&self.task_id))
            .map_err(Rejection::Failed)?;
        match self
            .handler
            .live_deliver(params, request, StartMode::Live)
            .await?
        {
            Delivery::Started {
                receiver, recorded, ..
            } => {
                // The execution that ended has published all it will; what
                // this stream has not forwarded yet goes out before it moves on.
                if !self.flush(inner, last_generation, out).await {
                    return Ok(false);
                }
                *inner = Some(subscription_stream(
                    receiver,
                    None,
                    0,
                    false,
                    self.wants_message_updates,
                    true,
                ));
                // The request recorded its message (and artifacts) before the
                // turn began, with nothing subscribed to hear it: this stream
                // is told, as it would be for a turn already running. An
                // artifact-only request has no message to announce.
                for event in recorded {
                    let announces_message = matches!(event, StreamResponse::MessageUpdate(_));
                    if announces_message && !self.wants_message_updates {
                        continue;
                    }
                    if let Some(generation) = generation_of(&event) {
                        *last_generation = (*last_generation).max(generation);
                    }
                    if out.send(Ok(event)).await.is_err() {
                        break;
                    }
                }
            }
            Delivery::Injected {
                receiver,
                sequence_before,
                ..
            } => {
                // A stream that already follows the execution gets the
                // message's events through it; one that does not (it was
                // idle) picks the execution up from just before them.
                if inner.is_none() {
                    *inner = Some(subscription_stream(
                        receiver,
                        None,
                        sequence_before,
                        false,
                        self.wants_message_updates,
                        true,
                    ));
                }
            }
        }
        Ok(true)
    }
}
