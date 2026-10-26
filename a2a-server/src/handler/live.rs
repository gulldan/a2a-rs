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

use super::*;
use tokio_stream::wrappers::ReceiverStream;

/// What a live session's uplink carries: a request, and, for an HTTP uplink
/// call that is waiting for its acknowledgement, where to send the task state.
pub(crate) struct LiveItem {
    pub(crate) request: StreamRequest,
    pub(crate) ack: Option<oneshot::Sender<Task>>,
}

/// The open live sessions, by task, so a short-lived HTTP call can be routed to
/// the session whose downlink is already open.
#[derive(Default)]
pub(crate) struct LiveSessions {
    open: std::sync::Mutex<HashMap<TaskId, mpsc::Sender<LiveItem>>>,
}

impl LiveSessions {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<TaskId, mpsc::Sender<LiveItem>>> {
        self.open
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn register(&self, task_id: &str, sender: mpsc::Sender<LiveItem>) {
        self.lock().insert(task_id.to_string(), sender);
    }

    fn route(&self, task_id: &str) -> Option<mpsc::Sender<LiveItem>> {
        self.lock()
            .get(task_id)
            .filter(|sender| !sender.is_closed())
            .cloned()
    }

    fn unregister(&self, task_id: &str, sender: &mpsc::Sender<LiveItem>) {
        let mut open = self.lock();
        if open
            .get(task_id)
            .is_some_and(|current| current.same_channel(sender))
        {
            open.remove(task_id);
        }
    }
}

/// The answer to one `SendLiveMessage` call over a transport that has no
/// native duplex (A2A 1.1 §3.1.12): the call that opens the exchange is the
/// downlink, and a later call for the same task only gets the task state back.
pub enum LiveResponse {
    /// This call is the downlink: the events, until the exchange ends.
    Stream(BoxStream<'static, Result<StreamResponse, A2AError>>),
    /// This call was input for an exchange that is already open; here is the
    /// task as it stands. Its events arrive on the downlink, never here.
    Ack(Box<Task>),
}

/// Why a live request was not carried out.
enum Rejection {
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
    fn into_error(self) -> A2AError {
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
enum Delivery {
    /// The message started a new turn.
    Started {
        task: Task,
        receiver: broadcast::Receiver<ExecutionEvent>,
        /// The generation of the timeline entry it became (0 if none).
        message_generation: i64,
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

/// Whether `error` is the refusal of a second execution for a task that is
/// already running one.
fn is_in_progress(error: &A2AError) -> bool {
    error.code == error_code::INVALID_REQUEST && error.message.contains("already in progress")
}

fn last_message_generation(task: &Task) -> i64 {
    task.timeline
        .iter()
        .flatten()
        .rev()
        .find(|entry| matches!(entry.content, TimelineContent::Message(_)))
        .map_or(0, |entry| entry.generation)
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
                        ack: Some(ack),
                    };
                    if session.send(item).await.is_ok() {
                        if let Ok(task) = acknowledged.await {
                            return Ok(LiveResponse::Ack(Box::new(task)));
                        }
                    }
                    // The exchange ended while the call was on its way: this
                    // call opens a new one.
                }
            }
        }

        let (sender, receiver) = mpsc::channel(LIVE_UPLINK_CAPACITY);
        let _ = sender.send(LiveItem { request, ack: None }).await;
        let uplink = ReceiverStream::new(receiver).boxed();
        let downlink = self.open_live(params, uplink, Some(sender)).await?;
        Ok(LiveResponse::Stream(downlink))
    }

    /// Opens an exchange whose uplink is `uplink` (its first item opens it),
    /// returning the downlink.
    pub(super) async fn open_live(
        &self,
        params: &ServiceParams,
        mut uplink: BoxStream<'static, LiveItem>,
        registration: Option<mpsc::Sender<LiveItem>>,
    ) -> Result<BoxStream<'static, Result<StreamResponse, A2AError>>, A2AError> {
        self.require_streaming()?;
        let wants_message_updates = is_at_least_1_1(&requested_protocol_version(params));
        let first = uplink
            .next()
            .await
            .ok_or_else(|| A2AError::invalid_request("a live stream needs a first request"))?;

        let (task_id, opening, inner) = match first.request.payload {
            StreamRequestPayload::SendMessage(request) => {
                self.authorize(params, request_task_id(&request).as_deref())?;
                match self
                    .live_deliver(params, request)
                    .await
                    .map_err(Rejection::into_error)?
                {
                    Delivery::Started { task, receiver, .. } => {
                        let events =
                            subscription_stream(receiver, None, 0, false, wants_message_updates);
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
                        );
                        (task.id.clone(), Some(task), Some(events))
                    }
                }
            }
            StreamRequestPayload::Subscribe(subscribe) => {
                self.authorize(params, Some(&subscribe.id))?;
                let task_id = subscribe.id.clone();
                match self.subscribe_to_task(params, subscribe).await {
                    Ok(events) => (task_id, None, Some(events)),
                    // An idle task that is not finished is still there to be
                    // observed: the exchange opens with it and waits.
                    Err(error) if error.code == error_code::TASK_NOT_FOUND => {
                        match self.task_store.get(&task_id).await? {
                            Some(task) if !task.status.state.is_terminal() => {
                                (task_id, Some(task), None)
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
        tokio::spawn(session.run(opening, inner, uplink, out));
        Ok(ReceiverStream::new(downlink).boxed())
    }

    /// Hands a message to a task: into the execution that is running, or as the
    /// start of a new turn.
    async fn live_deliver(
        &self,
        params: &ServiceParams,
        request: SendMessageRequest,
    ) -> Result<Delivery, Rejection> {
        self.validate_artifact_updates(&request)?;
        if !is_artifact_only(&request) {
            self.validate_content_types(&request.message)?;
        }
        let task_id = request_task_id(&request);
        if let Some(task_id) = &task_id {
            if let Some(delivery) = self.try_inject(&request, task_id).await? {
                return Ok(delivery);
            }
        }
        match self.start_execution(params, request.clone(), false).await {
            Ok((task, receiver)) => {
                let message_generation = last_message_generation(&task);
                Ok(Delivery::Started {
                    task,
                    receiver,
                    message_generation,
                })
            }
            // Another request started the task between the look and the start:
            // the message belongs to that execution now.
            Err(error) if is_in_progress(&error) => match &task_id {
                Some(task_id) => match self.try_inject(&request, task_id).await? {
                    Some(delivery) => Ok(delivery),
                    None => Err(Rejection::Failed(error)),
                },
                None => Err(Rejection::Failed(error)),
            },
            Err(error) => Err(Rejection::Failed(error)),
        }
    }

    /// Appends the message to the task under the writer lock and passes it to
    /// the running executor. `None` when no execution is running.
    async fn try_inject(
        &self,
        request: &SendMessageRequest,
        task_id: &str,
    ) -> Result<Option<Delivery>, Rejection> {
        let Some(active) = self.execution_manager.get(task_id).await else {
            return Ok(None);
        };
        let writer = active.writer.lock().await;
        // The execution may have ended, or the request preparing it failed,
        // while this waited for the lock.
        let registered = self
            .execution_manager
            .get(task_id)
            .await
            .is_some_and(|current| Arc::ptr_eq(&current, &active));
        let base = match (registered, active.snapshot().await) {
            (true, Some(base)) => base,
            _ => return Ok(None),
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
        // Room for the message is secured before anything is changed, so a
        // message the executor cannot take is declined with nothing recorded.
        let permit = if artifact_only {
            None
        } else {
            let inputs = active.inputs.get().ok_or_else(|| {
                Rejection::Declined("the agent does not take input while it works".to_string())
            })?;
            Some(inputs.try_reserve().map_err(|error| match error {
                mpsc::error::TrySendError::Full(()) => {
                    Rejection::Declined("the agent is busy with earlier input".to_string())
                }
                mpsc::error::TrySendError::Closed(()) => {
                    Rejection::Declined("the agent is no longer taking input".to_string())
                }
            })?)
        };

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
        drop(writer);
        self.task_changed.notify_waiters();

        for event in &events {
            if let Err(error) = send_push_notifications(
                task_id,
                self.push_config_store.as_deref(),
                self.push_sender.as_deref(),
                event,
            )
            .await
            {
                tracing::warn!(task_id, %error, "could not push a live input");
            }
        }
        Ok(Some(Delivery::Injected {
            task,
            receiver,
            sequence_before,
            sequence_after,
        }))
    }
}

/// One running exchange: the loop that owns the downlink.
struct LiveRun {
    handler: DefaultRequestHandler,
    params: ServiceParams,
    /// The task the exchange is bound to, for its whole life.
    task_id: TaskId,
    wants_message_updates: bool,
    registration: Option<mpsc::Sender<LiveItem>>,
}

impl LiveRun {
    async fn run(
        self,
        opening: Option<Task>,
        mut inner: Option<BoxStream<'static, Result<StreamResponse, A2AError>>>,
        mut uplink: BoxStream<'static, LiveItem>,
        out: mpsc::Sender<Result<StreamResponse, A2AError>>,
    ) {
        if let Some(task) = opening {
            if out.send(Ok(StreamResponse::Task(task))).await.is_err() {
                return self.finish();
            }
        }
        let mut uplink_open = true;
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
                item = uplink.next(), if uplink_open => match item {
                    Some(item) => {
                        if !self.handle(item, &mut inner, &out).await {
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
        self.finish();
    }

    fn finish(&self) {
        if let Some(registration) = &self.registration {
            self.handler.live.unregister(&self.task_id, registration);
        }
    }

    /// Carries out one request that arrived after the exchange opened. Returns
    /// `false` when the downlink is gone.
    async fn handle(
        &self,
        item: LiveItem,
        inner: &mut Option<BoxStream<'static, Result<StreamResponse, A2AError>>>,
        out: &mpsc::Sender<Result<StreamResponse, A2AError>>,
    ) -> bool {
        let LiveItem { request, ack } = item;
        let mut reference_id = None;
        let result: Result<(), Rejection> = match request.payload {
            StreamRequestPayload::SendMessage(mut request) => {
                reference_id = Some(request.message.message_id.clone()).filter(|id| !id.is_empty());
                match self.bind(&mut request) {
                    Ok(()) => self.deliver(request, inner, out).await,
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
        if let Err(rejection) = result {
            let event = rejection.into_stream_error(&self.task_id, reference_id);
            if out.send(Ok(event)).await.is_err() {
                return false;
            }
        }
        if let Some(ack) = ack {
            if let Ok(task) = self.handler.load_task(&self.task_id).await {
                let _ = ack.send(task);
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
        request: SendMessageRequest,
        inner: &mut Option<BoxStream<'static, Result<StreamResponse, A2AError>>>,
        out: &mpsc::Sender<Result<StreamResponse, A2AError>>,
    ) -> Result<(), Rejection> {
        self.handler
            .authorize(&self.params, Some(&self.task_id))
            .map_err(Rejection::Failed)?;
        match self.handler.live_deliver(&self.params, request).await? {
            Delivery::Started {
                receiver,
                message_generation,
                task,
            } => {
                *inner = Some(subscription_stream(
                    receiver,
                    None,
                    0,
                    false,
                    self.wants_message_updates,
                ));
                // The message started a turn nobody was running: the stream
                // is told it was appended, as it is for one that joins a
                // running turn.
                if self.wants_message_updates && message_generation > 0 {
                    let message =
                        task.timeline.iter().flatten().rev().find_map(|entry| {
                            match &entry.content {
                                TimelineContent::Message(message) => Some(message.clone()),
                                _ => None,
                            }
                        });
                    if let Some(message) = message {
                        let _ = out
                            .send(Ok(StreamResponse::MessageUpdate(TaskMessageUpdateEvent {
                                task_id: task.id.clone(),
                                context_id: task.context_id.clone(),
                                message,
                                metadata: None,
                                generation: message_generation,
                            })))
                            .await;
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
                    ));
                }
            }
        }
        Ok(())
    }
}
