// Copyright AGNTCY Contributors (https://github.com/agntcy)
// Copyright A2A Contributors (https://github.com/a2aproject)
// SPDX-License-Identifier: Apache-2.0
use a2a::*;
use futures::stream::BoxStream;
use serde_json::Value;
use std::collections::HashMap;

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, PoisonError};

use crate::middleware::{ServiceParams, User};

/// A client message that reached a running execution: sent on a live stream
/// (`SendLiveMessage`) or with `SendMessage` or `SendStreamingMessage` to a
/// task that is working.
///
/// By the time an executor reads it the server has already appended it to the
/// task's timeline and announced it to the subscribers; the executor decides
/// what the message means for its work.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ClientInput {
    /// The message the client sent.
    pub message: Message,
    /// The task generation at which the message was appended to the timeline.
    pub generation: i64,
}

impl ClientInput {
    pub fn new(message: Message, generation: i64) -> Self {
        ClientInput {
            message,
            generation,
        }
    }
}

/// The client messages that arrive while an execution runs, on a live stream
/// or sent to the working task (see [`ExecutorContext::inputs`]).
///
/// The server owns the queue behind it: a message that is still unread when
/// the execution ends is not lost, the server answers its sender with a
/// `StreamError`. An executor reads with [`recv`](Self::recv), alongside its own
/// work, and should drain what is there before it finishes a turn.
pub struct ClientInputs {
    queue: Arc<InputQueue>,
}

impl ClientInputs {
    /// A queue for exercising an executor on its own: whatever the sender
    /// pushes, `recv` yields.
    pub fn channel(capacity: usize) -> (ClientInputSender, ClientInputs) {
        let queue = Arc::new(InputQueue::new(capacity));
        (
            ClientInputSender {
                queue: queue.clone(),
                closer: Arc::new(CloseOnDrop(queue.clone())),
            },
            ClientInputs { queue },
        )
    }

    pub(crate) fn from_queue(queue: Arc<InputQueue>) -> Self {
        ClientInputs { queue }
    }

    /// The next message, waiting for one if none is queued. `None` once the
    /// server has closed the queue (the execution is over) and nothing is left.
    pub async fn recv(&mut self) -> Option<ClientInput> {
        loop {
            let notified = self.queue.notify.notified();
            tokio::pin!(notified);
            // Registered before the queue is read, so a push in between wakes it.
            notified.as_mut().enable();
            {
                let mut state = self.queue.lock();
                if let Some(input) = state.items.pop_front() {
                    return Some(input);
                }
                if state.closed {
                    return None;
                }
            }
            notified.await;
        }
    }

    /// A message that is already queued, without waiting.
    pub fn try_recv(&mut self) -> Option<ClientInput> {
        self.queue.lock().items.pop_front()
    }
}

impl std::fmt::Debug for ClientInputs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientInputs").finish_non_exhaustive()
    }
}

/// The sending end of a [`ClientInputs`] made with [`ClientInputs::channel`].
/// When the last clone is dropped the queue is closed: what was sent is still
/// read, then `recv` returns `None`.
#[derive(Clone)]
pub struct ClientInputSender {
    queue: Arc<InputQueue>,
    closer: Arc<CloseOnDrop>,
}

/// Closes the queue when the last sender goes away.
struct CloseOnDrop(Arc<InputQueue>);

impl Drop for CloseOnDrop {
    fn drop(&mut self) {
        self.0.close();
    }
}

impl ClientInputSender {
    /// Queues `input`. `Err` gives it back when the queue is full or closed.
    pub fn try_send(&self, input: ClientInput) -> Result<(), Box<ClientInput>> {
        match self.queue.try_reserve() {
            Ok(permit) => {
                permit.send(input);
                Ok(())
            }
            Err(_) => Err(Box::new(input)),
        }
    }

    /// Ends the queue: what was sent is still read, then `recv` returns `None`.
    pub fn close(&self) {
        let _ = &self.closer;
        self.queue.close();
    }
}

/// Why a message could not be queued for the executor.
pub(crate) enum InputRefusal {
    Full,
    Closed,
}

pub(crate) struct InputQueue {
    state: Mutex<InputState>,
    notify: tokio::sync::Notify,
    capacity: usize,
}

struct InputState {
    items: VecDeque<ClientInput>,
    /// Places promised to permits that have not sent yet.
    reserved: usize,
    closed: bool,
}

impl InputQueue {
    pub(crate) fn new(capacity: usize) -> Self {
        InputQueue {
            state: Mutex::new(InputState {
                items: VecDeque::new(),
                reserved: 0,
                closed: false,
            }),
            notify: tokio::sync::Notify::new(),
            capacity,
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, InputState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Secures room for one message, so that recording it can be undone by
    /// simply not sending.
    pub(crate) fn try_reserve(self: &Arc<Self>) -> Result<InputPermit, InputRefusal> {
        let mut state = self.lock();
        if state.closed {
            return Err(InputRefusal::Closed);
        }
        if state.items.len() + state.reserved >= self.capacity {
            return Err(InputRefusal::Full);
        }
        state.reserved += 1;
        Ok(InputPermit {
            queue: self.clone(),
            sent: false,
        })
    }

    /// Refuses anything further; what is queued can still be read.
    pub(crate) fn close(&self) {
        self.lock().closed = true;
        self.notify.notify_waiters();
    }

    /// Closes the queue and returns what the executor never read.
    pub(crate) fn close_and_drain(&self) -> Vec<ClientInput> {
        let unread = {
            let mut state = self.lock();
            state.closed = true;
            state.items.drain(..).collect()
        };
        self.notify.notify_waiters();
        unread
    }
}

/// Room for one message in an [`InputQueue`].
pub(crate) struct InputPermit {
    queue: Arc<InputQueue>,
    sent: bool,
}

impl InputPermit {
    pub(crate) fn send(mut self, input: ClientInput) {
        {
            let mut state = self.queue.lock();
            state.reserved -= 1;
            state.items.push_back(input);
        }
        self.sent = true;
        self.queue.notify.notify_one();
    }
}

impl Drop for InputPermit {
    fn drop(&mut self) {
        if !self.sent {
            self.queue.lock().reserved -= 1;
        }
    }
}

/// Context passed to the [`AgentExecutor`] for each execution.
///
/// Non-exhaustive: fields are added as the protocol grows, and only the
/// handler builds one.
#[non_exhaustive]
pub struct ExecutorContext {
    /// The message that triggered execution. `None` for a cancellation, and for
    /// a request that carried only `artifactUpdates` for an existing task.
    pub message: Option<Message>,
    /// The task ID being executed.
    pub task_id: TaskId,
    /// The existing task, if this is a continuation. For a new task it is
    /// `None`, except on a blocking `SendMessage`, where it is the task as the
    /// request prepared it (not saved yet: the executor's first event creates it).
    pub stored_task: Option<Task>,
    /// Context ID grouping related interactions.
    pub context_id: String,
    /// Request metadata.
    pub metadata: Option<HashMap<String, Value>>,
    /// Authenticated user info.
    pub user: Option<User>,
    /// Service parameters (headers/metadata) from the request.
    pub service_params: ServiceParams,
    /// Optional tenant ID.
    pub tenant: Option<String>,
    /// Client messages that arrive while this execution runs, on a live stream
    /// or sent to the working task. Present only when [`AgentExecutor::accepts_live_input`] is true;
    /// the executor reads it alongside its own work, and it ends when the
    /// execution does.
    pub inputs: Option<ClientInputs>,
}

impl ExecutorContext {
    /// A context for `task_id` in `context_id` with nothing else set. The
    /// handler builds the real ones; this is for exercising an executor on its
    /// own, as the struct is non-exhaustive and cannot be built literally
    /// outside this crate.
    pub fn new(task_id: impl Into<TaskId>, context_id: impl Into<String>) -> Self {
        ExecutorContext {
            message: None,
            task_id: task_id.into(),
            stored_task: None,
            context_id: context_id.into(),
            metadata: None,
            user: None,
            service_params: ServiceParams::new(),
            tenant: None,
            inputs: None,
        }
    }

    pub fn with_inputs(mut self, inputs: ClientInputs) -> Self {
        self.inputs = Some(inputs);
        self
    }

    pub fn with_message(mut self, message: Message) -> Self {
        self.message = Some(message);
        self
    }

    pub fn with_stored_task(mut self, task: Task) -> Self {
        self.stored_task = Some(task);
        self
    }

    pub fn with_metadata(mut self, metadata: HashMap<String, Value>) -> Self {
        self.metadata = Some(metadata);
        self
    }

    pub fn with_user(mut self, user: User) -> Self {
        self.user = Some(user);
        self
    }

    pub fn with_service_params(mut self, service_params: ServiceParams) -> Self {
        self.service_params = service_params;
        self
    }

    pub fn with_tenant(mut self, tenant: impl Into<String>) -> Self {
        self.tenant = Some(tenant.into());
        self
    }

    /// A task in `state` for this execution, with this context's ids, for an
    /// [`Agent`] to create with [`Start::Task`].
    pub fn new_task(&self, state: TaskState) -> Task {
        Task {
            id: self.task_id.clone(),
            context_id: self.context_id.clone(),
            status: TaskStatus {
                state,
                message: None,
                timestamp: None,
            },
            artifacts: None,
            history: None,
            timeline: None,
            metadata: None,
            generation: 0,
        }
    }

    pub fn task_info(&self) -> (TaskId, String) {
        (self.task_id.clone(), self.context_id.clone())
    }
}

/// The user's business logic entry point.
///
/// Implement this trait to handle incoming messages and cancellation requests.
/// The executor returns a stream of events (status updates, artifact updates,
/// messages, or task objects).
///
/// For a new task its first event decides whether a task exists (A2A §3.1.1):
/// a task, status or artifact event creates it, and the server saves it then;
/// a [`Message`] is a direct reply, nothing is saved, and whatever the
/// executor emits afterwards is ignored (it may keep working until it
/// returns). An executor that fails or panics before it has said anything
/// leaves no task; one that ends without an event leaves the task as the
/// request prepared it. Opening a live exchange is the exception: that task
/// is created up front. See [`Agent`] for the same contract as a typed return
/// value.
#[async_trait::async_trait]
pub trait AgentExecutor: Send + Sync + 'static {
    /// Execute a message. Returns a stream of events.
    fn execute(&self, ctx: ExecutorContext)
    -> BoxStream<'static, Result<StreamResponse, A2AError>>;

    /// Cancel an in-progress task. Returns a stream of events.
    fn cancel(&self, ctx: ExecutorContext) -> BoxStream<'static, Result<StreamResponse, A2AError>>;

    /// Whether this executor reads [`ExecutorContext::inputs`]: client messages
    /// sent while it is working, on a live stream or with `SendMessage` or
    /// `SendStreamingMessage`. Off by default, in which case such a message is
    /// declined while the execution runs (a `StreamError` on a live stream, a
    /// busy error on the other calls), which the protocol allows; it is
    /// accepted as a new turn once the task is idle again.
    ///
    /// A blocking `SendMessage` that joins a running turn waits for it to end,
    /// or to reach an interrupted state; an executor that keeps a turn open
    /// without ever interrupting it holds such a call open.
    fn accepts_live_input(&self) -> bool {
        false
    }
}

// A value built once per request and consumed at once: boxing the variants
// would only make it harder to write.
#[allow(clippy::large_enum_variant)]
/// How an [`Agent`] answers a message: with a direct reply, or by creating a
/// task (A2A §3.1.1).
pub enum Start {
    /// A direct reply. No task exists, and nothing is saved: neither the
    /// client's message nor its artifacts. The call ends with the reply; an
    /// agent that has more to do spawns it.
    Message(Message),
    /// The agent creates the task: `Task` is its first state (its `id` and
    /// `context_id` are the ones in the [`ExecutorContext`]), and the
    /// [`TaskRun`] is the work that follows.
    Task(Task, TaskRun),
}

/// The work of a task an [`Agent`] created, after the task exists.
pub struct TaskRun {
    run: Box<dyn FnOnce(Option<ClientInputs>) -> EventStream + Send>,
}

type EventStream = BoxStream<'static, Result<StreamResponse, A2AError>>;

impl TaskRun {
    /// The work, given the client's input while it runs (`None` unless
    /// [`Agent::accepts_live_input`]). It returns the events that follow the
    /// task: status and artifact updates, until the task is terminal or waits
    /// for input. A message the client sends while it runs arrives on the
    /// input.
    pub fn new<F, S>(run: F) -> Self
    where
        F: FnOnce(Option<ClientInputs>) -> S + Send + 'static,
        S: futures::Stream<Item = Result<StreamResponse, A2AError>> + Send + 'static,
    {
        TaskRun {
            run: Box::new(move |inputs| Box::pin(run(inputs))),
        }
    }

    /// No events after the task is created: it stays as it was created.
    pub fn empty() -> Self {
        TaskRun::new(|_| futures::stream::empty())
    }

    fn run(
        self,
        inputs: Option<ClientInputs>,
    ) -> BoxStream<'static, Result<StreamResponse, A2AError>> {
        (self.run)(inputs)
    }
}

/// An agent that decides, for each message, whether a task exists.
///
/// The server creates and saves a task only when [`Agent::start`] returns
/// [`Start::Task`], and queues the client's messages for it only from then on.
/// (A live exchange is bound to its task from its first request, so that one
/// is created up front, and a reply to it does not end the exchange.)
/// [`AgentExecutor`] is the same contract as a stream of events, whose first
/// event decides; it runs on the server unchanged.
#[async_trait::async_trait]
pub trait Agent: Send + Sync + 'static {
    /// Starts on `ctx.message`: a direct reply, or the task and its work.
    async fn start(&self, ctx: ExecutorContext) -> Result<Start, A2AError>;

    /// Cancels an in-progress task. Returns a stream of events.
    fn cancel(&self, ctx: ExecutorContext) -> BoxStream<'static, Result<StreamResponse, A2AError>>;

    /// Whether the [`TaskRun`] reads the client's messages while it works; see
    /// [`AgentExecutor::accepts_live_input`].
    fn accepts_live_input(&self) -> bool {
        false
    }
}

/// An [`Agent`] as the executor the server drives: the first event is the
/// task, or the direct reply.
pub(crate) struct AgentExecutorAdapter<A: Agent>(pub(crate) Arc<A>);

impl<A: Agent> AgentExecutor for AgentExecutorAdapter<A> {
    fn execute(
        &self,
        mut ctx: ExecutorContext,
    ) -> BoxStream<'static, Result<StreamResponse, A2AError>> {
        use futures::StreamExt;

        let agent = Arc::clone(&self.0);
        let (task_id, context_id) = ctx.task_info();
        Box::pin(
            futures::stream::once(async move {
                let inputs = ctx.inputs.take();
                match agent.start(ctx).await {
                    Err(error) => futures::stream::once(async move { Err(error) }).boxed(),
                    Ok(Start::Message(message)) => {
                        futures::stream::once(async move { Ok(StreamResponse::Message(message)) })
                            .boxed()
                    }
                    // The task is the one this execution is for.
                    Ok(Start::Task(task, _))
                        if task.id != task_id || task.context_id != context_id =>
                    {
                        futures::stream::once(
                            async move { Err(A2AError::invalid_agent_response()) },
                        )
                        .boxed()
                    }
                    Ok(Start::Task(task, run)) => {
                        futures::stream::once(async move { Ok(StreamResponse::Task(task)) })
                            // The work starts once the task has been created.
                            .chain(futures::stream::once(async move { run.run(inputs) }).flatten())
                            .boxed()
                    }
                }
            })
            .flatten(),
        )
    }

    fn cancel(&self, ctx: ExecutorContext) -> BoxStream<'static, Result<StreamResponse, A2AError>> {
        self.0.cancel(ctx)
    }

    fn accepts_live_input(&self) -> bool {
        self.0.accepts_live_input()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_executor_context_task_info() {
        let ctx = ExecutorContext {
            message: None,
            task_id: "t1".into(),
            stored_task: None,
            context_id: "c1".into(),
            metadata: None,
            user: None,
            service_params: HashMap::new(),
            tenant: None,
            inputs: None,
        };
        let (tid, cid) = ctx.task_info();
        assert_eq!(tid, "t1");
        assert_eq!(cid, "c1");
    }
}
