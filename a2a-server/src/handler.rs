// Copyright AGNTCY Contributors (https://github.com/agntcy)
// Copyright A2A Contributors (https://github.com/a2aproject)
// SPDX-License-Identifier: Apache-2.0
use a2a::*;
use async_trait::async_trait;
use futures::{StreamExt, stream::BoxStream};
use std::time::Duration;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{Notify, RwLock, broadcast, mpsc, oneshot};

use crate::executor::{ClientInputs, InputQueue};
use crate::middleware::ServiceParams;
use crate::pagination::resolve_page_size;
use crate::task_store::{apply_history_length, apply_timeline_length};

mod live;

pub use live::LiveResponse;
use live::{LiveItem, LiveSessions};

const EXECUTION_BUFFER_CAPACITY: usize = 32;

/// How many requests may queue on a live exchange's uplink, and how many events
/// on its downlink before the session waits for the client to read.
const LIVE_UPLINK_CAPACITY: usize = 32;
const LIVE_DOWNLINK_CAPACITY: usize = 32;

/// How many client messages may wait for an executor that reads its live input
/// slowly; one more is declined as "not accepting input right now".
const LIVE_INPUT_CAPACITY: usize = 16;

/// How long a long-poll `GetTask` (`currentGeneration`) is held open when the
/// task does not advance. The spec lets a server return earlier with the
/// current state.
const DEFAULT_LONG_POLL_TIMEOUT: Duration = Duration::from_secs(30);

/// Upper bound on the wait between two checks of the store during a
/// long-poll, so a task changed by another process is still noticed.
const LONG_POLL_RECHECK: Duration = Duration::from_secs(1);

#[derive(Clone)]
struct ExecutionEvent {
    sequence: u64,
    result: Result<StreamResponse, A2AError>,
    kind: EventKind,
}

/// How an event is meant to be delivered.
#[derive(Clone, Copy, PartialEq, Eq)]
enum EventKind {
    Normal,
    /// A `Task` snapshot that changed nothing. It sits at a generation that was
    /// already announced, so streams meant for clients leave it out; the
    /// blocking send, which only needs to know the executor spoke, keeps it.
    Repeated,
    /// Something only a live exchange is told (a `StreamError` about a message
    /// of its own), not the other subscribers of the task.
    LiveOnly,
    /// The executor's direct reply, with no task: the last thing any stream
    /// following the execution is sent.
    Reply,
}

#[derive(Default)]
struct ActiveExecutionState {
    sequence: u64,
    snapshot_task: Option<Task>,
}

struct ActiveExecution {
    sender: broadcast::Sender<ExecutionEvent>,
    state: RwLock<ActiveExecutionState>,
    /// Serialises every mutation of the task while it executes. The snapshot
    /// in `state` is the one authoritative copy: a writer takes this lock,
    /// reads the snapshot, applies its change, saves it and publishes the
    /// result, so executor events, a cancel and a follow-up cannot each work
    /// from a stale copy of their own.
    ///
    /// It is the task's own writer (see [`ExecutionManager::writer_for`]), not
    /// one per execution: a cancel or a follow-up that arrives while no
    /// execution is running serialises on the same lock.
    writer: Arc<tokio::sync::Mutex<()>>,
    /// Where live client messages go while the executor runs; set once, when
    /// the executor accepts live input.
    inputs: std::sync::OnceLock<Arc<InputQueue>>,
    /// Set while the task this execution is for has not been created: a new
    /// task is created by the executor's first event, and does not exist if
    /// the executor answers with a message alone.
    uncreated: std::sync::atomic::AtomicBool,
    /// The push configuration the request carried, kept until the task exists.
    pending_push: std::sync::Mutex<Option<TaskPushNotificationConfig>>,
}

impl ActiveExecution {
    #[cfg(test)]
    fn new(task: Option<Task>) -> Arc<Self> {
        Self::with_writer(task, Arc::new(tokio::sync::Mutex::new(())))
    }

    fn with_writer(task: Option<Task>, writer: Arc<tokio::sync::Mutex<()>>) -> Arc<Self> {
        let (sender, _) = broadcast::channel(EXECUTION_BUFFER_CAPACITY);
        Arc::new(Self {
            sender,
            state: RwLock::new(ActiveExecutionState {
                sequence: 0,
                snapshot_task: task,
            }),
            writer,
            inputs: std::sync::OnceLock::new(),
            uncreated: std::sync::atomic::AtomicBool::new(false),
            pending_push: std::sync::Mutex::new(None),
        })
    }

    async fn snapshot(&self) -> Option<Task> {
        self.state.read().await.snapshot_task.clone()
    }

    async fn set_snapshot(&self, task: Task) {
        self.state.write().await.snapshot_task = Some(task);
    }

    fn subscribe(&self) -> broadcast::Receiver<ExecutionEvent> {
        self.sender.subscribe()
    }

    async fn resubscribe(&self) -> (broadcast::Receiver<ExecutionEvent>, Option<Task>, u64) {
        let receiver = self.sender.subscribe();
        let state = self.state.read().await;
        (receiver, state.snapshot_task.clone(), state.sequence)
    }

    async fn publish(
        &self,
        result: Result<&StreamResponse, &A2AError>,
        snapshot_task: Option<Task>,
    ) {
        self.publish_kind(result, snapshot_task, EventKind::Normal)
            .await;
    }

    async fn publish_marked(
        &self,
        result: Result<&StreamResponse, &A2AError>,
        snapshot_task: Option<Task>,
        repeated: bool,
    ) {
        let kind = if repeated {
            EventKind::Repeated
        } else {
            EventKind::Normal
        };
        self.publish_kind(result, snapshot_task, kind).await;
    }

    async fn publish_kind(
        &self,
        result: Result<&StreamResponse, &A2AError>,
        snapshot_task: Option<Task>,
        kind: EventKind,
    ) {
        let sequence = {
            let mut state = self.state.write().await;
            if let Some(task) = snapshot_task {
                state.snapshot_task = Some(task);
            }
            state.sequence += 1;
            state.sequence
        };

        // Task state remains available for a later subscription, but transient
        // events do not need to be allocated when no subscriber is listening.
        if self.sender.receiver_count() == 0 {
            return;
        }

        let event = ExecutionEvent {
            sequence,
            result: result.cloned().map_err(Clone::clone),
            kind,
        };
        let _ = self.sender.send(event);
    }

    async fn snapshot_is_terminal(&self) -> bool {
        let state = self.state.read().await;
        state
            .snapshot_task
            .as_ref()
            .is_some_and(|task| task.status.state.is_terminal())
    }
}

/// How many deliveries wait for a task's webhook before the oldest news is
/// dropped: a receiver that is down must not make the server queue for ever,
/// and a client that sees a gap in the generations reads the task instead.
const PUSH_QUEUE_CAPACITY: usize = 1024;
/// How long a task's delivery worker waits for more before it retires.
const PUSH_WORKER_IDLE: Duration = Duration::from_secs(60);

/// One webhook delivery.
struct PushJob {
    event: StreamResponse,
    store: Arc<dyn crate::PushConfigStore>,
    sender: Arc<crate::HttpPushSender>,
    /// The execution whose event this is, or 0: a delivery that fails fails
    /// that execution (`fail_on_error`).
    execution: usize,
}

struct PushQueue {
    id: u64,
    tx: mpsc::Sender<PushJob>,
}

#[derive(Default)]
struct PushQueuesInner {
    queues: std::sync::Mutex<HashMap<TaskId, PushQueue>>,
    failures: std::sync::Mutex<HashMap<TaskId, (usize, A2AError)>>,
    next_id: std::sync::atomic::AtomicU64,
}

/// A queue and a worker per task that has webhooks to call. A task's
/// deliveries go out one at a time, in the order they were queued, so what an
/// execution publishes under its writer reaches the receiver in that order,
/// and nothing that publishes waits for a receiver: a slow or re-entrant one
/// only delays its own task's deliveries.
#[derive(Default, Clone)]
struct PushQueues(Arc<PushQueuesInner>);

impl PushQueues {
    /// Queues `event` for the task's webhooks. Never waits.
    fn push(
        &self,
        task_id: &str,
        execution: usize,
        store: &Option<Arc<dyn crate::PushConfigStore>>,
        sender: &Option<Arc<crate::HttpPushSender>>,
        event: &StreamResponse,
    ) {
        let (Some(store), Some(sender)) = (store, sender) else {
            return;
        };
        let mut job = PushJob {
            event: event.clone(),
            store: Arc::clone(store),
            sender: Arc::clone(sender),
            execution,
        };
        // A worker that is retiring has closed its queue: the next one is new.
        for _ in 0..2 {
            let tx = self.queue_for(task_id);
            match tx.try_send(job) {
                Ok(()) => return,
                Err(mpsc::error::TrySendError::Full(_)) => {
                    tracing::warn!(task_id, "webhook queue full; dropping a notification");
                    return;
                }
                Err(mpsc::error::TrySendError::Closed(returned)) => job = returned,
            }
        }
    }

    fn queue_for(&self, task_id: &str) -> mpsc::Sender<PushJob> {
        let mut queues = self
            .0
            .queues
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(queue) = queues.get(task_id).filter(|queue| !queue.tx.is_closed()) {
            return queue.tx.clone();
        }
        let (tx, rx) = mpsc::channel(PUSH_QUEUE_CAPACITY);
        let id = self
            .0
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        queues.insert(task_id.to_string(), PushQueue { id, tx: tx.clone() });
        tokio::spawn(push_worker(
            Arc::clone(&self.0),
            task_id.to_string(),
            id,
            rx,
        ));
        tx
    }

    /// The failure of a delivery for `execution`, if one failed.
    fn take_failure(&self, task_id: &str, execution: usize) -> Option<A2AError> {
        let mut failures = self
            .0
            .failures
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match failures.get(task_id) {
            Some((failed, _)) if *failed == execution => {
                failures.remove(task_id).map(|(_, error)| error)
            }
            _ => None,
        }
    }
}

async fn push_worker(
    inner: Arc<PushQueuesInner>,
    task_id: TaskId,
    id: u64,
    mut rx: mpsc::Receiver<PushJob>,
) {
    loop {
        let job = match tokio::time::timeout(PUSH_WORKER_IDLE, rx.recv()).await {
            Ok(Some(job)) => job,
            Ok(None) => break,
            // Idle: retire. Close first, so a delivery queued meanwhile is
            // still taken and a later one starts a new worker.
            Err(_) => {
                rx.close();
                {
                    let mut queues = inner
                        .queues
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    if queues.get(&task_id).is_some_and(|queue| queue.id == id) {
                        queues.remove(&task_id);
                    }
                }
                match rx.recv().await {
                    Some(job) => job,
                    None => break,
                }
            }
        };
        if let Err(error) = deliver_push_notifications(
            &task_id,
            job.store.as_ref(),
            job.sender.as_ref(),
            &job.event,
        )
        .await
        {
            tracing::warn!(task_id = %task_id, %error, "could not push a notification");
            if job.execution != 0 {
                inner
                    .failures
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(task_id.clone(), (job.execution, error));
            }
        }
    }
}

#[derive(Default)]
struct ExecutionManager {
    executions: RwLock<HashMap<TaskId, Arc<ActiveExecution>>>,
    /// One writer lock per task, shared by whoever is mutating it. Held weakly
    /// so a task nobody is touching costs nothing.
    writers: std::sync::Mutex<TaskWriters>,
    /// The webhook deliveries of each task, in the order its events were
    /// published.
    pushes: PushQueues,
}

#[derive(Default)]
struct TaskWriters {
    locks: HashMap<TaskId, std::sync::Weak<tokio::sync::Mutex<()>>>,
    calls: usize,
}

impl ExecutionManager {
    /// The task's writer lock. Every mutation of a task takes it: executor
    /// events, a cancel, a follow-up being prepared. Two callers asking for the
    /// same task while either still holds it get the same lock.
    fn writer_for(&self, task_id: &str) -> Arc<tokio::sync::Mutex<()>> {
        Self::lock_for(&self.writers, task_id)
    }

    fn lock_for(
        locks: &std::sync::Mutex<TaskWriters>,
        task_id: &str,
    ) -> Arc<tokio::sync::Mutex<()>> {
        let mut writers = locks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        writers.calls += 1;
        if writers.calls % 256 == 0 {
            writers.locks.retain(|_, lock| lock.strong_count() > 0);
        }
        if let Some(lock) = writers
            .locks
            .get(task_id)
            .and_then(std::sync::Weak::upgrade)
        {
            return lock;
        }
        let lock = Arc::new(tokio::sync::Mutex::new(()));
        writers
            .locks
            .insert(task_id.to_string(), Arc::downgrade(&lock));
        lock
    }
}

impl ExecutionManager {
    /// Claims the task for one execution before anything reads it, so two
    /// requests for the same task cannot both prepare from the same stored
    /// state. The snapshot is filled in once the request has been applied.
    ///
    /// The task's writer is taken first and handed back with the reservation:
    /// anyone who finds the execution registered (a subscriber, a cancel) then
    /// waits on a lock that is already held, instead of slipping in between
    /// the registration and the first write.
    async fn reserve(
        &self,
        task_id: &str,
    ) -> Result<(Arc<ActiveExecution>, tokio::sync::OwnedMutexGuard<()>), A2AError> {
        let writer = self.writer_for(task_id);
        let guard = Arc::clone(&writer).lock_owned().await;
        self.reserve_held(task_id, guard).await
    }

    /// [`Self::reserve`] for a caller that already holds the task's writer.
    async fn reserve_held(
        &self,
        task_id: &str,
        guard: tokio::sync::OwnedMutexGuard<()>,
    ) -> Result<(Arc<ActiveExecution>, tokio::sync::OwnedMutexGuard<()>), A2AError> {
        let writer = self.writer_for(task_id);
        let mut executions = self.executions.write().await;
        if let Some(existing) = executions.get(task_id) {
            // A live reservation holds the writer until its snapshot is set,
            // and this caller holds it now: an entry that still has no
            // snapshot belongs to a request that was dropped, whose release is
            // still on its way. It is replaced, not mistaken for a running
            // execution.
            if existing.snapshot().await.is_some() {
                return Err(A2AError::invalid_request(format!(
                    "task execution is already in progress: {task_id}"
                )));
            }
        }

        let active = ActiveExecution::with_writer(None, writer);
        executions.insert(task_id.to_string(), active.clone());
        Ok((active, guard))
    }

    async fn get(&self, task_id: &str) -> Option<Arc<ActiveExecution>> {
        let executions = self.executions.read().await;
        executions.get(task_id).cloned()
    }

    async fn resubscribe(
        &self,
        task_id: &str,
    ) -> Option<(broadcast::Receiver<ExecutionEvent>, Option<Task>, u64)> {
        let active = self.get(task_id).await?;
        let (receiver, snapshot_task, sequence) = active.resubscribe().await;
        Some((receiver, snapshot_task, sequence))
    }

    /// [`Self::finish`] without waiting: `false` when the registry is busy.
    fn try_finish(&self, task_id: &str, active: &Arc<ActiveExecution>) -> bool {
        let Ok(mut executions) = self.executions.try_write() else {
            return false;
        };
        if executions
            .get(task_id)
            .is_some_and(|current| Arc::ptr_eq(current, active))
        {
            executions.remove(task_id);
        }
        true
    }

    async fn finish(&self, task_id: &str, active: &Arc<ActiveExecution>) {
        let mut executions = self.executions.write().await;
        let should_remove = executions
            .get(task_id)
            .is_some_and(|current| Arc::ptr_eq(current, active));
        if should_remove {
            executions.remove(task_id);
        }
    }
}

/// Releases a task's execution reservation when dropped, unless disarmed.
///
/// The reservation is taken before several awaits, so a request future that is
/// dropped part-way (a client that disconnects) or an executor that panics
/// would otherwise leave the task "already in progress" for good.
struct ReservationGuard {
    manager: Arc<ExecutionManager>,
    task_id: TaskId,
    active: Arc<ActiveExecution>,
    armed: bool,
}

impl ReservationGuard {
    fn new(manager: Arc<ExecutionManager>, task_id: TaskId, active: Arc<ActiveExecution>) -> Self {
        Self {
            manager,
            task_id,
            active,
            armed: true,
        }
    }

    /// Hands responsibility for the reservation to the spawned execution.
    fn disarm(mut self) {
        self.armed = false;
    }

    /// Gives the task back now, so the next request already finds it free.
    /// Dropping the guard does the same, but only after the fact.
    async fn release(mut self) {
        self.armed = false;
        self.manager.finish(&self.task_id, &self.active).await;
    }
}

impl Drop for ReservationGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        // Released now if the registry is free, so a request already waiting
        // for the writer this future still holds finds the task released, not
        // a stale registration. Only a registry that is busy at this instant
        // falls back to releasing from a spawned task.
        if self.manager.try_finish(&self.task_id, &self.active) {
            return;
        }
        // `finish` is async; without a runtime to run it on (the process is
        // shutting down) there is nothing left to release.
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let manager = Arc::clone(&self.manager);
            let task_id = self.task_id.clone();
            let active = Arc::clone(&self.active);
            runtime.spawn(async move { manager.finish(&task_id, &active).await });
        }
    }
}

#[derive(Clone)]
struct ExecutionRuntime {
    executor: Arc<dyn crate::AgentExecutor>,
    task_store: Arc<dyn crate::TaskStore>,
    push_config_store: Option<Arc<dyn crate::PushConfigStore>>,
    push_sender: Option<Arc<crate::HttpPushSender>>,
    execution_manager: Arc<ExecutionManager>,
    task_changed: Arc<Notify>,
}

struct SubscriptionState {
    receiver: broadcast::Receiver<ExecutionEvent>,
    snapshot_task: Option<Task>,
    min_sequence: u64,
    done: bool,
}

/// The protocol version a request announced through `A2A-Version`, or "1.0"
/// when it sent none.
fn requested_protocol_version(params: &ServiceParams) -> String {
    params
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(SVC_PARAM_VERSION))
        .and_then(|(_, values)| values.first())
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .unwrap_or("1.0")
        .to_string()
}

/// Whether `version` (an `A2A-Version` value) is 1.1 or later within 1.x.
fn is_at_least_1_1(version: &str) -> bool {
    let mut parts = version.trim().split('.');
    let major = parts.next().and_then(|part| part.parse::<u32>().ok());
    let minor = parts
        .next()
        .map_or(Some(0), |part| part.parse::<u32>().ok());
    matches!((major, minor), (Some(1), Some(minor)) if minor >= 1)
}

/// Applies the response-trimming parameters shared by every operation that
/// returns a task: the deprecated `historyLength` and its replacement
/// `timelineLength`.
fn apply_lengths(task: &mut Task, history_length: Option<i32>, timeline_length: Option<i32>) {
    apply_history_length(task, history_length);
    apply_timeline_length(task, timeline_length);
}

fn is_terminal_event(event: &StreamResponse) -> bool {
    match event {
        StreamResponse::Task(task) => task.status.state.is_terminal(),
        StreamResponse::StatusUpdate(update) => update.status.state.is_terminal(),
        _ => false,
    }
}

fn task_id_for_event(event: &StreamResponse) -> Option<&str> {
    match event {
        StreamResponse::Task(task) => Some(&task.id),
        StreamResponse::StatusUpdate(update) => Some(&update.task_id),
        StreamResponse::ArtifactUpdate(update) => Some(&update.task_id),
        StreamResponse::MessageUpdate(update) => Some(&update.task_id),
        StreamResponse::Message(_) | StreamResponse::Error(_) => None,
        _ => None,
    }
}

fn should_interrupt_non_streaming(
    req: &SendMessageRequest,
    event: &StreamResponse,
) -> Option<TaskId> {
    if req
        .configuration
        .as_ref()
        .and_then(|config| config.return_immediately)
        .unwrap_or(false)
    {
        return match event {
            StreamResponse::Message(_) => None,
            _ => task_id_for_event(event).map(ToOwned::to_owned),
        };
    }

    // An interrupted state ends a blocking call (A2A §3.1.1): the turn may
    // stay open to take more input, so the stream ending is not what to wait for.
    let interrupted =
        |state: &TaskState| matches!(state, TaskState::AuthRequired | TaskState::InputRequired);
    match event {
        StreamResponse::Task(task) if interrupted(&task.status.state) => Some(task.id.clone()),
        StreamResponse::StatusUpdate(update) if interrupted(&update.status.state) => {
            Some(update.task_id.clone())
        }
        _ => None,
    }
}

/// The error for a request on a unary or streaming call that could not be
/// carried out. A message the running agent will not take is reported as the
/// task being busy, which is what these calls said before they could join a
/// running turn.
fn unary_refusal(rejection: live::Rejection, request: &SendMessageRequest) -> A2AError {
    match rejection {
        live::Rejection::Declined(reason) => A2AError::invalid_request(format!(
            "task execution is already in progress: {} ({reason})",
            request_task_id(request).unwrap_or_default()
        )),
        live::Rejection::Failed(error) => error,
    }
}

/// Calls the webhooks registered for the task with `event`.
async fn deliver_push_notifications(
    task_id: &str,
    push_config_store: &dyn crate::PushConfigStore,
    push_sender: &crate::HttpPushSender,
    event: &StreamResponse,
) -> Result<(), A2AError> {
    let configs = push_config_store.list(task_id).await?;
    for config in configs {
        // The same rule as for streams: an appended message is only sent to a
        // receiver registered under 1.1 or later (no version means 1.0).
        if matches!(event, StreamResponse::MessageUpdate(_))
            && !is_at_least_1_1(config.protocol_version.as_deref().unwrap_or("1.0"))
        {
            continue;
        }
        push_sender.send_push(&config, event.clone()).await?;
    }

    Ok(())
}

async fn save_task(task_store: &dyn crate::TaskStore, task: Task) -> Result<Task, A2AError> {
    save_task_with(task_store, task, false).await
}

/// Saves `task`: `creating` for the first save of a task, which is created
/// outright; otherwise it is updated, and created if the store does not know it.
async fn save_task_with(
    task_store: &dyn crate::TaskStore,
    mut task: Task,
    creating: bool,
) -> Result<Task, A2AError> {
    // DM-SERIAL-001: a persisted/returned task status must carry a timestamp.
    // This is the single persistence chokepoint for executor events, so
    // backfilling here covers the task-snapshot and status-update paths
    // alike, while preserving any timestamp the executor already supplied.
    if task.status.timestamp.is_none() {
        task.status.timestamp = Some(chrono::Utc::now());
    }
    if creating {
        task_store.create(task.clone()).await?;
        return Ok(task);
    }
    match task_store.update(task.clone()).await {
        Ok(_) => Ok(task),
        Err(error) if error.code == error_code::TASK_NOT_FOUND => {
            task_store.create(task.clone()).await?;
            Ok(task)
        }
        Err(error) => Err(error),
    }
}

/// DM-SERIAL-001: backfills a missing status timestamp on the event itself,
/// before it is both persisted (via `apply_event_to_task`/`save_task`) and
/// broadcast to any live subscriber -- `save_task`'s own backfill only
/// covers what gets persisted; a subscriber watching `send_streaming_message`
/// or `subscribe_to_task` receives the broadcast event directly, never a
/// reload from the store, so backfilling only at persistence time still
/// leaves a live subscriber seeing a timestamp-less status.
fn backfill_status_timestamp(event: &mut StreamResponse) {
    let status = match event {
        StreamResponse::Task(task) => &mut task.status,
        StreamResponse::StatusUpdate(update) => &mut update.status,
        _ => return,
    };
    if status.timestamp.is_none() {
        status.timestamp = Some(chrono::Utc::now());
    }
}

/// Records an agent status as the task's next timeline entry, advancing the
/// generation. Returns the generation after the mutation.
fn record_status(task: &mut Task, status: TaskStatus) -> i64 {
    task.generation += 1;
    task.timeline
        .get_or_insert_with(Vec::new)
        .push(TimelineEntry {
            content: TimelineContent::Status(status),
            generation: task.generation,
        });
    task.generation
}

/// Merges an artifact update into the task, advancing the generation. The
/// stored artifact is stamped with the generations at which it started and
/// completed; the update itself is left as sent apart from the generation it
/// is assigned. Returns the generation after the mutation.
fn merge_artifact(task: &mut Task, update: &mut TaskArtifactUpdateEvent) -> Result<i64, A2AError> {
    task.generation += 1;
    let generation = task.generation;
    let append = update.append.unwrap_or(false);
    // The wire cannot tell an unset `lastChunk` from `false`, so only an
    // explicit `true` completes an artifact here; one left open is closed when
    // the task reaches a terminal state.
    let complete = update.last_chunk == Some(true);

    let artifacts = task.artifacts.get_or_insert_with(Vec::new);
    let existing = artifacts
        .iter_mut()
        .find(|artifact| artifact.artifact_id == update.artifact.artifact_id);
    if append {
        let artifact = existing.ok_or_else(A2AError::invalid_agent_response)?;
        artifact.parts.extend(update.artifact.parts.iter().cloned());
        if let Some(metadata) = &update.artifact.metadata {
            artifact
                .metadata
                .get_or_insert_with(Default::default)
                .extend(metadata.clone());
        }
        if complete {
            artifact.end_generation = Some(generation);
        }
    } else {
        let mut replacement = update.artifact.clone();
        // Replacing an artifact keeps who wrote it.
        replacement.role = replacement
            .role
            .clone()
            .or_else(|| existing.as_ref().and_then(|artifact| artifact.role.clone()));
        replacement.start_generation = Some(
            existing
                .as_ref()
                .and_then(|artifact| artifact.start_generation)
                .unwrap_or(generation),
        );
        replacement.end_generation = complete.then_some(generation);
        match existing {
            Some(artifact) => *artifact = replacement,
            None => artifacts.push(replacement),
        }
    }
    Ok(generation)
}

/// A request that carries only `artifactUpdates` for an existing task. The
/// native `message` is not optional, so the wire's absent message arrives as
/// an empty one with no id (see the protobuf conversion); a message that has
/// an id, a body or a task of its own is never mistaken for it.
fn is_artifact_only(req: &SendMessageRequest) -> bool {
    !req.artifact_updates.is_empty()
        && req.message.message_id.is_empty()
        && req.message.parts.is_empty()
        && req.message.task_id.is_none()
        && req.message.context_id.is_none()
}

/// The task a send request names: the message's, or for an artifact-only
/// request the one its updates target.
fn request_task_id(req: &SendMessageRequest) -> Option<String> {
    if is_artifact_only(req) {
        req.artifact_updates
            .iter()
            .map(|update| update.task_id.as_str())
            .find(|task_id| !task_id.is_empty())
            .map(str::to_string)
    } else {
        req.message.task_id.clone()
    }
}

/// Applies the artifacts a client wrote in `artifactUpdates`, in list order,
/// advancing the generation for each. The client does not own `generation`:
/// whatever it sent is ignored and the artifact is attributed to the user.
///
/// When the message creates the task the client cannot know its ids, so they
/// are assigned; on an existing task they must match. A client may only touch
/// artifacts it wrote itself.
fn apply_client_artifacts(
    task: &mut Task,
    req: &SendMessageRequest,
    new_task: bool,
) -> Result<Vec<TaskArtifactUpdateEvent>, A2AError> {
    let mut applied = Vec::with_capacity(req.artifact_updates.len());
    for update in &req.artifact_updates {
        if update.artifact.artifact_id.is_empty() {
            return Err(A2AError::invalid_params(
                "an artifact update needs an artifactId",
            ));
        }
        let mut update = update.clone();
        if !new_task {
            if !update.task_id.is_empty() && update.task_id != task.id {
                return Err(A2AError::invalid_params(format!(
                    "artifact update targets task {}, not {}",
                    update.task_id, task.id
                )));
            }
            if !update.context_id.is_empty() && update.context_id != task.context_id {
                return Err(A2AError::invalid_params(format!(
                    "artifact update names context {}, but task {} belongs to {}",
                    update.context_id, task.id, task.context_id
                )));
            }
        }
        let existing_role = task
            .artifacts
            .iter()
            .flatten()
            .find(|artifact| artifact.artifact_id == update.artifact.artifact_id)
            .map(|artifact| artifact.role.clone());
        if existing_role.is_some_and(|role| role != Some(Role::User)) {
            return Err(A2AError::invalid_params(format!(
                "artifact {} was not written by the client and cannot be changed by it",
                update.artifact.artifact_id
            )));
        }
        update.artifact.role = Some(Role::User);
        update.task_id = task.id.clone();
        update.context_id = task.context_id.clone();
        update.generation = merge_artifact(task, &mut update).map_err(|_| {
            A2AError::invalid_params(format!(
                "cannot append to unknown artifact {}",
                update.artifact.artifact_id
            ))
        })?;
        applied.push(update);
    }
    Ok(applied)
}

/// Records a follow-up client message in the task's timeline, then applies
/// the artifacts sent with it (A2A 1.1 §3.4: the message first).
///
/// Returns the events that describe what was recorded, in order, for a live
/// stream to announce: the message as it was appended, then each artifact.
fn record_client_input(
    task: &mut Task,
    req: &SendMessageRequest,
) -> Result<Vec<StreamResponse>, A2AError> {
    let mut events = Vec::new();
    if !is_artifact_only(req) {
        task.generation += 1;
        task.timeline
            .get_or_insert_with(Vec::new)
            .push(TimelineEntry {
                content: TimelineContent::Message(req.message.clone()),
                generation: task.generation,
            });
        events.push(client_message_event(task, &req.message));
    }
    events.extend(
        apply_client_artifacts(task, req, false)?
            .into_iter()
            .map(StreamResponse::ArtifactUpdate),
    );
    Ok(events)
}

/// The event that announces a client message appended to `task` at its current
/// generation.
fn client_message_event(task: &Task, message: &Message) -> StreamResponse {
    StreamResponse::MessageUpdate(TaskMessageUpdateEvent {
        task_id: task.id.clone(),
        context_id: task.context_id.clone(),
        message: message.clone(),
        metadata: None,
        generation: task.generation,
    })
}

/// Where a cancellation stands while its executor's events are applied.
struct CancelEntry {
    /// The store gave the transition its generation and timeline entry.
    recorded_by_store: bool,
    /// The generation it gave.
    generation: i64,
    /// An event announcing the cancellation has been published.
    announced: bool,
}

impl CancelEntry {
    /// Whether `event` is the executor's own status for the cancellation, which
    /// belongs in the store's entry rather than in one of its own: the task has
    /// not moved on since (nothing else advanced the generation).
    fn can_amend(&self, base: &Task, event: &StreamResponse) -> bool {
        self.recorded_by_store
            && !self.announced
            && base.generation == self.generation
            && matches!(
                event,
                StreamResponse::StatusUpdate(update) if update.status.state == TaskState::Canceled
            )
    }

    /// Notes that a published event announced the canceled state.
    fn note_published(&mut self, published: bool, event: &StreamResponse) {
        let canceled = match event {
            StreamResponse::StatusUpdate(update) => update.status.state == TaskState::Canceled,
            StreamResponse::Task(task) => task.status.state == TaskState::Canceled,
            _ => false,
        };
        self.announced |= published && canceled;
    }
}

/// Whether the artifacts an executor supplied differ in content from the
/// stored ones. The generations the server stamps are not part of the content.
fn artifacts_differ(supplied: &[Artifact], stored: &[Artifact]) -> bool {
    supplied.len() != stored.len()
        || supplied.iter().any(|a| {
            stored
                .iter()
                .find(|b| b.artifact_id == a.artifact_id)
                .is_none_or(|b| {
                    a.name != b.name
                        || a.description != b.description
                        || a.parts != b.parts
                        || a.metadata != b.metadata
                        || a.extensions != b.extensions
                })
        })
}

/// A terminal task has no more output coming, so any artifact still open is
/// completed at the generation that ended it.
fn close_open_artifacts(task: &mut Task, generation: i64) {
    for artifact in task.artifacts.iter_mut().flatten() {
        artifact.end_generation.get_or_insert(generation);
    }
}

/// Rebases a snapshot an executor built onto the stored task. The generation,
/// timeline, history and artifact stamps belong to the server: an executor
/// that does not set `artifacts` or `history` leaves what is stored in place,
/// and a snapshot that changes nothing is not a mutation.
fn rebase_snapshot(task: &mut Task, previous: Task) {
    let status_changed = task.status.state != previous.status.state
        || task.status.message != previous.status.message;
    // The artifacts the client wrote are the server's to keep: an executor
    // lists what it produced, not what the client sent it.
    let supplied = task.artifacts.take().map(|mut new| {
        for kept in previous.artifacts.iter().flatten() {
            if kept.role == Some(Role::User)
                && !new.iter().any(|a| a.artifact_id == kept.artifact_id)
            {
                new.push(kept.clone());
            }
        }
        new
    });
    let artifacts_changed = supplied
        .as_ref()
        .is_some_and(|new| artifacts_differ(new, previous.artifacts.as_deref().unwrap_or(&[])));

    task.timeline = previous.timeline;
    task.generation = previous.generation;
    if task.history.is_none() {
        task.history = previous.history;
    }
    if task.metadata.is_none() {
        task.metadata = previous.metadata;
    }
    let stored_artifacts = previous.artifacts;
    task.artifacts = match supplied {
        Some(new) if artifacts_changed => Some(new),
        _ => stored_artifacts.clone(),
    };

    if status_changed || artifacts_changed {
        task.generation += 1;
        let generation = task.generation;
        if status_changed {
            task.timeline
                .get_or_insert_with(Vec::new)
                .push(TimelineEntry {
                    content: TimelineContent::Status(task.status.clone()),
                    generation,
                });
        }
        let stored = stored_artifacts.unwrap_or_default();
        for artifact in task.artifacts.iter_mut().flatten() {
            let before = stored
                .iter()
                .find(|stored| stored.artifact_id == artifact.artifact_id);
            artifact.start_generation = artifact
                .start_generation
                .or(before.and_then(|stored| stored.start_generation))
                .or(Some(generation));
            artifact.end_generation = artifact
                .end_generation
                .or(before.and_then(|stored| stored.end_generation));
            artifact.role = artifact
                .role
                .clone()
                .or_else(|| before.and_then(|stored| stored.role.clone()));
        }
    }
    if task.status.state.is_terminal() {
        close_open_artifacts(task, task.generation);
    }
}

/// Applies an executor event to the stored task and stamps the event with the
/// task's generation after the mutation (A2A 1.1 §3.2.7): every status or
/// artifact update advances the generation by exactly one and maps to one
/// event.
async fn apply_event_to_task(
    task_store: &dyn crate::TaskStore,
    current_task: Option<Task>,
    event: &mut StreamResponse,
) -> Result<Option<Task>, A2AError> {
    apply_event_to_new_or_stored_task(task_store, current_task, event, false).await
}

/// [`apply_event_to_task`], where `creating` says the event is the first of a
/// task that is not in the store yet.
async fn apply_event_to_new_or_stored_task(
    task_store: &dyn crate::TaskStore,
    current_task: Option<Task>,
    event: &mut StreamResponse,
    creating: bool,
) -> Result<Option<Task>, A2AError> {
    match event {
        StreamResponse::Task(task) => {
            let previous = match current_task {
                Some(previous) => Some(previous),
                None => task_store.get(&task.id).await?,
            };
            match previous {
                Some(previous) => rebase_snapshot(task, previous),
                None => {
                    task.generation = 0;
                    task.timeline = None;
                    let generation = record_status(task, task.status.clone());
                    for artifact in task.artifacts.iter_mut().flatten() {
                        artifact.start_generation.get_or_insert(generation);
                    }
                    if task.status.state.is_terminal() {
                        close_open_artifacts(task, generation);
                    }
                }
            }
            save_task_with(task_store, task.clone(), creating)
                .await
                .map(Some)
        }
        StreamResponse::StatusUpdate(update) => {
            let mut task = match current_task {
                Some(task) => task,
                None => task_store
                    .get(&update.task_id)
                    .await?
                    .ok_or_else(|| A2AError::task_not_found(&update.task_id))?,
            };
            task.status = update.status.clone();
            update.generation = record_status(&mut task, update.status.clone());
            if task.status.state.is_terminal() {
                close_open_artifacts(&mut task, update.generation);
            }
            save_task_with(task_store, task, creating).await.map(Some)
        }
        StreamResponse::ArtifactUpdate(update) => {
            let mut task = match current_task {
                Some(task) => task,
                None => task_store
                    .get(&update.task_id)
                    .await?
                    .ok_or_else(|| A2AError::task_not_found(&update.task_id))?,
            };
            update.generation = merge_artifact(&mut task, update)?;
            save_task_with(task_store, task, creating).await.map(Some)
        }
        // A message appended to the task, and a rejection of a client request,
        // are what the server reports about *clients*. An executor has no
        // business producing either: forwarded, they would carry no
        // generation and reach subscribers that never asked for them.
        StreamResponse::MessageUpdate(_) | StreamResponse::Error(_) => {
            Err(A2AError::invalid_agent_response())
        }
        _ => Ok(current_task),
    }
}

/// Applies `event` to the task under the writer lock the caller holds and
/// publishes it to the subscribers.
///
/// A `Task` snapshot that changed nothing keeps the generation it found. It
/// is published marked as a repeat, which client-facing streams drop: the
/// generation was already announced, and a second event at it would break
/// A2A 1.1 §3.2.7. Returns whether the event is one clients should see (and
/// be pushed).
async fn apply_and_publish(
    task_store: &dyn crate::TaskStore,
    active: &ActiveExecution,
    base: Option<Task>,
    event: &mut StreamResponse,
) -> Result<bool, A2AError> {
    apply_and_publish_with(task_store, active, base, event, false).await
}

/// [`apply_and_publish`], where `creating` says the event is the first of a
/// task that is not in the store yet.
async fn apply_and_publish_with(
    task_store: &dyn crate::TaskStore,
    active: &ActiveExecution,
    base: Option<Task>,
    event: &mut StreamResponse,
    creating: bool,
) -> Result<bool, A2AError> {
    let generation_before = base.as_ref().map(|task| task.generation);
    let updated_task = apply_event_to_new_or_stored_task(task_store, base, event, creating).await?;
    let repeated = matches!(
        &*event,
        StreamResponse::Task(task) if Some(task.generation) == generation_before
    );
    active
        .publish_marked(Ok(&*event), updated_task, repeated)
        .await;
    Ok(!repeated)
}

fn subscription_stream(
    receiver: broadcast::Receiver<ExecutionEvent>,
    snapshot_task: Option<Task>,
    min_sequence: u64,
    include_repeats: bool,
    message_updates: bool,
    live: bool,
) -> BoxStream<'static, Result<StreamResponse, A2AError>> {
    Box::pin(futures::stream::unfold(
        SubscriptionState {
            receiver,
            snapshot_task,
            min_sequence,
            done: false,
        },
        move |mut state| async move {
            if state.done {
                return None;
            }

            if let Some(task) = state.snapshot_task.take() {
                if task.status.state.is_terminal() {
                    state.done = true;
                }
                return Some((Ok(StreamResponse::Task(task)), state));
            }

            loop {
                match state.receiver.recv().await {
                    Ok(event) if event.sequence <= state.min_sequence => continue,
                    Ok(event) if event.kind == EventKind::Repeated && !include_repeats => {
                        state.min_sequence = event.sequence;
                        continue;
                    }
                    Ok(event) if event.kind == EventKind::LiveOnly && !live => {
                        state.min_sequence = event.sequence;
                        continue;
                    }
                    // A message appended to the task is a payload kind a 1.0
                    // client does not know; only one that announced 1.1 or later
                    // is sent it, and reconciles the generation gap otherwise.
                    Ok(event)
                        if !message_updates
                            && matches!(&event.result, Ok(StreamResponse::MessageUpdate(_))) =>
                    {
                        state.min_sequence = event.sequence;
                        continue;
                    }
                    Ok(event) => {
                        state.min_sequence = event.sequence;
                        match &event.result {
                            Ok(item) if is_terminal_event(item) => state.done = true,
                            Err(_) => state.done = true,
                            _ if event.kind == EventKind::Reply => state.done = true,
                            _ => {}
                        }
                        return Some((event.result.clone(), state));
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        state.done = true;
                        return Some((
                            Err(crate::sanitized_internal_error(
                                "subscription fell behind active execution",
                            )),
                            state,
                        ));
                    }
                    Err(broadcast::error::RecvError::Closed) => return None,
                }
            }
        },
    ))
}

async fn drive_execution(
    runtime: ExecutionRuntime,
    task_id: TaskId,
    active: Arc<ActiveExecution>,
    exec_ctx: crate::ExecutorContext,
) {
    // What a turn that takes over from this one inherits from the request.
    let turn = TurnTemplate {
        service_params: exec_ctx.service_params.clone(),
        tenant: exec_ctx.tenant.clone(),
        metadata: exec_ctx.metadata.clone(),
    };
    let mut stream = runtime.executor.execute(exec_ctx);
    // Set once the execution has been released at a direct reply.
    let mut finished = false;
    // Identifies this execution among the deliveries queued for the task.
    let execution = Arc::as_ptr(&active) as usize;

    while let Some(result) = stream.next().await {
        if active.snapshot_is_terminal().await {
            break;
        }
        // A webhook that failed, with `fail_on_error`, fails the execution.
        if let Some(error) = runtime
            .execution_manager
            .pushes
            .take_failure(&task_id, execution)
        {
            fail_execution(&runtime.execution_manager, &active, &task_id, &error).await;
            break;
        }

        match result {
            Ok(event)
                if active.uncreated.load(std::sync::atomic::Ordering::SeqCst)
                    && matches!(event, StreamResponse::Message(_)) =>
            {
                // The executor answered with a message alone: there is no task,
                // and nothing is saved. The reply is the last thing a stream
                // following this execution is sent.
                {
                    let _writer = active.writer.lock().await;
                    active
                        .publish_kind(Ok(&event), None, EventKind::Reply)
                        .await;
                }
                release_execution(&runtime.execution_manager, &active, &task_id).await;
                runtime.task_changed.notify_waiters();
                finished = true;
                // The executor may go on working until it returns, but nothing
                // it says now belongs to any task.
                while let Some(rest) = stream.next().await {
                    if rest.is_ok() {
                        tracing::warn!(
                            task_id = %task_id,
                            "the executor emitted an event after a direct reply; ignored"
                        );
                    }
                }
                break;
            }
            Ok(mut event) => {
                backfill_status_timestamp(&mut event);
                // One writer at a time: the snapshot is read, changed, saved
                // and published under the lock, so a cancel that lands
                // meanwhile is seen here instead of being overwritten. The
                // push below runs after the lock is released: a slow webhook
                // must not hold up a cancel.
                let published = {
                    let _writer = active.writer.lock().await;
                    let base = active.snapshot().await;
                    if base
                        .as_ref()
                        .is_some_and(|task| task.status.state.is_terminal())
                    {
                        None
                    } else {
                        // A terminal event ends every stream; what the executor
                        // never read is answered before it, not after.
                        if is_terminal_event(&event) {
                            reject_unread_inputs_locked(&active, &task_id).await;
                        }
                        // The executor's first event creates a new task.
                        let creating = active.uncreated.load(std::sync::atomic::Ordering::SeqCst);
                        let applied = apply_and_publish_with(
                            runtime.task_store.as_ref(),
                            &active,
                            base,
                            &mut event,
                            creating,
                        )
                        .await;
                        // Whatever the first event was, the executor has spoken:
                        // the task exists, or the event was refused and there is
                        // none. Cleared with the writer held, so a follow-up
                        // that finds the task finds it created.
                        active
                            .uncreated
                            .store(false, std::sync::atomic::Ordering::SeqCst);
                        match applied {
                            Ok(published) => {
                                if creating {
                                    save_pending_push_config(&active, &runtime).await;
                                }
                                // Queued under the writer, in the order events are
                                // published, and never waited for.
                                if published {
                                    runtime.execution_manager.pushes.push(
                                        &task_id,
                                        execution,
                                        &runtime.push_config_store,
                                        &runtime.push_sender,
                                        &event,
                                    );
                                }
                                runtime.task_changed.notify_waiters();
                                Some(published)
                            }
                            Err(error) => {
                                release_execution_locked(
                                    &runtime.execution_manager,
                                    &active,
                                    &task_id,
                                )
                                .await;
                                active.publish(Err(&error), None).await;
                                None
                            }
                        }
                    }
                };
                let Some(published) = published else {
                    break;
                };
                if !published {
                    continue;
                }
                if is_terminal_event(&event) {
                    break;
                }
            }
            Err(error) => {
                fail_execution(&runtime.execution_manager, &active, &task_id, &error).await;
                break;
            }
        }
    }

    if !finished {
        end_turn(&runtime, &active, &task_id, &turn).await;
        runtime.task_changed.notify_waiters();
    }
}

/// What a turn that takes over from another inherits from the request.
struct TurnTemplate {
    service_params: ServiceParams,
    tenant: Option<String>,
    metadata: Option<HashMap<String, serde_json::Value>>,
}

/// Ends a turn whose executor has returned, under the task's writer.
///
/// An executor that ended without saying anything still has a task: the one
/// the request prepared. Messages the executor never read, while the task goes
/// on, are not turned away: they were accepted and recorded, so the next turn
/// is theirs. It starts here, under the same writer, with the first as its
/// message and the rest queued. A message is accepted only if some turn will
/// read it.
async fn end_turn(
    runtime: &ExecutionRuntime,
    active: &Arc<ActiveExecution>,
    task_id: &TaskId,
    turn: &TurnTemplate,
) {
    let writer = Arc::clone(&active.writer).lock_owned().await;
    if active
        .uncreated
        .swap(false, std::sync::atomic::Ordering::SeqCst)
    {
        if let Some(task) = active.snapshot().await {
            match save_task_with(runtime.task_store.as_ref(), task, true).await {
                Ok(_) => save_pending_push_config(active, runtime).await,
                Err(error) => {
                    tracing::warn!(task_id = %task_id, %error, "could not create the task");
                }
            }
        }
    }
    let snapshot = active.snapshot().await;
    let goes_on = snapshot
        .as_ref()
        .is_some_and(|task| !task.status.state.is_terminal());
    let unread = match active.inputs.get() {
        Some(queue) if goes_on => queue.close_and_drain(),
        _ => Vec::new(),
    };
    let (Some(snapshot), false) = (snapshot, unread.is_empty()) else {
        release_execution_locked(&runtime.execution_manager, active, task_id).await;
        return;
    };
    runtime.execution_manager.finish(task_id, active).await;
    if let Err(unread) = start_next_turn(runtime, task_id, snapshot, unread, turn, writer).await {
        // Nothing could take them over: they are answered, as at any end.
        reject_inputs(active, task_id, unread).await;
    }
}

/// Starts the turn that takes over `unread` messages, under the task's
/// writer. Gives them back if it cannot.
async fn start_next_turn(
    runtime: &ExecutionRuntime,
    task_id: &TaskId,
    snapshot: Task,
    mut unread: Vec<crate::executor::ClientInput>,
    turn: &TurnTemplate,
    writer: tokio::sync::OwnedMutexGuard<()>,
) -> Result<(), Vec<crate::executor::ClientInput>> {
    let Ok((next, guard)) = runtime
        .execution_manager
        .reserve_held(task_id, writer)
        .await
    else {
        return Err(unread);
    };
    next.set_snapshot(snapshot.clone()).await;
    let first = unread.remove(0);
    let queue = Arc::new(InputQueue::new(LIVE_INPUT_CAPACITY));
    for input in unread {
        if let Ok(permit) = queue.try_reserve() {
            permit.send(input);
        }
    }
    let _ = next.inputs.set(queue.clone());
    let exec_ctx = crate::ExecutorContext {
        message: Some(first.message),
        task_id: task_id.clone(),
        stored_task: Some(snapshot.clone()),
        context_id: snapshot.context_id.clone(),
        metadata: turn.metadata.clone(),
        user: None,
        service_params: turn.service_params.clone(),
        tenant: turn.tenant.clone(),
        inputs: Some(ClientInputs::from_queue(queue)),
        new_task: false,
    };
    drop(guard);
    let runtime = runtime.clone();
    let task_id = task_id.clone();
    tokio::spawn(run_execution(runtime, task_id, next, exec_ctx));
    Ok(())
}

/// [`drive_execution_guarded`] as a boxed future: an execution can start the
/// next one as it ends, which a plain `async fn` cannot prove `Send` for.
fn run_execution(
    runtime: ExecutionRuntime,
    task_id: TaskId,
    active: Arc<ActiveExecution>,
    exec_ctx: crate::ExecutorContext,
) -> futures::future::BoxFuture<'static, ()> {
    Box::pin(drive_execution_guarded(runtime, task_id, active, exec_ctx))
}

/// Ends an execution's hold on its task in one step, under the task's writer:
/// the live-input queue is closed (what the executor never read is answered)
/// and the execution deregistered together. A message delivered to the task
/// decides under the same writer, so it finds either a running execution that
/// is taking input, or none.
async fn release_execution(
    manager: &ExecutionManager,
    active: &Arc<ActiveExecution>,
    task_id: &str,
) {
    let _writer = active.writer.lock().await;
    release_execution_locked(manager, active, task_id).await;
}

/// [`release_execution`] for a caller that already holds the task's writer.
/// Every place that closes the queue while the task goes on does so through
/// this, so a closed queue is never found on a registered execution.
async fn release_execution_locked(
    manager: &ExecutionManager,
    active: &Arc<ActiveExecution>,
    task_id: &str,
) {
    reject_unread_inputs_locked(active, task_id).await;
    manager.finish(task_id, active).await;
}

/// Saves the push configuration the request that started the execution
/// carried, now that the task it is for exists.
async fn save_pending_push_config(active: &ActiveExecution, runtime: &ExecutionRuntime) {
    let pending = active
        .pending_push
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take();
    if let (Some(config), Some(store)) = (pending, runtime.push_config_store.as_ref()) {
        if let Err(error) = store.save(config).await {
            tracing::warn!(%error, "could not save the push configuration of a new task");
        }
    }
}

/// Closes the execution's live-input queue and tells the exchanges about each
/// message the executor never read. Those messages were accepted, recorded and
/// announced; the executor's turn ended without handling them, which the
/// client that sent one is told by a `StreamError` naming it, rather than by
/// silence.
async fn reject_unread_inputs(active: &ActiveExecution, task_id: &str) {
    if active.inputs.get().is_none() {
        return;
    }
    // Under the writer: no message can be queued once the queue is closed.
    let _writer = active.writer.lock().await;
    reject_unread_inputs_locked(active, task_id).await;
}

/// [`reject_unread_inputs`] for a caller that already holds the task's writer.
/// Called before a terminal event is published, so the errors come first: a
/// stream ends on the terminal event and would never see them after it.
async fn reject_unread_inputs_locked(active: &ActiveExecution, task_id: &str) {
    let Some(queue) = active.inputs.get() else {
        return;
    };
    reject_inputs(active, task_id, queue.close_and_drain()).await;
}

/// Answers each of `inputs` with a `StreamError` naming it. The caller holds
/// the task's writer.
async fn reject_inputs(
    active: &ActiveExecution,
    task_id: &str,
    inputs: Vec<crate::executor::ClientInput>,
) {
    for input in inputs {
        let event = StreamResponse::Error(StreamError {
            task_id: task_id.to_string(),
            code: "UNAVAILABLE".to_string(),
            message: "the agent finished its turn before reading this message".to_string(),
            reference_id: Some(input.message.message_id.clone()).filter(|id| !id.is_empty()),
            metadata: None,
        });
        active
            .publish_kind(Ok(&event), None, EventKind::LiveOnly)
            .await;
    }
}

/// Ends an execution with `error`: what the executor never read is answered
/// first, then the error reaches the subscribers (which it ends).
async fn fail_execution(
    manager: &ExecutionManager,
    active: &Arc<ActiveExecution>,
    task_id: &str,
    error: &A2AError,
) {
    let _writer = active.writer.lock().await;
    // An executor that fails before it has said anything created no task.
    active
        .uncreated
        .store(false, std::sync::atomic::Ordering::SeqCst);
    release_execution_locked(manager, active, task_id).await;
    active.publish(Err(error), None).await;
}

/// Publishes an error to the subscribers in order with the events around it:
/// two unlocked publishers could broadcast out of sequence, and a subscriber
/// drops what arrives behind a later sequence number.
async fn publish_error(active: &ActiveExecution, error: &A2AError) {
    let _writer = active.writer.lock().await;
    active.publish(Err(error), None).await;
}

/// Runs the execution and, if the executor panicked, fails the task: the
/// subscribers get a terminal `FAILED` event instead of a stream that just
/// stops, and the stored task does not stay `WORKING` for good.
async fn drive_execution_guarded(
    runtime: ExecutionRuntime,
    task_id: TaskId,
    active: Arc<ActiveExecution>,
    exec_ctx: crate::ExecutorContext,
) {
    use futures::FutureExt;

    let task_store = Arc::clone(&runtime.task_store);
    let manager = Arc::clone(&runtime.execution_manager);
    let task_changed = Arc::clone(&runtime.task_changed);
    let push_config_store = runtime.push_config_store.clone();
    let push_sender = runtime.push_sender.clone();
    let outcome = std::panic::AssertUnwindSafe(drive_execution(
        runtime,
        task_id.clone(),
        active.clone(),
        exec_ctx,
    ))
    .catch_unwind()
    .await;
    if outcome.is_ok() {
        return;
    }

    tracing::error!(task_id = %task_id, "the agent executor panicked; failing the task");
    let mut failed_event = None;
    {
        let _writer = active.writer.lock().await;
        // A task the executor never created, or answered with a message alone,
        // stays uncreated: the panic is reported on the streams, and there is
        // no task to fail.
        let registered = manager
            .get(&task_id)
            .await
            .is_some_and(|current| Arc::ptr_eq(&current, &active));
        if !registered {
            // The execution was released before the panic (it came from
            // dropping the executor's stream): a later turn may hold the task
            // now, and this one has nothing left to fail.
        } else if active.uncreated.load(std::sync::atomic::Ordering::SeqCst) {
            active
                .publish(
                    Err(&A2AError::internal("the agent failed unexpectedly")),
                    None,
                )
                .await;
        } else if let Some(base) = active.snapshot().await {
            if !base.status.state.is_terminal() {
                reject_unread_inputs_locked(&active, &task_id).await;
                let mut event = StreamResponse::StatusUpdate(TaskStatusUpdateEvent {
                    task_id: task_id.clone(),
                    context_id: base.context_id.clone(),
                    status: TaskStatus {
                        state: TaskState::Failed,
                        message: Some(Message::new(
                            Role::Agent,
                            vec![Part::text("The agent failed unexpectedly.")],
                        )),
                        timestamp: Some(chrono::Utc::now()),
                    },
                    metadata: None,
                    generation: 0,
                });
                match apply_and_publish(task_store.as_ref(), &active, Some(base), &mut event).await
                {
                    Ok(_) => failed_event = Some(event),
                    Err(error) => {
                        release_execution_locked(&manager, &active, &task_id).await;
                        active.publish(Err(&error), None).await;
                    }
                }
            }
        }
    }
    reject_unread_inputs(&active, &task_id).await;
    if let Some(event) = failed_event {
        manager
            .pushes
            .push(&task_id, 0, &push_config_store, &push_sender, &event);
    }
    release_execution(&manager, &active, &task_id).await;
    task_changed.notify_waiters();
}

/// Transport-agnostic request handler interface.
///
/// This is the core interface consumed by all protocol bindings
/// (JSON-RPC, REST, gRPC). The [`DefaultRequestHandler`] provides a
/// standard implementation that orchestrates task lifecycle, storage,
/// and executor dispatch.
#[async_trait]
pub trait RequestHandler: Send + Sync + 'static {
    async fn send_message(
        &self,
        params: &ServiceParams,
        req: SendMessageRequest,
    ) -> Result<SendMessageResponse, A2AError>;

    async fn send_streaming_message(
        &self,
        params: &ServiceParams,
        req: SendMessageRequest,
    ) -> Result<BoxStream<'static, Result<StreamResponse, A2AError>>, A2AError>;

    async fn get_task(&self, params: &ServiceParams, req: GetTaskRequest)
    -> Result<Task, A2AError>;

    async fn list_tasks(
        &self,
        params: &ServiceParams,
        req: ListTasksRequest,
    ) -> Result<ListTasksResponse, A2AError>;

    async fn cancel_task(
        &self,
        params: &ServiceParams,
        req: CancelTaskRequest,
    ) -> Result<Task, A2AError>;

    async fn subscribe_to_task(
        &self,
        params: &ServiceParams,
        req: SubscribeToTaskRequest,
    ) -> Result<BoxStream<'static, Result<StreamResponse, A2AError>>, A2AError>;

    async fn create_push_config(
        &self,
        params: &ServiceParams,
        req: TaskPushNotificationConfig,
    ) -> Result<TaskPushNotificationConfig, A2AError>;

    async fn get_push_config(
        &self,
        params: &ServiceParams,
        req: GetTaskPushNotificationConfigRequest,
    ) -> Result<TaskPushNotificationConfig, A2AError>;

    async fn list_push_configs(
        &self,
        params: &ServiceParams,
        req: ListTaskPushNotificationConfigsRequest,
    ) -> Result<ListTaskPushNotificationConfigsResponse, A2AError>;

    async fn delete_push_config(
        &self,
        params: &ServiceParams,
        req: DeleteTaskPushNotificationConfigRequest,
    ) -> Result<(), A2AError>;

    async fn get_extended_agent_card(
        &self,
        params: &ServiceParams,
        req: GetExtendedAgentCardRequest,
    ) -> Result<AgentCard, A2AError>;

    /// `SendLiveMessage` over a transport with native duplex (gRPC): `uplink`
    /// carries the client's requests and the returned stream the task's events,
    /// for as long as the exchange lasts. A request that cannot even open the
    /// exchange is an `Err`; one rejected later is a `StreamError` on the
    /// stream, which stays open.
    ///
    /// Not supported unless the handler overrides it.
    async fn send_live_message(
        &self,
        params: &ServiceParams,
        uplink: BoxStream<'static, StreamRequest>,
    ) -> Result<BoxStream<'static, Result<StreamResponse, A2AError>>, A2AError> {
        let _ = (params, uplink);
        Err(A2AError::unsupported_operation(
            "SendLiveMessage is not supported",
        ))
    }

    /// `SendLiveMessage` over a transport that has none (JSON-RPC, REST): each
    /// call carries one request. The call that opens an exchange gets its events
    /// back; a later call for the same task is input to that exchange and gets
    /// only the task as it stands (see [`LiveResponse`]).
    ///
    /// Not supported unless the handler overrides it.
    async fn send_live_request(
        &self,
        params: &ServiceParams,
        request: StreamRequest,
    ) -> Result<LiveResponse, A2AError> {
        let _ = (params, request);
        Err(A2AError::unsupported_operation(
            "SendLiveMessage is not supported",
        ))
    }
}

/// Decides whether a request may proceed.
///
/// The handler never treats a non-empty header as a principal. An
/// implementation must validate whatever credential it accepts (token, DID,
/// session) and return [`A2AError`] when that check fails. `task_id` is set
/// when the method addresses one task and is reserved for owner checks.
pub trait RequestAuthorizer: Send + Sync {
    fn authorize(&self, params: &ServiceParams, task_id: Option<&str>) -> Result<(), A2AError>;
}

impl<F> RequestAuthorizer for F
where
    F: Fn(&ServiceParams, Option<&str>) -> Result<(), A2AError> + Send + Sync,
{
    fn authorize(&self, params: &ServiceParams, task_id: Option<&str>) -> Result<(), A2AError> {
        self(params, task_id)
    }
}

/// Default implementation of [`RequestHandler`] that orchestrates
/// task lifecycle management, storage, and executor dispatch.
///
/// Cheap to clone: everything it holds is shared.
#[derive(Clone)]
pub struct DefaultRequestHandler {
    executor: Arc<dyn crate::AgentExecutor>,
    task_store: Arc<dyn crate::TaskStore>,
    execution_manager: Arc<ExecutionManager>,
    push_config_store: Option<Arc<dyn crate::PushConfigStore>>,
    push_sender: Option<Arc<crate::HttpPushSender>>,
    capabilities: AgentCapabilities,
    default_input_modes: Vec<String>,
    authorizer: Option<Arc<dyn RequestAuthorizer>>,
    extended_agent_card: Option<Arc<dyn ExtendedAgentCardResolver>>,
    task_changed: Arc<Notify>,
    long_poll_timeout: Duration,
    live: Arc<LiveSessions>,
}

/// Supplies the authenticated extended Agent Card (§12.4, §10.1).
///
/// A resolver rather than a stored card because §12.4 has the extended card
/// fetched *under a security scheme*: which skills an agent reveals can
/// legitimately differ by caller, and `ServiceParams` is where the caller's
/// credentials arrive. An agent serving one card to everyone can ignore the
/// argument — [`DefaultRequestHandler::with_extended_agent_card`] wraps a
/// plain card for exactly that case.
pub trait ExtendedAgentCardResolver: Send + Sync {
    fn resolve(&self, params: &ServiceParams) -> Result<AgentCard, A2AError>;
}

impl<F> ExtendedAgentCardResolver for F
where
    F: Fn(&ServiceParams) -> Result<AgentCard, A2AError> + Send + Sync,
{
    fn resolve(&self, params: &ServiceParams) -> Result<AgentCard, A2AError> {
        self(params)
    }
}

impl DefaultRequestHandler {
    pub fn new(executor: impl crate::AgentExecutor, task_store: impl crate::TaskStore) -> Self {
        DefaultRequestHandler {
            executor: Arc::new(executor),
            task_store: Arc::new(task_store),
            execution_manager: Arc::new(ExecutionManager::default()),
            push_config_store: None,
            push_sender: None,
            capabilities: AgentCapabilities::default(),
            default_input_modes: Vec::new(),
            authorizer: None,
            extended_agent_card: None,
            task_changed: Arc::new(Notify::new()),
            long_poll_timeout: DEFAULT_LONG_POLL_TIMEOUT,
            live: Arc::new(LiveSessions::default()),
        }
    }

    /// A handler for an [`Agent`](crate::Agent), which decides for each message
    /// whether a task exists.
    pub fn from_agent(agent: impl crate::Agent, task_store: impl crate::TaskStore) -> Self {
        Self::new(
            crate::executor::AgentExecutorAdapter(Arc::new(agent)),
            task_store,
        )
    }

    /// Bounds how long a `GetTask` carrying `currentGeneration` is held open
    /// when the task does not advance. The request then returns the current
    /// state, which the spec allows.
    pub fn with_long_poll_timeout(mut self, timeout: Duration) -> Self {
        self.long_poll_timeout = timeout;
        self
    }

    /// Serve `card` as the extended Agent Card, identically for every caller.
    ///
    /// Also declares the `extendedAgentCard` capability, since serving one
    /// and advertising it should not be two separate things to remember —
    /// an explicit `with_capabilities(..)` still overrides it either way.
    pub fn with_extended_agent_card(mut self, card: AgentCard) -> Self {
        self.capabilities.extended_agent_card.get_or_insert(true);
        self.with_extended_agent_card_resolver(move |_: &ServiceParams| Ok(card.clone()))
    }

    /// Serve an extended Agent Card computed per caller, so an agent can
    /// reveal different skills to different principals (§12.4).
    pub fn with_extended_agent_card_resolver(
        mut self,
        resolver: impl ExtendedAgentCardResolver + 'static,
    ) -> Self {
        self.capabilities.extended_agent_card.get_or_insert(true);
        self.extended_agent_card = Some(Arc::new(resolver));
        self
    }

    /// Install an authorizer. The default handler does not enforce one.
    pub fn with_authorizer(mut self, authorizer: impl RequestAuthorizer + 'static) -> Self {
        self.authorizer = Some(Arc::new(authorizer));
        self
    }

    fn authorize(&self, params: &ServiceParams, task_id: Option<&str>) -> Result<(), A2AError> {
        match &self.authorizer {
            Some(authorizer) => authorizer.authorize(params, task_id),
            None => Ok(()),
        }
    }

    pub fn with_push_config_store(
        mut self,
        push_config_store: impl crate::PushConfigStore,
    ) -> Self {
        self.push_sender = Some(Arc::new(crate::HttpPushSender::new(None)));
        self.push_config_store = Some(Arc::new(push_config_store));
        // Only a default: an explicit `Some(false)` from `with_capabilities`
        // stands, so the result no longer depends on which builder method was
        // called last.
        self.capabilities.push_notifications.get_or_insert(true);
        self
    }

    pub fn with_push_notifications(
        mut self,
        push_config_store: impl crate::PushConfigStore,
        push_sender: crate::HttpPushSender,
    ) -> Self {
        self.push_config_store = Some(Arc::new(push_config_store));
        self.push_sender = Some(Arc::new(push_sender));
        self.capabilities.push_notifications.get_or_insert(true);
        self
    }

    pub fn with_capabilities(mut self, capabilities: AgentCapabilities) -> Self {
        self.capabilities = capabilities;
        self
    }

    /// Declares the media types incoming message parts may use (§5.1's
    /// `defaultInputModes`). Empty (the default) means unrestricted, so
    /// embedders that never call this see no change in behavior.
    pub fn with_default_input_modes(mut self, modes: impl Into<Vec<String>>) -> Self {
        self.default_input_modes = modes.into();
        self
    }

    /// A part with a declared, non-empty media type that isn't among the
    /// advertised `defaultInputModes` is rejected; a part that declares none
    /// is unrestricted, and so is every part when no modes were declared.
    fn validate_content_types(&self, message: &Message) -> Result<(), A2AError> {
        if self.default_input_modes.is_empty() {
            return Ok(());
        }
        let supported = message.parts.iter().all(|part| {
            part.media_type.as_deref().is_none_or(|media_type| {
                media_type.is_empty() || self.default_input_modes.iter().any(|m| m == media_type)
            })
        });
        if supported {
            Ok(())
        } else {
            Err(A2AError::content_type_not_supported())
        }
    }

    /// §13.3: refuse a capability-gated operation the card does not declare.
    ///
    /// `None` means "not restricted", not "refused". `AgentCapabilities::
    /// default()` leaves every field `None` and most embedders never call
    /// [`Self::with_capabilities`], so treating `None` as a refusal would
    /// break all of them at once. Only an explicit `Some(false)` refuses.
    fn require_capability(
        declared: Option<bool>,
        unsupported: impl FnOnce() -> A2AError,
    ) -> Result<(), A2AError> {
        match declared {
            Some(false) => Err(unsupported()),
            _ => Ok(()),
        }
    }

    /// `artifactUpdates` is opt-in: unlike the other capabilities, an
    /// undeclared `userArtifacts` means the agent does not accept them.
    fn validate_artifact_updates(&self, req: &SendMessageRequest) -> Result<(), A2AError> {
        if req.artifact_updates.is_empty() || self.capabilities.user_artifacts == Some(true) {
            return Ok(());
        }
        Err(A2AError::unsupported_operation(
            "the agent card does not declare userArtifacts",
        ))
    }

    /// Long-poll for `GetTaskRequest.currentGeneration` (A2A 1.1 §3.2.7):
    /// returns once the task's generation exceeds `current`, or with the
    /// current state when the timeout elapses or the task is terminal.
    async fn wait_for_generation(&self, task_id: &str, current: i64) -> Result<Task, A2AError> {
        let deadline = tokio::time::Instant::now() + self.long_poll_timeout;
        loop {
            // Registered before the store is read, so a change landing in
            // between still wakes this wait.
            let changed = self.task_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();

            let task = self.load_task(task_id).await?;
            if task.generation > current || task.status.state.is_terminal() {
                return Ok(task);
            }
            let now = tokio::time::Instant::now();
            if now >= deadline {
                return Ok(task);
            }
            tokio::select! {
                _ = &mut changed => {}
                _ = tokio::time::sleep((deadline - now).min(LONG_POLL_RECHECK)) => {}
            }
        }
    }

    fn require_streaming(&self) -> Result<(), A2AError> {
        Self::require_capability(self.capabilities.streaming, || {
            A2AError::unsupported_operation("the agent card does not declare streaming")
        })
    }

    fn require_push_notifications(&self) -> Result<(), A2AError> {
        Self::require_capability(
            self.capabilities.push_notifications,
            A2AError::push_notification_not_supported,
        )
    }

    /// The push-config store, refusing first when the card declares that the
    /// agent does not support push notifications (§13.3). Distinct from
    /// [`Self::push_config_store`], which reports the *deployment* state — a
    /// store that was never supplied — with the same error code.
    fn push_config_store_checked(&self) -> Result<&dyn crate::PushConfigStore, A2AError> {
        self.require_push_notifications()?;
        self.push_config_store()
    }

    fn push_config_store(&self) -> Result<&dyn crate::PushConfigStore, A2AError> {
        self.push_config_store
            .as_deref()
            .ok_or_else(A2AError::push_notification_not_supported)
    }

    async fn load_task(&self, task_id: &str) -> Result<Task, A2AError> {
        self.task_store
            .get(task_id)
            .await?
            .ok_or_else(|| A2AError::task_not_found(task_id))
    }

    /// Applies the request to the task and persists the result, returning the
    /// task, whether it existed before, and its context id.
    ///
    /// The caller holds the task's execution reservation and its writer lock,
    /// so nothing else -- another request, a cancel -- can change it between
    /// the read, the `ifGenerationMatch` comparison and the save below.
    async fn prepare_task_for_execution(
        &self,
        req: &SendMessageRequest,
        task_id: &str,
        lazy: bool,
    ) -> Result<(Task, bool, String, Vec<StreamResponse>), A2AError> {
        let artifact_only = is_artifact_only(req);
        // A2A §3.4.2: a client-provided taskId MUST reference an existing task,
        // and client-provided ids for *creating* tasks are not supported. Only
        // an absent taskId may mint a new one.
        let stored = self.task_store.get(task_id).await?;
        if stored.is_none() && (req.message.task_id.is_some() || artifact_only) {
            return Err(A2AError::task_not_found(task_id));
        }
        if stored
            .as_ref()
            .is_some_and(|task| task.status.state.is_terminal())
        {
            return Err(A2AError::unsupported_operation(format!(
                "task {task_id} has already reached a terminal state"
            )));
        }

        // A2A 1.1 §3.2.2: `ifGenerationMatch` is an optimistic concurrency
        // guard for a follow-up message. It is ignored when the message
        // creates the task, so there is nothing to compare then.
        if let (Some(task), Some(expected)) = (
            stored.as_ref(),
            req.configuration
                .as_ref()
                .and_then(|configuration| configuration.if_generation_match),
        ) {
            if task.generation != expected {
                return Err(A2AError::task_generation_mismatch(
                    &task.id,
                    expected,
                    task.generation,
                ));
            }
        }

        // A2A §3.4.3: reject a mismatching contextId/taskId pair, and infer
        // contextId from the task when only taskId is given.
        let context_id = match stored.as_ref() {
            Some(task) => {
                if let Some(requested) = req.message.context_id.as_deref() {
                    if requested != task.context_id {
                        return Err(A2AError::invalid_params(format!(
                            "contextId {requested} does not match task {task_id}, \
                             which belongs to context {}",
                            task.context_id
                        )));
                    }
                }
                task.context_id.clone()
            }
            None => req
                .message
                .context_id
                .clone()
                .unwrap_or_else(new_context_id),
        };

        let existed = stored.is_some();
        let mut events = Vec::new();
        let task = if let Some(mut existing) = stored {
            // A continuation turn's own incoming message was never otherwise
            // recorded anywhere: the executor only sees it via `ctx.message`
            // for deciding this turn's behavior, and `apply_event_to_task`
            // only ever merges status/artifact deltas onto the snapshot
            // started here, never anything content-derived. Append it so
            // `history` actually accumulates across turns.
            if !artifact_only {
                existing
                    .history
                    .get_or_insert_with(Vec::new)
                    .push(req.message.clone());
            }
            events = record_client_input(&mut existing, req)?;
            // Persisted now rather than with the executor's first event, so
            // the message and its generation are visible at once and survive
            // an executor that answers with a direct message or nothing.
            self.task_store.update(existing.clone()).await?;
            existing
        } else {
            let mut task = Task {
                generation: 1,
                timeline: Some(vec![TimelineEntry {
                    content: TimelineContent::Message(req.message.clone()),
                    generation: 1,
                }]),
                id: task_id.to_string(),
                context_id: context_id.clone(),
                status: TaskStatus {
                    state: TaskState::Submitted,
                    message: None,
                    timestamp: Some(chrono::Utc::now()),
                },
                artifacts: None,
                history: Some(vec![req.message.clone()]),
                metadata: None,
            };
            events.push(client_message_event(&task, &req.message));
            events.extend(
                apply_client_artifacts(&mut task, req, true)?
                    .into_iter()
                    .map(StreamResponse::ArtifactUpdate),
            );
            // A task the executor has yet to create is only prepared: it is
            // saved with the executor's first event, or not at all.
            if !lazy {
                self.task_store.create(task.clone()).await?;
            }
            task
        };

        Ok((task, existed, context_id, events))
    }

    /// The push configuration the request carries, for `task_id`.
    fn request_push_config(
        &self,
        params: &ServiceParams,
        task_id: &str,
        req: &SendMessageRequest,
    ) -> Result<Option<TaskPushNotificationConfig>, A2AError> {
        let Some(mut config) = req
            .configuration
            .as_ref()
            .and_then(|configuration| configuration.task_push_notification_config.clone())
        else {
            return Ok(None);
        };
        // Refused up front when the handler has nowhere to keep it.
        self.push_config_store_checked()?;
        config.task_id = task_id.to_string();
        if config.tenant.is_none() {
            config.tenant = req.tenant.clone();
        }
        config
            .protocol_version
            .get_or_insert_with(|| requested_protocol_version(params));
        Ok(Some(config))
    }

    async fn save_request_push_config(
        &self,
        params: &ServiceParams,
        task_id: &str,
        req: &SendMessageRequest,
    ) -> Result<(), A2AError> {
        if let Some(config) = self.request_push_config(params, task_id, req)? {
            self.push_config_store_checked()?.save(config).await?;
        }
        Ok(())
    }

    /// Applies the executor's cancel events, recording the cancellation itself
    /// when they do not. `begin_cancel` has already moved the task to
    /// `CANCELED` without an event of its own, so the transition counts as
    /// recorded only once a `CANCELED` status entry follows it -- including
    /// when the executor fails, so the state change is never left without a
    /// generation of its own.
    async fn run_cancel(
        &self,
        task_id: &str,
        canceled: &Task,
        recorded_by_store: bool,
        active: Option<&Arc<ActiveExecution>>,
        exec_ctx: crate::ExecutorContext,
    ) -> Result<(), A2AError> {
        let mut stream = self.executor.cancel(exec_ctx);
        let mut current_task = Some(canceled.clone());
        let mut failure = None;
        let mut entry = CancelEntry {
            recorded_by_store,
            generation: canceled.generation,
            announced: false,
        };

        // Whatever goes wrong below, the cancellation is still recorded
        // afterwards: `begin_cancel` has already changed the task.
        while let Some(event) = stream.next().await {
            let mut event = match event {
                Ok(event) => event,
                Err(error) => {
                    failure = Some(error);
                    break;
                }
            };
            backfill_status_timestamp(&mut event);
            let published = match self
                .apply_cancel_event(active, &mut current_task, &mut entry, &mut event)
                .await
            {
                Ok(published) => published,
                Err(error) => {
                    failure = Some(error);
                    break;
                }
            };
            if !published {
                continue;
            }
            self.execution_manager.pushes.push(
                task_id,
                0,
                &self.push_config_store,
                &self.push_sender,
                &event,
            );
            if is_terminal_event(&event) {
                break;
            }
        }

        let recorded = if entry.recorded_by_store {
            self.announce_cancel_if_missing(task_id, active, &entry, &current_task)
                .await
        } else {
            self.record_cancel_if_missing(task_id, canceled, active, &mut current_task)
                .await
        };
        // The recorded transition is announced like any other, even when the
        // cancel itself is about to be reported as failed.
        if let Ok(Some(event)) = &recorded {
            self.execution_manager.pushes.push(
                task_id,
                0,
                &self.push_config_store,
                &self.push_sender,
                event,
            );
        }
        if let Some(error) = failure {
            if let Some(active) = active {
                publish_error(active, &error).await;
            }
            return Err(error);
        }
        recorded?;
        Ok(())
    }

    /// Applies one event of a cancel under the task's writer lock. Returns
    /// whether it was published to the subscribers.
    async fn apply_cancel_event(
        &self,
        active: Option<&Arc<ActiveExecution>>,
        current_task: &mut Option<Task>,
        entry: &mut CancelEntry,
        event: &mut StreamResponse,
    ) -> Result<bool, A2AError> {
        // The task is already canceled: a cancel executor may restate that,
        // with its own message, but not move the task anywhere else -- back to
        // work, or to a different end.
        let contradicts = match &*event {
            StreamResponse::Task(task) => task.status.state != TaskState::Canceled,
            StreamResponse::StatusUpdate(update) => update.status.state != TaskState::Canceled,
            _ => false,
        };
        if contradicts {
            return Err(A2AError::invalid_agent_response());
        }
        let published = match active {
            Some(active) => {
                let _writer = active.writer.lock().await;
                let base = active.snapshot().await;
                if let Some(base) = base.as_ref().filter(|base| entry.can_amend(base, event)) {
                    // The store already gave the cancellation its generation
                    // and timeline entry; the executor's status is that
                    // entry's content, announced as its one event.
                    let amended = self.amend_cancel_entry(base.clone(), event).await?;
                    active
                        .publish_marked(Ok(&*event), Some(amended), false)
                        .await;
                    entry.announced = true;
                    true
                } else {
                    let published =
                        apply_and_publish(self.task_store.as_ref(), active, base, event).await?;
                    entry.note_published(published, event);
                    published
                }
            }
            None if current_task
                .as_ref()
                .is_some_and(|base| entry.can_amend(base, event)) =>
            {
                let base = current_task.take().expect("checked above");
                *current_task = Some(self.amend_cancel_entry(base, event).await?);
                entry.announced = true;
                true
            }
            None => {
                let generation_before = current_task.as_ref().map(|task| task.generation);
                *current_task =
                    apply_event_to_task(self.task_store.as_ref(), current_task.take(), event)
                        .await?;
                // A snapshot that changed nothing is not announced a second
                // time at the generation it found.
                let published = !matches!(
                    &*event,
                    StreamResponse::Task(task) if Some(task.generation) == generation_before
                );
                entry.note_published(published, event);
                published
            }
        };
        self.task_changed.notify_waiters();
        Ok(published)
    }

    /// Gives the executor's `CANCELED` status to the entry the store already
    /// recorded for the cancellation, without advancing the generation, and
    /// stamps the event with it. Returns the saved task.
    async fn amend_cancel_entry(
        &self,
        mut task: Task,
        event: &mut StreamResponse,
    ) -> Result<Task, A2AError> {
        let StreamResponse::StatusUpdate(update) = event else {
            return Err(A2AError::internal(
                "only a status can amend the cancel entry",
            ));
        };
        task.status = update.status.clone();
        if let Some(TimelineEntry {
            content: TimelineContent::Status(status),
            ..
        }) = task.timeline.iter_mut().flatten().last()
        {
            *status = update.status.clone();
        }
        update.generation = task.generation;
        save_task(self.task_store.as_ref(), task).await
    }

    /// Announces the cancellation the store recorded when the executor sent no
    /// status of its own: a status event at the generation the store gave it,
    /// with no further advance. Returns the event, for the push.
    async fn announce_cancel_if_missing(
        &self,
        task_id: &str,
        active: Option<&Arc<ActiveExecution>>,
        entry: &CancelEntry,
        current_task: &Option<Task>,
    ) -> Result<Option<StreamResponse>, A2AError> {
        if entry.announced {
            return Ok(None);
        }
        let _writer = match active {
            Some(active) => Some(active.writer.lock().await),
            None => None,
        };
        let base = match active {
            Some(active) => active.snapshot().await,
            None => current_task.clone(),
        };
        let Some(base) = base else {
            return Ok(None);
        };
        if base.generation != entry.generation {
            // Something else changed the task since the store recorded the
            // cancellation (an artifact the cancel executor produced, say), so
            // announcing that entry now would repeat a generation already
            // used. The cancellation is recorded again, at the current one.
            let mut status = base.status.clone();
            status.state = TaskState::Canceled;
            let mut event = StreamResponse::StatusUpdate(TaskStatusUpdateEvent {
                task_id: task_id.to_string(),
                context_id: base.context_id.clone(),
                status,
                metadata: None,
                generation: 0,
            });
            match active {
                Some(active) => {
                    apply_and_publish(self.task_store.as_ref(), active, Some(base), &mut event)
                        .await?;
                }
                None => {
                    apply_event_to_task(self.task_store.as_ref(), Some(base), &mut event).await?;
                }
            }
            self.task_changed.notify_waiters();
            return Ok(Some(event));
        }
        let event = StreamResponse::StatusUpdate(TaskStatusUpdateEvent {
            task_id: task_id.to_string(),
            context_id: base.context_id.clone(),
            status: base.status.clone(),
            metadata: None,
            generation: base.generation,
        });
        if let Some(active) = active {
            active.publish(Ok(&event), None).await;
        }
        self.task_changed.notify_waiters();
        Ok(Some(event))
    }

    /// Records the cancellation as a status event unless one has followed the
    /// store transition already. Returns the event it recorded, if any.
    async fn record_cancel_if_missing(
        &self,
        task_id: &str,
        canceled: &Task,
        active: Option<&Arc<ActiveExecution>>,
        current_task: &mut Option<Task>,
    ) -> Result<Option<StreamResponse>, A2AError> {
        let _writer = match active {
            Some(active) => Some(active.writer.lock().await),
            None => None,
        };
        let base = match active {
            Some(active) => active.snapshot().await,
            None => current_task.clone(),
        };
        let recorded = base.as_ref().is_some_and(|task| {
            task.timeline.iter().flatten().any(|entry| {
                entry.generation > canceled.generation
                    && matches!(
                        &entry.content,
                        TimelineContent::Status(status) if status.state == TaskState::Canceled
                    )
            })
        });
        if recorded {
            return Ok(None);
        }
        // Whatever the executor did, this records a cancellation.
        let mut status = base
            .as_ref()
            .map_or_else(|| canceled.status.clone(), |task| task.status.clone());
        status.state = TaskState::Canceled;
        let mut event = StreamResponse::StatusUpdate(TaskStatusUpdateEvent {
            task_id: task_id.to_string(),
            context_id: canceled.context_id.clone(),
            status,
            metadata: None,
            generation: 0,
        });
        match active {
            Some(active) => {
                apply_and_publish(self.task_store.as_ref(), active, base, &mut event).await?;
            }
            None => {
                *current_task =
                    apply_event_to_task(self.task_store.as_ref(), base, &mut event).await?;
            }
        }
        self.task_changed.notify_waiters();
        Ok(Some(event))
    }

    async fn start_execution(
        &self,
        params: &ServiceParams,
        req: SendMessageRequest,
        mode: StartMode,
        writer: Option<tokio::sync::OwnedMutexGuard<()>>,
    ) -> Result<
        (
            Task,
            broadcast::Receiver<ExecutionEvent>,
            Vec<StreamResponse>,
        ),
        A2AError,
    > {
        let include_task_snapshot_in_context = mode == StartMode::Blocking;
        // A new task is created by the executor: only a live exchange, which is
        // bound to its task from the first request, creates it up front.
        let lazy = mode != StartMode::Live;
        let artifact_only = is_artifact_only(&req);
        if !artifact_only {
            if req.message.message_id.is_empty() && !req.artifact_updates.is_empty() {
                return Err(A2AError::invalid_params(
                    "a message sent with artifactUpdates must have a messageId",
                ));
            }
            self.validate_content_types(&req.message)?;
        }
        self.validate_artifact_updates(&req)?;
        let task_id = match request_task_id(&req) {
            Some(task_id) => task_id,
            None if artifact_only => {
                return Err(A2AError::invalid_params(
                    "artifactUpdates sent without a message must name the task",
                ));
            }
            None => new_task_id(),
        };

        // Claim the task before reading it: from here until the execution
        // ends, this request and a cancel are the only writers. The guard gives the claim back
        // on every early return and if this future is dropped.
        let (active, writer_guard) = match writer {
            Some(held) => self.execution_manager.reserve_held(&task_id, held).await?,
            None => self.execution_manager.reserve(&task_id).await?,
        };
        let reservation = ReservationGuard::new(
            Arc::clone(&self.execution_manager),
            task_id.clone(),
            Arc::clone(&active),
        );
        let (task, existed, context_id, input_events) = {
            // The writer, held since the reservation until the snapshot below
            // is set, makes a cancel or a subscriber that finds this execution
            // wait for it instead of racing it.
            let prepared = async {
                let prepared = self
                    .prepare_task_for_execution(&req, &task_id, lazy)
                    .await?;
                if lazy && !prepared.1 {
                    // For a task that may never exist, the configuration waits
                    // for the executor to create it.
                    let config = self.request_push_config(params, &task_id, &req)?;
                    *active
                        .pending_push
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) = config;
                } else {
                    self.save_request_push_config(params, &task_id, &req)
                        .await?;
                }
                Ok::<_, A2AError>(prepared)
            }
            .await;
            match prepared {
                Ok(prepared) => {
                    active.set_snapshot(prepared.0.clone()).await;
                    if lazy && !prepared.1 {
                        active
                            .uncreated
                            .store(true, std::sync::atomic::Ordering::SeqCst);
                    }
                    prepared
                }
                Err(error) => {
                    // Released while the writer is still held, so whoever was
                    // waiting for it finds the task free, not half-reserved.
                    reservation.release().await;
                    return Err(error);
                }
            }
        };
        self.task_changed.notify_waiters();
        // An executor that reads live input gets a queue for it, owned by the
        // server so what the executor never reads is not lost. It is in place
        // before the writer is let go, so no message is turned away for want of
        // one.
        let inputs = if self.executor.accepts_live_input() {
            let queue = Arc::new(InputQueue::new(LIVE_INPUT_CAPACITY));
            let _ = active.inputs.set(queue.clone());
            Some(ClientInputs::from_queue(queue))
        } else {
            None
        };
        // Subscribed before the writer is let go: nothing published after that
        // can be missed, a cancel that lands straight away included.
        let receiver = active.subscribe();
        drop(writer_guard);

        let exec_ctx = crate::ExecutorContext {
            message: (!artifact_only).then_some(req.message),
            task_id: task_id.clone(),
            // The task as it stands once the request is applied. A task this
            // request created is withheld unless the caller asked for it
            // (`send_message` does, streaming does not): executors use its
            // absence to recognise a new task.
            stored_task: (existed || include_task_snapshot_in_context).then(|| task.clone()),
            context_id,
            metadata: req.metadata,
            user: None,
            service_params: params.clone(),
            tenant: req.tenant,
            inputs,
            new_task: !existed,
        };

        let runtime = ExecutionRuntime {
            executor: Arc::clone(&self.executor),
            task_store: Arc::clone(&self.task_store),
            push_config_store: self.push_config_store.clone(),
            push_sender: self.push_sender.clone(),
            execution_manager: Arc::clone(&self.execution_manager),
            task_changed: Arc::clone(&self.task_changed),
        };
        let active_execution = active.clone();
        let execution_task_id = task_id.clone();

        // From here the spawned execution owns the reservation. It keeps its
        // own guard so the task is released even if the executor panics.
        reservation.disarm();
        let release = ReservationGuard::new(
            Arc::clone(&self.execution_manager),
            task_id.clone(),
            Arc::clone(&active),
        );
        tokio::spawn(async move {
            drive_execution_guarded(runtime, execution_task_id, active_execution, exec_ctx).await;
            // Reached only if the executor did not panic: the execution has
            // already released the task itself.
            release.disarm();
        });

        Ok((task, receiver, input_events))
    }
}

/// How a request reaches `start_execution`.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum StartMode {
    /// `SendMessage` that waits for the result.
    Blocking,
    /// `SendStreamingMessage`.
    Streaming,
    /// The first request of a live exchange.
    Live,
}

/// Opens a stream with the task, as A2A requires of a streaming send
/// (§3.1.2). An agent that answers with a message alone gets a stream that is
/// that message and nothing else, and one whose first event is already a task
/// snapshot needs no other opening. Otherwise (a status or artifact update
/// first) the server's own snapshot goes first, so a client never has to guess
/// the task's state from a partial event.
fn with_opening_task(
    stream: BoxStream<'static, Result<StreamResponse, A2AError>>,
    opening: Task,
) -> BoxStream<'static, Result<StreamResponse, A2AError>> {
    Box::pin(
        futures::stream::once(async move {
            let mut stream = stream;
            let Some(first) = stream.next().await else {
                // The executor said nothing a client should see (it only
                // restated the task, or ended): the stream is the task.
                return futures::stream::iter([Ok(StreamResponse::Task(opening))]).boxed();
            };
            let head: Vec<_> = match &first {
                Ok(StreamResponse::Message(_) | StreamResponse::Task(_)) | Err(_) => vec![first],
                _ => vec![Ok(StreamResponse::Task(opening)), first],
            };
            futures::stream::iter(head).chain(stream).boxed()
        })
        .flatten(),
    )
}

#[async_trait]
impl RequestHandler for DefaultRequestHandler {
    async fn send_message(
        &self,
        params: &ServiceParams,
        req: SendMessageRequest,
    ) -> Result<SendMessageResponse, A2AError> {
        self.authorize(params, request_task_id(&req).as_deref())?;
        // ACTS CORE-HIST-003: configuration.historyLength governs this
        // response's own inline task the same way it governs a later
        // GetTask, so it has to be applied here too, not just there.
        let history_length = req
            .configuration
            .as_ref()
            .and_then(|configuration| configuration.history_length);
        let timeline_length = req
            .configuration
            .as_ref()
            .and_then(|configuration| configuration.timeline_length);
        let (task_id, receiver, min_sequence) = match self
            .live_deliver(params, req.clone(), StartMode::Blocking)
            .await
            .map_err(|rejection| unary_refusal(rejection, &req))?
        {
            live::Delivery::Started { task, receiver, .. } => (task.id, receiver, 0),
            // The task is running and its executor took the message: the call
            // waits for the turn it joined, as it would for one it started.
            live::Delivery::Injected {
                mut task,
                receiver,
                sequence_before,
                ..
            } => {
                // Nothing of the running turn is awaited for a call that only
                // wants the task: it is returned as the message left it.
                if req
                    .configuration
                    .as_ref()
                    .and_then(|configuration| configuration.return_immediately)
                    .unwrap_or(false)
                {
                    apply_lengths(&mut task, history_length, timeline_length);
                    return Ok(SendMessageResponse::Task(task));
                }
                (task.id, receiver, sequence_before)
            }
        };
        // Unlike a client-facing stream this one keeps the executor's no-op
        // snapshots: the blocking send only needs to know the executor spoke.
        let mut stream = subscription_stream(receiver, None, min_sequence, true, false, false);
        let mut last_event = None;

        while let Some(item) = stream.next().await {
            let event = item?;

            if let Some(interrupt_task_id) = should_interrupt_non_streaming(&req, &event) {
                let mut task = self.load_task(&interrupt_task_id).await?;
                apply_lengths(&mut task, history_length, timeline_length);
                return Ok(SendMessageResponse::Task(task));
            }

            match event {
                StreamResponse::Message(message) => {
                    return Ok(SendMessageResponse::Message(message));
                }
                other => last_event = Some(other),
            }
        }

        match last_event {
            Some(StreamResponse::Task(mut task)) => {
                apply_lengths(&mut task, history_length, timeline_length);
                Ok(SendMessageResponse::Task(task))
            }
            Some(StreamResponse::StatusUpdate(update)) => {
                let mut task = self.load_task(&update.task_id).await?;
                apply_lengths(&mut task, history_length, timeline_length);
                Ok(SendMessageResponse::Task(task))
            }
            Some(StreamResponse::ArtifactUpdate(update)) => {
                let mut task = self.load_task(&update.task_id).await?;
                apply_lengths(&mut task, history_length, timeline_length);
                Ok(SendMessageResponse::Task(task))
            }
            Some(StreamResponse::Message(message)) => Ok(SendMessageResponse::Message(message)),
            Some(_) | None => {
                let mut task = self.load_task(&task_id).await?;
                apply_lengths(&mut task, history_length, timeline_length);
                Ok(SendMessageResponse::Task(task))
            }
        }
    }

    async fn send_streaming_message(
        &self,
        params: &ServiceParams,
        req: SendMessageRequest,
    ) -> Result<BoxStream<'static, Result<StreamResponse, A2AError>>, A2AError> {
        self.authorize(params, request_task_id(&req).as_deref())?;
        self.require_streaming()?;
        let (task, receiver, min_sequence) = match self
            .live_deliver(params, req.clone(), StartMode::Streaming)
            .await
            .map_err(|rejection| unary_refusal(rejection, &req))?
        {
            live::Delivery::Started { task, receiver, .. } => (task, receiver, 0),
            // The opening task already holds the message and its artifacts, so
            // the stream follows from after them.
            live::Delivery::Injected {
                task,
                receiver,
                sequence_after,
                ..
            } => (task, receiver, sequence_after),
        };
        Ok(with_opening_task(
            subscription_stream(
                receiver,
                None,
                min_sequence,
                false,
                is_at_least_1_1(&requested_protocol_version(params)),
                false,
            ),
            task,
        ))
    }

    async fn get_task(
        &self,
        params: &ServiceParams,
        req: GetTaskRequest,
    ) -> Result<Task, A2AError> {
        self.authorize(params, Some(&req.id))?;
        let mut task = match req.current_generation {
            Some(current) => self.wait_for_generation(&req.id, current).await?,
            None => self.load_task(&req.id).await?,
        };
        apply_lengths(&mut task, req.history_length, req.timeline_length);
        Ok(task)
    }

    /// Bounds the page twice, and neither is redundant: `TaskStore` is a
    /// public trait.
    ///
    /// Clamping `page_size` before the call bounds a store that honours it
    /// -- the ordinary case, and the one that turns `pageSize=999999999`
    /// into a real query. Truncating the response bounds a store that
    /// ignores it, which cannot be detected from here.
    ///
    /// On truncation the store's `next_page_token` is left alone: paging is
    /// the store's own scheme (the in-tree stores use offsets, another may
    /// use an opaque cursor) so no correct token can be synthesised. See
    /// [`TaskStore::list`].
    async fn list_tasks(
        &self,
        params: &ServiceParams,
        req: ListTasksRequest,
    ) -> Result<ListTasksResponse, A2AError> {
        self.authorize(params, None)?;
        let page_size = resolve_page_size(req.page_size);
        let req = ListTasksRequest {
            page_size: Some(page_size as i32),
            ..req
        };
        let mut response = self.task_store.list(&req).await?;
        response.tasks.truncate(page_size);
        for task in &mut response.tasks {
            apply_lengths(task, req.history_length, req.timeline_length);
        }
        Ok(response)
    }

    async fn cancel_task(
        &self,
        params: &ServiceParams,
        req: CancelTaskRequest,
    ) -> Result<Task, A2AError> {
        self.authorize(params, Some(&req.id))?;

        // Whoever is mutating a task holds its writer lock, execution or not,
        // so a cancel takes its turn here: from the store transition until the
        // snapshot is updated, nothing else may save. The execution is looked
        // up only once the lock is held, so it cannot be one that has just
        // finished (or one a follow-up reserves a moment later).
        let writer = self.execution_manager.writer_for(&req.id);
        let transition = writer.lock_owned().await;
        let active_execution = self.execution_manager.get(&req.id).await;
        let task = self.load_task(&req.id).await?;
        let generation_before = task.generation;

        // Idempotent: re-cancelling an already-canceled task is a no-op that
        // returns the current task (matching a2a-go). Other terminal states
        // are still not cancelable, per A2A and a2a-cli SPEC.md §10.4.
        if task.status.state == TaskState::Canceled {
            return Ok(task);
        }
        if task.status.state.is_terminal() {
            return Err(A2AError::task_not_cancelable(&req.id));
        }

        // Atomically transition the task to CANCELED so two concurrent cancel
        // requests cannot both pass the check-then-act window.
        let task = match self.task_store.begin_cancel(&req.id).await {
            Ok(task) => task,
            Err(error) if error.code == error_code::TASK_NOT_CANCELABLE => {
                // Raced with another cancel or with task completion. Keep the
                // same semantics as the upfront check: a task another cancel
                // already moved to CANCELED is returned (idempotent); any
                // other terminal state is still not cancelable.
                let raced = self.load_task(&req.id).await?;
                if raced.status.state == TaskState::Canceled {
                    return Ok(raced);
                }
                return Err(error);
            }
            Err(error) => return Err(error),
        };
        if let Some(active) = &active_execution {
            // The cancellation ends every stream of the task, and its event is
            // published below: what the executor never read is answered before
            // that. (Only now that the transition is certain: a cancel that
            // failed leaves the task, and its input, as they were.)
            reject_unread_inputs_locked(active, &req.id).await;
            // The executor must see the cancellation before it writes again.
            active.set_snapshot(task.clone()).await;
        }
        drop(transition);
        // The task changed under every long-poll waiting on it.
        self.task_changed.notify_waiters();

        let exec_ctx = crate::ExecutorContext {
            message: None,
            task_id: req.id.clone(),
            stored_task: Some(task.clone()),
            context_id: task.context_id.clone(),
            metadata: req.metadata,
            user: None,
            service_params: params.clone(),
            tenant: req.tenant,
            inputs: None,
            new_task: false,
        };
        // A store that advanced the generation recorded the transition itself.
        let recorded_by_store = task.generation > generation_before;
        let outcome = self
            .run_cancel(
                &req.id,
                &task,
                recorded_by_store,
                active_execution.as_ref(),
                exec_ctx,
            )
            .await;

        if let Some(active) = active_execution {
            self.execution_manager.finish(&req.id, &active).await;
        }
        outcome?;

        self.load_task(&req.id).await
    }

    async fn subscribe_to_task(
        &self,
        params: &ServiceParams,
        req: SubscribeToTaskRequest,
    ) -> Result<BoxStream<'static, Result<StreamResponse, A2AError>>, A2AError> {
        self.subscribe_stream(params, req, false).await
    }

    async fn send_live_message(
        &self,
        params: &ServiceParams,
        uplink: BoxStream<'static, StreamRequest>,
    ) -> Result<BoxStream<'static, Result<StreamResponse, A2AError>>, A2AError> {
        let uplink = uplink
            .map(|request| LiveItem {
                request,
                params: None,
                ack: None,
            })
            .boxed();
        self.open_live(params, live::Uplink::Stream(uplink), None)
            .await
    }

    async fn send_live_request(
        &self,
        params: &ServiceParams,
        request: StreamRequest,
    ) -> Result<LiveResponse, A2AError> {
        self.live_request(params, request).await
    }

    async fn create_push_config(
        &self,
        params: &ServiceParams,
        req: TaskPushNotificationConfig,
    ) -> Result<TaskPushNotificationConfig, A2AError> {
        self.authorize(params, Some(&req.task_id))?;
        let mut req = req;
        req.protocol_version
            .get_or_insert_with(|| requested_protocol_version(params));
        self.push_config_store_checked()?.save(req).await
    }

    async fn get_push_config(
        &self,
        params: &ServiceParams,
        req: GetTaskPushNotificationConfigRequest,
    ) -> Result<TaskPushNotificationConfig, A2AError> {
        self.authorize(params, Some(&req.task_id))?;
        self.push_config_store_checked()?
            .get(&req.task_id, &req.id)
            .await
    }

    async fn list_push_configs(
        &self,
        params: &ServiceParams,
        req: ListTaskPushNotificationConfigsRequest,
    ) -> Result<ListTaskPushNotificationConfigsResponse, A2AError> {
        self.authorize(params, Some(&req.task_id))?;
        let mut configs = self.push_config_store_checked()?.list(&req.task_id).await?;
        configs.sort_by(|left, right| left.id.cmp(&right.id));

        // Paging for push configs is done here, not in the store: the store
        // is asked for every config for the task and the page is cut from
        // that. PushConfigStore is public, so this clamp is what bounds the
        // response for an implementation with no per-task limit of its own.
        let page_size = resolve_page_size(req.page_size);
        let start = if let Some(ref token) = req.page_token {
            token
                .parse::<usize>()
                .map_err(|_| A2AError::invalid_params("invalid page token"))?
        } else {
            0
        };
        let start = start.min(configs.len());
        let end = (start + page_size).min(configs.len());
        let next_page_token = (end < configs.len()).then(|| end.to_string());

        Ok(ListTaskPushNotificationConfigsResponse {
            configs: configs[start..end].to_vec(),
            next_page_token,
        })
    }

    async fn delete_push_config(
        &self,
        params: &ServiceParams,
        req: DeleteTaskPushNotificationConfigRequest,
    ) -> Result<(), A2AError> {
        self.authorize(params, Some(&req.task_id))?;
        self.push_config_store_checked()?
            .delete(&req.task_id, &req.id)
            .await
    }

    async fn get_extended_agent_card(
        &self,
        params: &ServiceParams,
        _req: GetExtendedAgentCardRequest,
    ) -> Result<AgentCard, A2AError> {
        self.authorize(params, None)?;
        Self::require_capability(self.capabilities.extended_agent_card, || {
            A2AError::unsupported_operation(
                "the agent card does not declare an extended agent card",
            )
        })?;
        match &self.extended_agent_card {
            Some(resolver) => resolver.resolve(params),
            // A2A defines a code for exactly this condition.
            // UNSUPPORTED_OPERATION would say the agent does not do extended
            // cards at all, when the truth is that this deployment has not
            // configured one — the client can act on that difference.
            None => Err(A2AError::extended_card_not_configured()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::ExecutorContext;
    use crate::pagination::{DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE};
    use crate::push::{InMemoryPushConfigStore, PushConfigStore};
    use crate::task_store::{InMemoryTaskStore, TaskStore, TaskVersion};

    use crate::test_util::install_crypto_provider;
    use axum::{
        Router,
        body::Bytes,
        extract::State,
        http::{HeaderMap, StatusCode, header},
        routing::post,
    };
    use futures::stream;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicI32, Ordering};
    use tokio::{
        net::TcpListener,
        sync::{Notify, mpsc, oneshot},
        time::{Duration, timeout},
    };

    struct EchoExecutor;

    struct PushEventExecutor;

    struct ResumableExecutor {
        release: Arc<Notify>,
    }

    #[derive(Debug)]
    struct CapturedPush {
        authorization: Option<String>,
        notification_token: Option<String>,
        event: StreamResponse,
    }

    impl crate::AgentExecutor for EchoExecutor {
        fn execute(
            &self,
            ctx: ExecutorContext,
        ) -> BoxStream<'static, Result<StreamResponse, A2AError>> {
            let task = Task {
                generation: 0,
                timeline: None,
                id: ctx.task_id.clone(),
                context_id: ctx.context_id.clone(),
                status: TaskStatus {
                    state: TaskState::Completed,
                    message: ctx.message,
                    timestamp: None,
                },
                artifacts: None,
                history: None,
                metadata: None,
            };
            Box::pin(stream::once(async move { Ok(StreamResponse::Task(task)) }))
        }

        fn cancel(
            &self,
            ctx: ExecutorContext,
        ) -> BoxStream<'static, Result<StreamResponse, A2AError>> {
            let task = Task {
                generation: 0,
                timeline: None,
                id: ctx.task_id.clone(),
                context_id: ctx.context_id.clone(),
                status: TaskStatus {
                    state: TaskState::Canceled,
                    message: None,
                    timestamp: None,
                },
                artifacts: None,
                history: None,
                metadata: None,
            };
            Box::pin(stream::once(async move { Ok(StreamResponse::Task(task)) }))
        }
    }

    impl crate::AgentExecutor for PushEventExecutor {
        fn execute(
            &self,
            ctx: ExecutorContext,
        ) -> BoxStream<'static, Result<StreamResponse, A2AError>> {
            let working = StreamResponse::StatusUpdate(TaskStatusUpdateEvent {
                generation: 0,
                task_id: ctx.task_id.clone(),
                context_id: ctx.context_id.clone(),
                status: TaskStatus {
                    state: TaskState::Working,
                    message: None,
                    timestamp: None,
                },
                metadata: None,
            });
            let completed = StreamResponse::Task(Task {
                generation: 0,
                timeline: None,
                id: ctx.task_id.clone(),
                context_id: ctx.context_id.clone(),
                status: TaskStatus {
                    state: TaskState::Completed,
                    message: ctx.message,
                    timestamp: None,
                },
                artifacts: None,
                history: ctx.stored_task.and_then(|task| task.history),
                metadata: None,
            });

            Box::pin(stream::iter(vec![Ok(working), Ok(completed)]))
        }

        fn cancel(
            &self,
            ctx: ExecutorContext,
        ) -> BoxStream<'static, Result<StreamResponse, A2AError>> {
            let task = Task {
                generation: 0,
                timeline: None,
                id: ctx.task_id.clone(),
                context_id: ctx.context_id.clone(),
                status: TaskStatus {
                    state: TaskState::Canceled,
                    message: None,
                    timestamp: None,
                },
                artifacts: None,
                history: None,
                metadata: None,
            };
            Box::pin(stream::once(async move { Ok(StreamResponse::Task(task)) }))
        }
    }

    impl crate::AgentExecutor for ResumableExecutor {
        fn execute(
            &self,
            ctx: ExecutorContext,
        ) -> BoxStream<'static, Result<StreamResponse, A2AError>> {
            let release = self.release.clone();
            let working_task = Task {
                generation: 0,
                timeline: None,
                id: ctx.task_id.clone(),
                context_id: ctx.context_id.clone(),
                status: TaskStatus {
                    state: TaskState::Working,
                    message: None,
                    timestamp: None,
                },
                artifacts: None,
                history: ctx.stored_task.and_then(|task| task.history),
                metadata: None,
            };
            let completed_task = Task {
                generation: 0,
                timeline: None,
                id: working_task.id.clone(),
                context_id: working_task.context_id.clone(),
                status: TaskStatus {
                    state: TaskState::Completed,
                    message: None,
                    timestamp: None,
                },
                artifacts: None,
                history: working_task.history.clone(),
                metadata: None,
            };

            Box::pin(stream::unfold(
                (Some(working_task), Some((release, completed_task))),
                |(working_task, completion)| async move {
                    if let Some(task) = working_task {
                        return Some((Ok(StreamResponse::Task(task)), (None, completion)));
                    }

                    if let Some((release, task)) = completion {
                        release.notified().await;
                        return Some((Ok(StreamResponse::Task(task)), (None, None)));
                    }

                    None
                },
            ))
        }

        fn cancel(
            &self,
            ctx: ExecutorContext,
        ) -> BoxStream<'static, Result<StreamResponse, A2AError>> {
            let task = Task {
                generation: 0,
                timeline: None,
                id: ctx.task_id.clone(),
                context_id: ctx.context_id.clone(),
                status: TaskStatus {
                    state: TaskState::Canceled,
                    message: None,
                    timestamp: None,
                },
                artifacts: None,
                history: None,
                metadata: None,
            };
            Box::pin(stream::once(async move { Ok(StreamResponse::Task(task)) }))
        }
    }

    /// Mirrors itk's own `tck-multi-turn` ACTS behavior: only ever emits a
    /// `StatusUpdate`, never a full `Task`, so a test against it exercises
    /// exactly the path a real executor that never touches `history` relies
    /// on the server to handle.
    struct MultiTurnExecutor;

    impl crate::AgentExecutor for MultiTurnExecutor {
        fn execute(
            &self,
            ctx: ExecutorContext,
        ) -> BoxStream<'static, Result<StreamResponse, A2AError>> {
            let done = ctx
                .message
                .as_ref()
                .and_then(Message::text)
                .is_some_and(|text| text.trim().eq_ignore_ascii_case("done"));
            let state = if done {
                TaskState::Completed
            } else {
                TaskState::InputRequired
            };
            let update = StreamResponse::StatusUpdate(TaskStatusUpdateEvent {
                generation: 0,
                task_id: ctx.task_id.clone(),
                context_id: ctx.context_id.clone(),
                status: TaskStatus {
                    state,
                    message: None,
                    timestamp: None,
                },
                metadata: None,
            });
            Box::pin(stream::once(async move { Ok(update) }))
        }

        fn cancel(
            &self,
            ctx: ExecutorContext,
        ) -> BoxStream<'static, Result<StreamResponse, A2AError>> {
            let task = Task {
                generation: 0,
                timeline: None,
                id: ctx.task_id.clone(),
                context_id: ctx.context_id.clone(),
                status: TaskStatus {
                    state: TaskState::Canceled,
                    message: None,
                    timestamp: None,
                },
                artifacts: None,
                history: None,
                metadata: None,
            };
            Box::pin(stream::once(async move { Ok(StreamResponse::Task(task)) }))
        }
    }

    /// A `TaskStore` that behaves exactly like `InMemoryTaskStore` except
    /// for `list`, which it drives from the fields below.
    ///
    /// `TaskStore` is public, so nothing obliges a third-party
    /// implementation to bound its own response. Only a decorator can model
    /// that; everything else delegates, so a cap test cannot accidentally
    /// depend on stub behaviour.
    struct PagingTaskStore {
        inner: InMemoryTaskStore,
        count: usize,
        /// `true` models a well-behaved store, bounded by the handler
        /// clamping the request; `false` models one that ignores
        /// `page_size`, bounded only by the handler truncating the response.
        honour_page_size: bool,
        /// The `page_size` the handler actually asked for. Recorded because
        /// the response bound alone cannot distinguish a clamped request
        /// from an unclamped one -- both end up 100 items wide.
        seen_page_size: Arc<AtomicI32>,
        /// Continuation token the store reports, so truncation's handling of
        /// it can be observed.
        next_page_token: String,
    }

    impl PagingTaskStore {
        fn new(count: usize, honour_page_size: bool) -> Self {
            Self {
                inner: InMemoryTaskStore::new(),
                count,
                honour_page_size,
                seen_page_size: Arc::new(AtomicI32::new(-1)),
                next_page_token: String::new(),
            }
        }

        fn with_next_page_token(mut self, token: &str) -> Self {
            self.next_page_token = token.to_string();
            self
        }
    }

    #[async_trait]
    impl TaskStore for PagingTaskStore {
        async fn create(&self, task: Task) -> Result<TaskVersion, A2AError> {
            self.inner.create(task).await
        }

        async fn update(&self, task: Task) -> Result<TaskVersion, A2AError> {
            self.inner.update(task).await
        }

        async fn get(&self, task_id: &str) -> Result<Option<Task>, A2AError> {
            self.inner.get(task_id).await
        }

        async fn begin_cancel(&self, task_id: &str) -> Result<Task, A2AError> {
            self.inner.begin_cancel(task_id).await
        }

        async fn list(&self, req: &ListTasksRequest) -> Result<ListTasksResponse, A2AError> {
            self.seen_page_size
                .store(req.page_size.unwrap_or(-1), Ordering::SeqCst);
            let served = if self.honour_page_size {
                req.page_size.unwrap_or(i32::MAX).max(0) as usize
            } else {
                self.count
            }
            .min(self.count);
            let tasks = (0..served)
                .map(|i| Task {
                    generation: 0,
                    timeline: None,
                    id: format!("t-{i:04}"),
                    context_id: "c1".into(),
                    status: TaskStatus {
                        state: TaskState::Completed,
                        message: None,
                        timestamp: None,
                    },
                    artifacts: None,
                    history: None,
                    metadata: None,
                })
                .collect::<Vec<_>>();
            let total = tasks.len() as i32;
            Ok(ListTasksResponse {
                tasks,
                next_page_token: self.next_page_token.clone(),
                page_size: total,
                total_size: total,
            })
        }
    }

    /// The push-config equivalent, and the only way to reach the handler's
    /// page cap on that path: `InMemoryPushConfigStore` caps a task at
    /// `MAX_PUSH_CONFIGS_PER_TASK` (50), below the page cap of 100, so
    /// nothing routed through it can make the clamp bind. Everything but
    /// `list` delegates.
    struct PagingPushConfigStore {
        inner: InMemoryPushConfigStore,
        count: usize,
    }

    impl PagingPushConfigStore {
        fn new(count: usize) -> Self {
            Self {
                inner: InMemoryPushConfigStore::new(),
                count,
            }
        }
    }

    #[async_trait]
    impl crate::PushConfigStore for PagingPushConfigStore {
        async fn save(
            &self,
            config: TaskPushNotificationConfig,
        ) -> Result<TaskPushNotificationConfig, A2AError> {
            self.inner.save(config).await
        }

        async fn get(
            &self,
            task_id: &str,
            config_id: &str,
        ) -> Result<TaskPushNotificationConfig, A2AError> {
            self.inner.get(task_id, config_id).await
        }

        async fn list(&self, task_id: &str) -> Result<Vec<TaskPushNotificationConfig>, A2AError> {
            Ok((0..self.count)
                .map(|i| TaskPushNotificationConfig {
                    protocol_version: None,
                    task_id: task_id.to_string(),
                    url: format!("https://example.com/hook/{i}"),
                    id: Some(format!("cfg-{i:04}")),
                    token: None,
                    authentication: None,
                    tenant: None,
                })
                .collect())
        }

        async fn delete(&self, task_id: &str, config_id: &str) -> Result<(), A2AError> {
            self.inner.delete(task_id, config_id).await
        }

        async fn delete_all(&self, task_id: &str) -> Result<(), A2AError> {
            self.inner.delete_all(task_id).await
        }
    }

    /// Pre-create a task so a message may legitimately reference it: A2A
    /// §3.4.2 forbids client-provided taskIds for *creating* tasks.
    async fn seed_task(handler: &DefaultRequestHandler, task_id: &str, context_id: &str) {
        handler
            .task_store
            .create(Task {
                generation: 0,
                timeline: None,
                id: task_id.into(),
                context_id: context_id.into(),
                status: TaskStatus {
                    state: TaskState::Submitted,
                    message: None,
                    timestamp: None,
                },
                artifacts: None,
                history: None,
                metadata: None,
            })
            .await
            .unwrap();
    }

    fn make_handler() -> DefaultRequestHandler {
        DefaultRequestHandler::new(EchoExecutor, InMemoryTaskStore::new())
    }

    fn make_handler_with_push_configs() -> DefaultRequestHandler {
        DefaultRequestHandler::new(EchoExecutor, InMemoryTaskStore::new())
            .with_push_config_store(InMemoryPushConfigStore::new())
    }

    fn make_push_delivery_handler() -> DefaultRequestHandler {
        // URL validation is disabled because the test webhook runs on
        // 127.0.0.1; the default HttpPushSender rejects loopback targets.
        DefaultRequestHandler::new(PushEventExecutor, InMemoryTaskStore::new())
            .with_push_notifications(
                InMemoryPushConfigStore::new(),
                crate::HttpPushSender::new(Some(crate::HttpPushSenderConfig {
                    validate_urls: false,
                    ..Default::default()
                })),
            )
    }

    fn make_resumable_handler() -> (DefaultRequestHandler, Arc<Notify>) {
        let release = Arc::new(Notify::new());
        (
            DefaultRequestHandler::new(
                ResumableExecutor {
                    release: release.clone(),
                },
                InMemoryTaskStore::new(),
            ),
            release,
        )
    }

    fn make_message() -> Message {
        Message::new(Role::User, vec![Part::text("hello")])
    }

    async fn capture_push(
        State(sender): State<mpsc::UnboundedSender<CapturedPush>>,
        headers: HeaderMap,
        body: Bytes,
    ) -> StatusCode {
        let captured = CapturedPush {
            authorization: headers
                .get(header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                .map(ToOwned::to_owned),
            notification_token: headers
                .get("A2A-Notification-Token")
                .and_then(|value| value.to_str().ok())
                .map(ToOwned::to_owned),
            event: serde_json::from_slice(&body).unwrap(),
        };
        sender.send(captured).unwrap();
        StatusCode::ACCEPTED
    }

    async fn start_push_webhook() -> (
        String,
        mpsc::UnboundedReceiver<CapturedPush>,
        oneshot::Sender<()>,
        tokio::task::JoinHandle<()>,
    ) {
        let (sender, receiver) = mpsc::unbounded_channel();
        let app = Router::new()
            .route("/", post(capture_push))
            .with_state(sender);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = shutdown_rx.await;
                })
                .await
                .unwrap();
        });

        (format!("http://{address}/"), receiver, shutdown_tx, server)
    }

    async fn next_push(receiver: &mut mpsc::UnboundedReceiver<CapturedPush>) -> CapturedPush {
        timeout(Duration::from_secs(5), receiver.recv())
            .await
            .unwrap()
            .unwrap()
    }

    /// A subscriber that finds an execution still being prepared waits for it
    /// and then opens with the task, not with whatever comes next.
    #[tokio::test]
    async fn test_subscribe_waits_for_an_execution_that_is_being_prepared() {
        use futures::StreamExt;

        let handler = Arc::new(make_handler());
        seed_task(&handler, "t-prep", "c-prep").await;
        let (active, guard) = handler.execution_manager.reserve("t-prep").await.unwrap();

        let subscribing = {
            let handler = handler.clone();
            tokio::spawn(async move {
                handler
                    .subscribe_to_task(
                        &ServiceParams::new(),
                        SubscribeToTaskRequest {
                            last_seen_generation: None,
                            id: "t-prep".into(),
                            tenant: None,
                        },
                    )
                    .await
            })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!subscribing.is_finished(), "it waits for the preparation");

        let prepared = handler.task_store.get("t-prep").await.unwrap().unwrap();
        active.set_snapshot(prepared).await;
        drop(guard);

        let mut stream = subscribing.await.unwrap().unwrap();
        assert!(matches!(
            stream.next().await,
            Some(Ok(StreamResponse::Task(_)))
        ));
    }

    /// A cancel executor that restates the canceled task on an idle task is
    /// not announced a second time at the generation it found.
    #[tokio::test]
    async fn test_idle_cancel_does_not_announce_a_restated_snapshot() {
        let handler = make_handler();
        let canceled = Task {
            generation: 3,
            timeline: None,
            id: "t-idle-cancel".into(),
            context_id: "c-idle-cancel".into(),
            status: TaskStatus {
                state: TaskState::Canceled,
                message: None,
                timestamp: None,
            },
            artifacts: None,
            history: None,
            metadata: None,
        };
        let mut current = Some(canceled.clone());
        let mut event = StreamResponse::Task(canceled);

        let published = handler
            .apply_cancel_event(
                None,
                &mut current,
                &mut CancelEntry {
                    recorded_by_store: false,
                    generation: 3,
                    announced: false,
                },
                &mut event,
            )
            .await
            .unwrap();

        assert!(
            !published,
            "a snapshot that changes nothing is not published"
        );
        assert_eq!(current.unwrap().generation, 3);
    }

    /// A reservation whose request was dropped (its release still on its way)
    /// is replaced by the next request instead of being mistaken for a running
    /// execution.
    #[tokio::test]
    async fn test_reserve_replaces_a_dropped_requests_leftover() {
        let manager = ExecutionManager::default();
        let (stale, guard) = manager.reserve("t-stale").await.unwrap();
        drop(guard); // the request goes away; nothing released its entry

        let (fresh, _guard) = manager
            .reserve("t-stale")
            .await
            .expect("the leftover is not a running execution");

        assert!(!Arc::ptr_eq(&stale, &fresh));
        let registered = manager.get("t-stale").await.unwrap();
        assert!(Arc::ptr_eq(&registered, &fresh));
        // The old guard's late release must not remove the new reservation.
        manager.finish("t-stale", &stale).await;
        assert!(manager.get("t-stale").await.is_some());
    }

    /// A subscriber that finds such a leftover does not wait for it or spin on
    /// it: the task is simply not executing.
    #[tokio::test]
    async fn test_subscribe_ignores_a_dropped_requests_leftover() {
        let handler = make_handler();
        seed_task(&handler, "t-leftover", "c-leftover").await;
        let (_stale, guard) = handler
            .execution_manager
            .reserve("t-leftover")
            .await
            .unwrap();
        drop(guard);

        let outcome = tokio::time::timeout(
            Duration::from_secs(2),
            handler.subscribe_to_task(
                &ServiceParams::new(),
                SubscribeToTaskRequest {
                    last_seen_generation: None,
                    id: "t-leftover".into(),
                    tenant: None,
                },
            ),
        )
        .await
        .expect("the subscription neither hangs nor spins");

        // Idle and not terminal: refused, as when nothing was reserved at all.
        assert_eq!(
            outcome.err().map(|e| e.code),
            Some(error_code::TASK_NOT_FOUND)
        );
    }

    #[tokio::test]
    async fn test_send_message_creates_task() {
        let handler = make_handler();
        let params = ServiceParams::new();
        let req = SendMessageRequest {
            artifact_updates: Vec::new(),
            message: make_message(),
            configuration: None,
            metadata: None,
            tenant: None,
        };
        let result = handler.send_message(&params, req).await.unwrap();
        match result {
            SendMessageResponse::Task(task) => {
                assert_eq!(task.status.state, TaskState::Completed);
            }
            _ => panic!("expected Task response"),
        }
    }

    #[tokio::test]
    async fn test_send_streaming_message() {
        use futures::StreamExt;
        let handler = make_handler();
        let params = ServiceParams::new();
        let req = SendMessageRequest {
            artifact_updates: Vec::new(),
            message: make_message(),
            configuration: None,
            metadata: None,
            tenant: None,
        };
        let mut stream = handler.send_streaming_message(&params, req).await.unwrap();
        let event = stream.next().await.unwrap().unwrap();
        assert!(matches!(event, StreamResponse::Task(_)));
    }

    /// DM-SERIAL-001: `save_task`'s backfill only covers what gets persisted
    /// -- a live subscriber receives the broadcast event directly, never a
    /// reload from the store, so the timestamp must also be backfilled on
    /// the event itself before it is published.
    #[tokio::test]
    async fn test_streamed_task_event_has_a_backfilled_timestamp() {
        use futures::StreamExt;
        let handler = make_handler();
        let params = ServiceParams::new();
        let req = SendMessageRequest {
            artifact_updates: Vec::new(),
            message: make_message(),
            configuration: None,
            metadata: None,
            tenant: None,
        };
        let mut stream = handler.send_streaming_message(&params, req).await.unwrap();
        let event = stream.next().await.unwrap().unwrap();
        match event {
            StreamResponse::Task(task) => assert!(
                task.status.timestamp.is_some(),
                "a live-streamed task event must carry a backfilled timestamp"
            ),
            other => panic!("unexpected event: {other:?}"),
        }
    }

    /// `ArtifactUpdate` and `Message` carry no status of their own, so the
    /// backfill must leave them alone rather than panic on the missing arm.
    #[test]
    fn test_backfill_status_timestamp_ignores_events_without_a_status() {
        let mut artifact_update = StreamResponse::ArtifactUpdate(TaskArtifactUpdateEvent {
            generation: 0,
            task_id: "t1".into(),
            context_id: "c1".into(),
            artifact: Artifact {
                end_generation: None,
                role: None,
                start_generation: None,
                artifact_id: "a1".into(),
                name: None,
                description: None,
                parts: vec![Part::text("payload")],
                metadata: None,
                extensions: None,
            },
            append: None,
            last_chunk: None,
            metadata: None,
        });
        let before = artifact_update.clone();
        backfill_status_timestamp(&mut artifact_update);
        assert_eq!(artifact_update, before);

        let mut message = StreamResponse::Message(make_message());
        let before = message.clone();
        backfill_status_timestamp(&mut message);
        assert_eq!(message, before);
    }

    /// DM-SERIAL-001: a status update whose status omits a timestamp must be
    /// backfilled before the task is persisted, so a returned task never
    /// carries a timestamp-less status.
    #[tokio::test]
    async fn test_status_update_backfills_missing_timestamp() {
        let store = InMemoryTaskStore::new();
        store
            .create(Task {
                generation: 0,
                timeline: None,
                id: "t-backfill".into(),
                context_id: "ctx".into(),
                status: TaskStatus {
                    state: TaskState::Submitted,
                    message: None,
                    timestamp: Some(chrono::Utc::now()),
                },
                artifacts: None,
                history: None,
                metadata: None,
            })
            .await
            .unwrap();

        let updated = apply_event_to_task(
            &store,
            None,
            &mut StreamResponse::StatusUpdate(TaskStatusUpdateEvent {
                generation: 0,
                task_id: "t-backfill".into(),
                context_id: "ctx".into(),
                status: TaskStatus {
                    state: TaskState::Working,
                    message: None,
                    timestamp: None,
                },
                metadata: None,
            }),
        )
        .await
        .unwrap()
        .expect("status update yields a task");

        assert_eq!(updated.status.state, TaskState::Working);
        assert!(
            updated.status.timestamp.is_some(),
            "missing status timestamp must be backfilled"
        );
    }

    /// A timestamp supplied by the executor is authoritative and must be kept
    /// verbatim, not overwritten by the backfill.
    #[tokio::test]
    async fn test_status_update_preserves_supplied_timestamp() {
        let store = InMemoryTaskStore::new();
        store
            .create(Task {
                generation: 0,
                timeline: None,
                id: "t-preserve".into(),
                context_id: "ctx".into(),
                status: TaskStatus {
                    state: TaskState::Submitted,
                    message: None,
                    timestamp: None,
                },
                artifacts: None,
                history: None,
                metadata: None,
            })
            .await
            .unwrap();

        let supplied = chrono::DateTime::parse_from_rfc3339("2020-01-02T03:04:05Z")
            .unwrap()
            .with_timezone(&chrono::Utc);

        let updated = apply_event_to_task(
            &store,
            None,
            &mut StreamResponse::StatusUpdate(TaskStatusUpdateEvent {
                generation: 0,
                task_id: "t-preserve".into(),
                context_id: "ctx".into(),
                status: TaskStatus {
                    state: TaskState::Working,
                    message: None,
                    timestamp: Some(supplied),
                },
                metadata: None,
            }),
        )
        .await
        .unwrap()
        .expect("status update yields a task");

        assert_eq!(updated.status.timestamp, Some(supplied));
    }

    /// The task-snapshot arm persists through the same `save_task` chokepoint,
    /// so an executor `Task` event whose status omits a timestamp must also be
    /// backfilled (DM-SERIAL-001).
    #[tokio::test]
    async fn test_task_snapshot_backfills_missing_timestamp() {
        let store = InMemoryTaskStore::new();

        let returned = apply_event_to_task(
            &store,
            None,
            &mut StreamResponse::Task(Task {
                generation: 0,
                timeline: None,
                id: "t-snapshot".into(),
                context_id: "ctx".into(),
                status: TaskStatus {
                    state: TaskState::Completed,
                    message: None,
                    timestamp: None,
                },
                artifacts: None,
                history: None,
                metadata: None,
            }),
        )
        .await
        .unwrap()
        .expect("task event yields a task");

        assert!(
            returned.status.timestamp.is_some(),
            "missing status timestamp must be backfilled on the task-snapshot path"
        );
        let persisted = store.get("t-snapshot").await.unwrap().unwrap();
        assert!(
            persisted.status.timestamp.is_some(),
            "the persisted task must carry a timestamp"
        );
    }

    /// A timestamp already present on a task snapshot is authoritative and
    /// must survive the persistence chokepoint unchanged.
    #[tokio::test]
    async fn test_task_snapshot_preserves_supplied_timestamp() {
        let store = InMemoryTaskStore::new();

        let supplied = chrono::DateTime::parse_from_rfc3339("2021-06-07T08:09:10Z")
            .unwrap()
            .with_timezone(&chrono::Utc);

        let returned = apply_event_to_task(
            &store,
            None,
            &mut StreamResponse::Task(Task {
                generation: 0,
                timeline: None,
                id: "t-snapshot-kept".into(),
                context_id: "ctx".into(),
                status: TaskStatus {
                    state: TaskState::Completed,
                    message: None,
                    timestamp: Some(supplied),
                },
                artifacts: None,
                history: None,
                metadata: None,
            }),
        )
        .await
        .unwrap()
        .expect("task event yields a task");

        assert_eq!(returned.status.timestamp, Some(supplied));
        let persisted = store.get("t-snapshot-kept").await.unwrap().unwrap();
        assert_eq!(persisted.status.timestamp, Some(supplied));
    }

    #[tokio::test]
    async fn test_stream_disconnect_preserves_execution_for_resubscription() {
        use futures::StreamExt;

        let (handler, release) = make_resumable_handler();
        let params = ServiceParams::new();
        let mut message = make_message();
        message.task_id = Some("t-disconnect".into());
        message.context_id = Some("c-disconnect".into());
        seed_task(&handler, "t-disconnect", "c-disconnect").await;

        let mut stream = handler
            .send_streaming_message(
                &params,
                SendMessageRequest {
                    artifact_updates: Vec::new(),
                    message,
                    configuration: None,
                    metadata: None,
                    tenant: None,
                },
            )
            .await
            .unwrap();

        let initial = stream.next().await.unwrap().unwrap();
        match initial {
            StreamResponse::Task(task) => assert_eq!(task.status.state, TaskState::Working),
            _ => panic!("expected initial working task"),
        }
        drop(stream);

        let mut resumed = handler
            .subscribe_to_task(
                &params,
                SubscribeToTaskRequest {
                    last_seen_generation: None,
                    id: "t-disconnect".into(),
                    tenant: None,
                },
            )
            .await
            .unwrap();

        let snapshot = resumed.next().await.unwrap().unwrap();
        match snapshot {
            StreamResponse::Task(task) => assert_eq!(task.status.state, TaskState::Working),
            _ => panic!("expected working task snapshot"),
        }

        release.notify_waiters();

        let terminal = resumed.next().await.unwrap().unwrap();
        match terminal {
            StreamResponse::Task(task) => {
                assert_eq!(task.status.state, TaskState::Completed)
            }
            _ => panic!("expected terminal task"),
        }
        assert!(resumed.next().await.is_none());
    }

    #[tokio::test]
    async fn test_active_execution_keeps_snapshot_without_subscribers() {
        let task = Task {
            generation: 0,
            timeline: None,
            id: "t-buffering".into(),
            context_id: "c-buffering".into(),
            status: TaskStatus {
                state: TaskState::Working,
                message: None,
                timestamp: None,
            },
            artifacts: None,
            history: None,
            metadata: None,
        };
        let active = ActiveExecution::new(Some(task.clone()));

        let event = StreamResponse::Task(task.clone());
        active.publish(Ok(&event), Some(task)).await;

        let (mut receiver, snapshot_task, sequence) = active.resubscribe().await;
        assert_eq!(snapshot_task.unwrap().id, "t-buffering");
        assert_eq!(sequence, 1);
        assert!(receiver.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_active_execution_publishes_errors_to_subscribers() {
        let task = Task {
            generation: 0,
            timeline: None,
            id: "t-buffering-error".into(),
            context_id: "c-buffering-error".into(),
            status: TaskStatus {
                state: TaskState::Working,
                message: None,
                timestamp: None,
            },
            artifacts: None,
            history: None,
            metadata: None,
        };
        let active = ActiveExecution::new(Some(task));
        let mut receiver = active.subscribe();
        let error = A2AError::internal("execution failed");

        active.publish(Err(&error), None).await;

        let event = receiver.recv().await.unwrap();
        assert_eq!(event.sequence, 1);
        assert_eq!(event.result.unwrap_err().code, error.code);
    }

    #[tokio::test]
    async fn test_active_execution_drops_errors_without_subscribers() {
        let task = Task {
            generation: 0,
            timeline: None,
            id: "t-error-no-sub".into(),
            context_id: "c-error-no-sub".into(),
            status: TaskStatus {
                state: TaskState::Working,
                message: None,
                timestamp: None,
            },
            artifacts: None,
            history: None,
            metadata: None,
        };
        let active = ActiveExecution::new(Some(task.clone()));
        let error = A2AError::internal("execution failed");

        // Publish error without any subscribers
        active.publish(Err(&error), Some(task.clone())).await;

        // Resubscribe to verify snapshot was updated but no event was buffered
        let (mut receiver, snapshot_task, sequence) = active.resubscribe().await;
        assert_eq!(snapshot_task.unwrap().id, "t-error-no-sub");
        assert_eq!(sequence, 1);
        assert!(receiver.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_active_execution_publishes_events_with_snapshot_updates() {
        let initial_task = Task {
            generation: 0,
            timeline: None,
            id: "t-snapshot-update".into(),
            context_id: "c-snapshot-update".into(),
            status: TaskStatus {
                state: TaskState::Working,
                message: None,
                timestamp: None,
            },
            artifacts: None,
            history: None,
            metadata: None,
        };
        let active = ActiveExecution::new(Some(initial_task.clone()));
        let mut receiver = active.subscribe();

        let updated_task = Task {
            generation: 0,
            timeline: None,
            id: "t-snapshot-update".into(),
            context_id: "c-snapshot-update".into(),
            status: TaskStatus {
                state: TaskState::Working,
                message: None,
                timestamp: None,
            },
            artifacts: None,
            history: None,
            metadata: None,
        };

        let event = StreamResponse::Task(updated_task.clone());
        active.publish(Ok(&event), Some(updated_task.clone())).await;

        // Verify event was published to subscriber
        let published = receiver.recv().await.unwrap();
        assert_eq!(published.sequence, 1);
        match published.result {
            Ok(StreamResponse::Task(task)) => {
                assert_eq!(task.id, "t-snapshot-update");
            }
            _ => panic!("expected Ok(Task) event"),
        }

        // Verify snapshot was updated
        let (_, snapshot_task, sequence) = active.resubscribe().await;
        assert_eq!(snapshot_task.unwrap().id, "t-snapshot-update");
        assert_eq!(sequence, 1);
    }

    #[tokio::test]
    async fn test_active_execution_broadcasts_to_multiple_subscribers() {
        let task = Task {
            generation: 0,
            timeline: None,
            id: "t-multi-sub".into(),
            context_id: "c-multi-sub".into(),
            status: TaskStatus {
                state: TaskState::Working,
                message: None,
                timestamp: None,
            },
            artifacts: None,
            history: None,
            metadata: None,
        };
        let active = ActiveExecution::new(Some(task.clone()));

        // Create two subscribers
        let mut sub1 = active.subscribe();
        let mut sub2 = active.subscribe();

        let event = StreamResponse::Task(task);
        active.publish(Ok(&event), None).await;

        // Both subscribers should receive the same event
        let evt1 = sub1.recv().await.unwrap();
        let evt2 = sub2.recv().await.unwrap();

        assert_eq!(evt1.sequence, 1);
        assert_eq!(evt2.sequence, 1);
        assert!(matches!(evt1.result, Ok(StreamResponse::Task(_))));
        assert!(matches!(evt2.result, Ok(StreamResponse::Task(_))));
    }

    #[tokio::test]
    async fn test_get_task_not_found() {
        let handler = make_handler();
        let params = ServiceParams::new();
        let req = GetTaskRequest {
            current_generation: None,
            timeline_length: None,
            id: "nonexistent".into(),
            history_length: None,
            tenant: None,
        };
        let result = handler.get_task(&params, req).await;
        assert!(result.is_err());
    }

    // A2A §3.4.2 / §3.4.3: taskId and contextId rules on send_message (#248).

    fn send_req(task_id: Option<&str>, context_id: Option<&str>) -> SendMessageRequest {
        let mut msg = make_message();
        msg.task_id = task_id.map(Into::into);
        msg.context_id = context_id.map(Into::into);
        SendMessageRequest {
            artifact_updates: Vec::new(),
            message: msg,
            configuration: None,
            metadata: None,
            tenant: None,
        }
    }

    #[tokio::test]
    async fn test_send_message_rejects_unsupported_content_type() {
        install_crypto_provider();
        let handler = make_handler().with_default_input_modes(vec!["text/plain".into()]);
        let mut req = send_req(None, None);
        req.message.parts = vec![
            Part::data(serde_json::json!({"value": "unsupported"}))
                .with_media_type("application/x-unsupported-type-12345"),
        ];
        let err = handler
            .send_message(&ServiceParams::new(), req)
            .await
            .expect_err("an undeclared content type must be rejected");
        assert_eq!(err.code, error_code::CONTENT_TYPE_NOT_SUPPORTED);
    }

    #[tokio::test]
    async fn test_send_message_accepts_a_declared_content_type() {
        install_crypto_provider();
        let handler = make_handler().with_default_input_modes(vec!["text/plain".into()]);
        let mut req = send_req(None, None);
        req.message.parts = vec![Part::text("hello").with_media_type("text/plain")];
        handler
            .send_message(&ServiceParams::new(), req)
            .await
            .expect("a declared content type must be accepted");
    }

    #[tokio::test]
    async fn test_send_message_allows_any_content_type_when_none_declared() {
        install_crypto_provider();
        let handler = make_handler();
        let mut req = send_req(None, None);
        req.message.parts = vec![
            Part::data(serde_json::json!({"value": "x"})).with_media_type("application/x-anything"),
        ];
        handler
            .send_message(&ServiceParams::new(), req)
            .await
            .expect("no declared modes means no restriction");
    }

    #[tokio::test]
    async fn test_send_message_unknown_task_id_is_not_found() {
        install_crypto_provider();
        let handler = make_handler();
        // §3.4.2: a client-provided taskId MUST reference an existing task, and
        // MUST NOT create one. Silently minting a task here is what let a
        // mistyped or replayed id start a fresh conversation.
        let err = handler
            .send_message(&ServiceParams::new(), send_req(Some("t-absent"), None))
            .await
            .expect_err("an unknown taskId must not create a task");
        assert_eq!(err.code, error_code::TASK_NOT_FOUND);
        assert!(
            handler.task_store.get("t-absent").await.unwrap().is_none(),
            "the rejected id must not have been created"
        );
    }

    #[tokio::test]
    async fn test_send_message_to_terminal_task_is_rejected() {
        install_crypto_provider();
        let handler = make_handler();
        handler
            .task_store
            .create(Task {
                generation: 0,
                timeline: None,
                id: "t-done".into(),
                context_id: "c-done".into(),
                status: TaskStatus {
                    state: TaskState::Completed,
                    message: None,
                    timestamp: None,
                },
                artifacts: None,
                history: None,
                metadata: None,
            })
            .await
            .unwrap();
        let err = handler
            .send_message(&ServiceParams::new(), send_req(Some("t-done"), None))
            .await
            .expect_err("a follow-up to a terminal task must not be accepted");
        assert_eq!(err.code, error_code::UNSUPPORTED_OPERATION);
    }

    #[tokio::test]
    async fn test_send_message_mismatched_context_id_is_rejected() {
        install_crypto_provider();
        let handler = make_handler();
        seed_task(&handler, "t-ctx", "c-real").await;
        // §3.4.3: previously the stored contextId silently won, so a client
        // addressing the wrong conversation was never told.
        let err = handler
            .send_message(
                &ServiceParams::new(),
                send_req(Some("t-ctx"), Some("c-wrong")),
            )
            .await
            .expect_err("a mismatching contextId must be rejected");
        assert_eq!(err.code, error_code::INVALID_PARAMS);
        assert!(
            err.message.contains("c-wrong"),
            "error names the bad id: {}",
            err.message
        );
    }

    #[tokio::test]
    async fn test_send_message_matching_context_id_is_accepted() {
        install_crypto_provider();
        let handler = make_handler();
        seed_task(&handler, "t-ok", "c-ok").await;
        handler
            .send_message(&ServiceParams::new(), send_req(Some("t-ok"), Some("c-ok")))
            .await
            .expect("a matching pair must be accepted");
    }

    #[tokio::test]
    async fn test_send_message_accumulates_history_across_turns() {
        // ACTS CORE-HIST-005/006: a multi-turn conversation's history must
        // include every turn's message, not just the one that created the
        // task. MultiTurnExecutor only ever emits a StatusUpdate, so this
        // exercises prepare_task_for_execution's continuation path exactly
        // as a real executor (which never touches history itself) does.
        install_crypto_provider();
        let handler = DefaultRequestHandler::new(MultiTurnExecutor, InMemoryTaskStore::new());

        let mut turn1 = make_message();
        turn1.parts = vec![Part::text("turn one")];
        let resp1 = handler
            .send_message(
                &ServiceParams::new(),
                SendMessageRequest {
                    artifact_updates: Vec::new(),
                    message: turn1,
                    configuration: None,
                    metadata: None,
                    tenant: None,
                },
            )
            .await
            .unwrap();
        let task_id = match resp1 {
            SendMessageResponse::Task(t) => t.id,
            SendMessageResponse::Message(_) => panic!("expected a task"),
        };

        for text in ["turn two", "done"] {
            let mut turn = make_message();
            turn.task_id = Some(task_id.clone());
            turn.parts = vec![Part::text(text)];
            handler
                .send_message(
                    &ServiceParams::new(),
                    SendMessageRequest {
                        artifact_updates: Vec::new(),
                        message: turn,
                        configuration: None,
                        metadata: None,
                        tenant: None,
                    },
                )
                .await
                .unwrap();
        }

        let task = handler.task_store.get(&task_id).await.unwrap().unwrap();
        let history = task.history.expect("history must accumulate across turns");
        let texts: Vec<_> = history.iter().map(|m| m.text()).collect();
        assert_eq!(
            texts,
            vec![Some("turn one"), Some("turn two"), Some("done")]
        );
    }

    #[tokio::test]
    async fn test_send_message_applies_configuration_history_length_to_its_own_response() {
        // ACTS CORE-HIST-003: configuration.historyLength must govern
        // SendMessage's own inline task the same way history_length governs
        // a later GetTask, rather than always including the just-sent
        // message regardless of what was asked for.
        install_crypto_provider();
        let handler = DefaultRequestHandler::new(MultiTurnExecutor, InMemoryTaskStore::new());
        let mut message = make_message();
        message.parts = vec![Part::text("done")];
        let resp = handler
            .send_message(
                &ServiceParams::new(),
                SendMessageRequest {
                    artifact_updates: Vec::new(),
                    message,
                    configuration: Some(SendMessageConfiguration {
                        if_generation_match: None,
                        timeline_length: None,
                        accepted_output_modes: None,
                        task_push_notification_config: None,
                        history_length: Some(0),
                        return_immediately: None,
                    }),
                    metadata: None,
                    tenant: None,
                },
            )
            .await
            .unwrap();
        let task = match resp {
            SendMessageResponse::Task(t) => t,
            SendMessageResponse::Message(_) => panic!("expected a task"),
        };
        assert!(
            task.history.is_none_or(|h| h.is_empty()),
            "historyLength=0 must clear history on SendMessage's own response"
        );
    }

    #[tokio::test]
    async fn test_send_message_infers_context_id_from_the_task() {
        install_crypto_provider();
        let handler = make_handler();
        seed_task(&handler, "t-infer", "c-infer").await;
        // §3.4.3: contextId MUST be inferred when only taskId is given.
        handler
            .send_message(&ServiceParams::new(), send_req(Some("t-infer"), None))
            .await
            .unwrap();
        let task = handler.task_store.get("t-infer").await.unwrap().unwrap();
        assert_eq!(task.context_id, "c-infer");
    }

    #[tokio::test]
    async fn test_send_message_without_task_id_still_creates_one() {
        install_crypto_provider();
        let handler = make_handler();
        // The server generates the id. A contextId alone is allowed and starts
        // a new task within that conversation.
        let resp = handler
            .send_message(&ServiceParams::new(), send_req(None, Some("c-new")))
            .await
            .expect("a message with no taskId must still create a task");
        let _ = resp;
        let listed = handler
            .task_store
            .list(&ListTasksRequest {
                timeline_length: None,
                context_id: Some("c-new".into()),
                status: None,
                page_size: None,
                page_token: None,
                history_length: None,
                include_artifacts: None,
                status_timestamp_after: None,
                tenant: None,
            })
            .await
            .unwrap();
        assert_eq!(listed.tasks.len(), 1);
        assert_eq!(listed.tasks[0].context_id, "c-new");
    }

    #[tokio::test]
    async fn test_get_task_after_send() {
        let handler = make_handler();
        let params = ServiceParams::new();
        let mut msg = make_message();
        msg.task_id = Some("t1".into());
        msg.context_id = Some("c1".into());
        seed_task(&handler, "t1", "c1").await;
        let req = SendMessageRequest {
            artifact_updates: Vec::new(),
            message: msg,
            configuration: None,
            metadata: None,
            tenant: None,
        };
        handler.send_message(&params, req).await.unwrap();
        let task = handler
            .get_task(
                &params,
                GetTaskRequest {
                    current_generation: None,
                    timeline_length: None,
                    id: "t1".into(),
                    history_length: None,
                    tenant: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(task.id, "t1");
    }

    #[tokio::test]
    async fn test_list_tasks() {
        let handler = make_handler();
        let params = ServiceParams::new();
        let req = ListTasksRequest {
            timeline_length: None,
            context_id: None,
            status: None,
            page_size: None,
            page_token: None,
            history_length: None,
            status_timestamp_after: None,
            include_artifacts: None,
            tenant: None,
        };
        let resp = handler.list_tasks(&params, req).await.unwrap();
        assert!(resp.tasks.is_empty());
    }

    #[tokio::test]
    async fn test_cancel_task_not_found() {
        let handler = make_handler();
        let params = ServiceParams::new();
        let req = CancelTaskRequest {
            id: "nonexistent".into(),
            metadata: None,
            tenant: None,
        };
        let result = handler.cancel_task(&params, req).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_cancel_task_working() {
        let handler = make_handler();
        let params = ServiceParams::new();
        // First create a task via the store directly
        let task = Task {
            generation: 0,
            timeline: None,
            id: "t2".into(),
            context_id: "c2".into(),
            status: TaskStatus {
                state: TaskState::Working,
                message: None,
                timestamp: None,
            },
            artifacts: None,
            history: None,
            metadata: None,
        };
        handler.task_store.create(task).await.unwrap();
        let req = CancelTaskRequest {
            id: "t2".into(),
            metadata: None,
            tenant: None,
        };
        let result = handler.cancel_task(&params, req).await.unwrap();
        assert_eq!(result.status.state, TaskState::Canceled);
    }

    #[tokio::test]
    async fn test_cancel_task_concurrent_both_succeed_idempotently() {
        let handler = Arc::new(make_handler());
        let params = ServiceParams::new();
        let task = Task {
            generation: 0,
            timeline: None,
            id: "t-race".into(),
            context_id: "c-race".into(),
            status: TaskStatus {
                state: TaskState::Working,
                message: None,
                timestamp: None,
            },
            artifacts: None,
            history: None,
            metadata: None,
        };
        handler.task_store.create(task).await.unwrap();

        let handlers = vec![Arc::clone(&handler), Arc::clone(&handler)];
        let mut results = Vec::new();
        for handler in handlers {
            let params = params.clone();
            let req = CancelTaskRequest {
                id: "t-race".into(),
                metadata: None,
                tenant: None,
            };
            results.push(tokio::spawn(async move {
                handler.cancel_task(&params, req).await
            }));
        }
        for result in results {
            let task = result.await.unwrap().unwrap();
            assert_eq!(task.status.state, TaskState::Canceled);
        }
    }

    #[tokio::test]
    async fn test_cancel_task_already_completed() {
        let handler = make_handler();
        let params = ServiceParams::new();
        let task = Task {
            generation: 0,
            timeline: None,
            id: "t3".into(),
            context_id: "c3".into(),
            status: TaskStatus {
                state: TaskState::Completed,
                message: None,
                timestamp: None,
            },
            artifacts: None,
            history: None,
            metadata: None,
        };
        handler.task_store.create(task).await.unwrap();
        let req = CancelTaskRequest {
            id: "t3".into(),
            metadata: None,
            tenant: None,
        };
        let result = handler.cancel_task(&params, req).await;
        match result {
            Ok(_) => panic!("expected cancel of COMPLETED to fail"),
            Err(error) => assert_eq!(error.code, error_code::TASK_NOT_CANCELABLE),
        }
    }

    #[tokio::test]
    async fn test_cancel_task_already_canceled_is_idempotent() {
        let handler = make_handler();
        let params = ServiceParams::new();
        let task = Task {
            generation: 0,
            timeline: None,
            id: "t-canceled".into(),
            context_id: "c-canceled".into(),
            status: TaskStatus {
                state: TaskState::Canceled,
                message: None,
                timestamp: None,
            },
            artifacts: None,
            history: None,
            metadata: None,
        };
        handler.task_store.create(task).await.unwrap();
        let req = CancelTaskRequest {
            id: "t-canceled".into(),
            metadata: None,
            tenant: None,
        };
        let result = handler.cancel_task(&params, req).await.unwrap();
        assert_eq!(result.status.state, TaskState::Canceled);
    }

    #[tokio::test]
    async fn test_cancel_task_failed_and_rejected_are_not_cancelable() {
        let handler = make_handler();
        let params = ServiceParams::new();
        for (id, state) in [
            ("t-failed", TaskState::Failed),
            ("t-rejected", TaskState::Rejected),
        ] {
            handler
                .task_store
                .create(Task {
                    generation: 0,
                    timeline: None,
                    id: id.into(),
                    context_id: format!("c-{id}"),
                    status: TaskStatus {
                        state,
                        message: None,
                        timestamp: None,
                    },
                    artifacts: None,
                    history: None,
                    metadata: None,
                })
                .await
                .unwrap();
            let result = handler
                .cancel_task(
                    &params,
                    CancelTaskRequest {
                        id: id.into(),
                        metadata: None,
                        tenant: None,
                    },
                )
                .await;
            match result {
                Ok(_) => panic!("expected cancel of {id} to fail"),
                Err(error) => assert_eq!(error.code, error_code::TASK_NOT_CANCELABLE),
            }
        }
    }

    fn task_with_history(id: &str, context_id: &str, n: usize) -> Task {
        Task {
            generation: 0,
            timeline: None,
            id: id.into(),
            context_id: context_id.into(),
            status: TaskStatus {
                state: TaskState::Completed,
                message: None,
                timestamp: None,
            },
            artifacts: None,
            history: Some(
                (0..n)
                    .map(|i| Message::new(Role::User, vec![Part::text(format!("m{i}"))]))
                    .collect(),
            ),
            metadata: None,
        }
    }

    #[tokio::test]
    async fn test_get_task_applies_history_length() {
        let handler = make_handler();
        let params = ServiceParams::new();
        handler
            .task_store
            .create(task_with_history("hist", "c", 3))
            .await
            .unwrap();

        let full = handler
            .get_task(
                &params,
                GetTaskRequest {
                    current_generation: None,
                    timeline_length: None,
                    id: "hist".into(),
                    history_length: None,
                    tenant: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(full.history.as_ref().unwrap().len(), 3);

        let last = handler
            .get_task(
                &params,
                GetTaskRequest {
                    current_generation: None,
                    timeline_length: None,
                    id: "hist".into(),
                    history_length: Some(1),
                    tenant: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(last.history.as_ref().unwrap().len(), 1);

        for hl in [0, -1] {
            let empty = handler
                .get_task(
                    &params,
                    GetTaskRequest {
                        current_generation: None,
                        timeline_length: None,
                        id: "hist".into(),
                        history_length: Some(hl),
                        tenant: None,
                    },
                )
                .await
                .unwrap();
            assert!(
                empty.history.as_ref().unwrap().is_empty(),
                "history_length={hl} must clear history"
            );
        }
    }

    fn reject_unauthenticated(
        _params: &ServiceParams,
        _task_id: Option<&str>,
    ) -> Result<(), A2AError> {
        Err(A2AError::invalid_request("authentication required"))
    }

    #[tokio::test]
    async fn test_authorizer_rejects_handler_methods() {
        let handler = DefaultRequestHandler::new(EchoExecutor, InMemoryTaskStore::new())
            .with_authorizer(reject_unauthenticated)
            .with_push_config_store(InMemoryPushConfigStore::new());
        let params = ServiceParams::new();

        assert!(
            handler
                .send_message(
                    &params,
                    SendMessageRequest {
                        artifact_updates: Vec::new(),
                        message: make_message(),
                        configuration: None,
                        metadata: None,
                        tenant: None,
                    },
                )
                .await
                .is_err()
        );
        assert!(
            handler
                .get_task(
                    &params,
                    GetTaskRequest {
                        current_generation: None,
                        timeline_length: None,
                        id: "t".into(),
                        history_length: None,
                        tenant: None,
                    },
                )
                .await
                .is_err()
        );
        assert!(
            handler
                .list_tasks(
                    &params,
                    ListTasksRequest {
                        timeline_length: None,
                        context_id: None,
                        status: None,
                        page_size: None,
                        page_token: None,
                        history_length: None,
                        status_timestamp_after: None,
                        include_artifacts: None,
                        tenant: None,
                    },
                )
                .await
                .is_err()
        );
        assert!(
            handler
                .cancel_task(
                    &params,
                    CancelTaskRequest {
                        id: "t".into(),
                        metadata: None,
                        tenant: None,
                    },
                )
                .await
                .is_err()
        );
        assert!(
            handler
                .subscribe_to_task(
                    &params,
                    SubscribeToTaskRequest {
                        last_seen_generation: None,
                        id: "t".into(),
                        tenant: None,
                    },
                )
                .await
                .is_err()
        );
        assert!(
            handler
                .create_push_config(
                    &params,
                    TaskPushNotificationConfig {
                        protocol_version: None,
                        url: "https://example.com/push".into(),
                        id: None,
                        task_id: "t".into(),
                        token: None,
                        authentication: None,
                        tenant: None,
                    },
                )
                .await
                .is_err()
        );
        assert!(
            handler
                .get_push_config(
                    &params,
                    GetTaskPushNotificationConfigRequest {
                        task_id: "t".into(),
                        id: "cfg".into(),
                        tenant: None,
                    },
                )
                .await
                .is_err()
        );
        assert!(
            handler
                .list_push_configs(
                    &params,
                    ListTaskPushNotificationConfigsRequest {
                        task_id: "t".into(),
                        page_size: None,
                        page_token: None,
                        tenant: None,
                    },
                )
                .await
                .is_err()
        );
        assert!(
            handler
                .delete_push_config(
                    &params,
                    DeleteTaskPushNotificationConfigRequest {
                        task_id: "t".into(),
                        id: "cfg".into(),
                        tenant: None,
                    },
                )
                .await
                .is_err()
        );
        assert!(
            handler
                .get_extended_agent_card(&params, GetExtendedAgentCardRequest { tenant: None })
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_subscribe_requires_active_execution() {
        let handler = make_handler();
        let params = ServiceParams::new();
        let req = SubscribeToTaskRequest {
            last_seen_generation: None,
            id: "t1".into(),
            tenant: None,
        };
        let result = handler.subscribe_to_task(&params, req).await;
        match result {
            Ok(_) => panic!("expected subscribe_to_task to fail"),
            Err(error) => assert_eq!(error.code, error_code::TASK_NOT_FOUND),
        }
    }

    #[tokio::test]
    async fn test_subscribe_to_terminal_task_is_unsupported_not_not_found() {
        let handler = make_handler();
        handler
            .task_store
            .create(Task {
                generation: 0,
                timeline: None,
                id: "t-done".into(),
                context_id: "c-done".into(),
                status: TaskStatus {
                    state: TaskState::Completed,
                    message: None,
                    timestamp: None,
                },
                artifacts: None,
                history: None,
                metadata: None,
            })
            .await
            .unwrap();
        let result = handler
            .subscribe_to_task(
                &ServiceParams::new(),
                SubscribeToTaskRequest {
                    last_seen_generation: None,
                    id: "t-done".into(),
                    tenant: None,
                },
            )
            .await;
        match result {
            Ok(_) => panic!("expected subscribe_to_task to fail"),
            Err(error) => assert_eq!(error.code, error_code::UNSUPPORTED_OPERATION),
        }
    }

    #[tokio::test]
    async fn test_send_message_return_immediately_allows_resubscribe() {
        use futures::StreamExt;

        let (handler, release) = make_resumable_handler();
        let params = ServiceParams::new();
        let mut message = make_message();
        message.task_id = Some("t-resume".into());
        message.context_id = Some("c-resume".into());
        seed_task(&handler, "t-resume", "c-resume").await;

        let response = handler
            .send_message(
                &params,
                SendMessageRequest {
                    artifact_updates: Vec::new(),
                    message,
                    configuration: Some(SendMessageConfiguration {
                        if_generation_match: None,
                        timeline_length: None,
                        accepted_output_modes: None,
                        task_push_notification_config: None,
                        history_length: None,
                        return_immediately: Some(true),
                    }),
                    metadata: None,
                    tenant: None,
                },
            )
            .await
            .unwrap();

        match response {
            SendMessageResponse::Task(task) => {
                assert_eq!(task.status.state, TaskState::Working);
            }
            _ => panic!("expected Task response"),
        }

        let mut subscription = handler
            .subscribe_to_task(
                &params,
                SubscribeToTaskRequest {
                    last_seen_generation: None,
                    id: "t-resume".into(),
                    tenant: None,
                },
            )
            .await
            .unwrap();

        let snapshot = subscription.next().await.unwrap().unwrap();
        match snapshot {
            StreamResponse::Task(task) => assert_eq!(task.status.state, TaskState::Working),
            _ => panic!("expected snapshot task"),
        }

        release.notify_waiters();

        let terminal = subscription.next().await.unwrap().unwrap();
        match terminal {
            StreamResponse::Task(task) => assert_eq!(task.status.state, TaskState::Completed),
            _ => panic!("expected terminal task"),
        }

        assert!(subscription.next().await.is_none());

        let result = handler
            .subscribe_to_task(
                &params,
                SubscribeToTaskRequest {
                    last_seen_generation: None,
                    id: "t-resume".into(),
                    tenant: None,
                },
            )
            .await;
        match result {
            Ok(_) => panic!("expected subscribe_to_task to fail after completion"),
            Err(error) => assert_eq!(error.code, error_code::UNSUPPORTED_OPERATION),
        }
    }

    #[tokio::test]
    async fn test_push_config_not_supported_without_store() {
        let handler = make_handler();
        let params = ServiceParams::new();
        let result = handler
            .create_push_config(
                &params,
                TaskPushNotificationConfig {
                    protocol_version: None,
                    task_id: "t1".into(),
                    url: "http://example.com".into(),
                    id: None,
                    token: None,
                    authentication: None,
                    tenant: None,
                },
            )
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_list_push_configs_invalid_page_token_errors() {
        install_crypto_provider();
        let handler = make_handler_with_push_configs();
        let params = ServiceParams::new();
        handler
            .create_push_config(
                &params,
                TaskPushNotificationConfig {
                    protocol_version: None,
                    task_id: "t1".into(),
                    url: "https://example.com/callback".into(),
                    id: Some("cfg-1".into()),
                    token: None,
                    authentication: None,
                    tenant: None,
                },
            )
            .await
            .unwrap();

        let result = handler
            .list_push_configs(
                &params,
                ListTaskPushNotificationConfigsRequest {
                    task_id: "t1".into(),
                    page_size: None,
                    page_token: Some("not-a-number".into()),
                    tenant: None,
                },
            )
            .await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code, error_code::INVALID_PARAMS);
    }

    #[tokio::test]
    async fn test_list_tasks_page_size_cap_bounds_a_store_that_ignores_it() {
        install_crypto_provider();
        // A store that pays no attention to page_size. Clamping the request
        // does nothing here; only truncating the response bounds it.
        let handler = DefaultRequestHandler::new(
            EchoExecutor,
            PagingTaskStore::new(MAX_PAGE_SIZE * 3, false),
        );

        let resp = handler
            .list_tasks(
                &ServiceParams::new(),
                ListTasksRequest {
                    timeline_length: None,
                    context_id: None,
                    status: None,
                    page_size: Some(1_000_000),
                    page_token: None,
                    history_length: None,
                    include_artifacts: None,
                    status_timestamp_after: None,
                    tenant: None,
                },
            )
            .await
            .unwrap();

        assert_eq!(resp.tasks.len(), MAX_PAGE_SIZE);
    }

    #[tokio::test]
    async fn test_paging_stores_delegate_everything_except_list() {
        install_crypto_provider();
        // The cap tests above are only measuring the handler if these two
        // doubles differ from the real stores in `list` and nowhere else.
        // Pin that, so a later edit cannot quietly make them lie.
        let tasks = PagingTaskStore::new(0, true);
        let task = Task {
            generation: 0,
            timeline: None,
            id: "t-delegated".into(),
            context_id: "c1".into(),
            status: TaskStatus {
                state: TaskState::Submitted,
                message: None,
                timestamp: None,
            },
            artifacts: None,
            history: None,
            metadata: None,
        };
        tasks.create(task.clone()).await.unwrap();
        assert_eq!(
            tasks
                .get("t-delegated")
                .await
                .unwrap()
                .map(|t| t.status.state),
            Some(TaskState::Submitted)
        );
        let mut done = task.clone();
        done.status.state = TaskState::Completed;
        tasks.update(done).await.unwrap();
        assert_eq!(
            tasks
                .get("t-delegated")
                .await
                .unwrap()
                .map(|t| t.status.state),
            Some(TaskState::Completed)
        );

        // begin_cancel has no default (#250), so this must reach
        // InMemoryTaskStore's atomic implementation -- and refuse a task that
        // is already terminal, as the trait documents.
        let mut cancelable = task.clone();
        cancelable.id = "t-cancelable".into();
        cancelable.status.state = TaskState::Working;
        tasks.create(cancelable).await.unwrap();
        assert_eq!(
            tasks
                .begin_cancel("t-cancelable")
                .await
                .unwrap()
                .status
                .state,
            TaskState::Canceled
        );
        assert_eq!(
            tasks.begin_cancel("t-delegated").await.unwrap_err().code,
            error_code::TASK_NOT_CANCELABLE
        );

        let configs = PagingPushConfigStore::new(0);
        configs
            .save(TaskPushNotificationConfig {
                protocol_version: None,
                task_id: "t1".into(),
                url: "https://example.com/callback".into(),
                id: Some("cfg-1".into()),
                token: None,
                authentication: None,
                tenant: None,
            })
            .await
            .unwrap();
        assert_eq!(
            configs.get("t1", "cfg-1").await.unwrap().id.as_deref(),
            Some("cfg-1")
        );
        configs.delete("t1", "cfg-1").await.unwrap();
        assert!(configs.get("t1", "cfg-1").await.is_err());
        configs.delete_all("t1").await.unwrap();
    }

    #[tokio::test]
    async fn test_list_tasks_page_size_cap_bounds_a_store_that_honours_it() {
        install_crypto_provider();
        // The ordinary case: the store does what it is asked, so what it is
        // asked for has to be bounded. Without the clamp this store would
        // build a million tasks.
        let store = PagingTaskStore::new(MAX_PAGE_SIZE * 3, true);
        let seen = Arc::clone(&store.seen_page_size);
        let handler = DefaultRequestHandler::new(EchoExecutor, store);

        let resp = handler
            .list_tasks(
                &ServiceParams::new(),
                ListTasksRequest {
                    timeline_length: None,
                    context_id: None,
                    status: None,
                    page_size: Some(1_000_000),
                    page_token: None,
                    history_length: None,
                    include_artifacts: None,
                    status_timestamp_after: None,
                    tenant: None,
                },
            )
            .await
            .unwrap();

        assert_eq!(resp.tasks.len(), MAX_PAGE_SIZE);
        // The point of the clamp: the store is never asked for the million.
        // Asserting only on the response would pass with no clamp at all,
        // because the response bound would hide it.
        assert_eq!(seen.load(Ordering::SeqCst), MAX_PAGE_SIZE as i32);
    }

    #[tokio::test]
    async fn test_list_tasks_truncation_keeps_the_store_continuation_token() {
        install_crypto_provider();
        // Truncating must not strand the remainder. The handler cannot
        // synthesise a token for a paging scheme it does not own, so the
        // store's own token has to survive untouched -- otherwise a caller
        // that lost 200 tasks has no way to ask for them.
        let handler = DefaultRequestHandler::new(
            EchoExecutor,
            PagingTaskStore::new(MAX_PAGE_SIZE * 3, false)
                .with_next_page_token("cursor-from-store"),
        );

        let resp = handler
            .list_tasks(
                &ServiceParams::new(),
                ListTasksRequest {
                    timeline_length: None,
                    context_id: None,
                    status: None,
                    page_size: None,
                    page_token: None,
                    history_length: None,
                    include_artifacts: None,
                    status_timestamp_after: None,
                    tenant: None,
                },
            )
            .await
            .unwrap();

        assert_eq!(resp.tasks.len(), DEFAULT_PAGE_SIZE);
        assert_eq!(resp.next_page_token, "cursor-from-store");
    }

    #[tokio::test]
    async fn test_list_tasks_honours_a_page_size_under_the_cap() {
        install_crypto_provider();
        // The clamp is an upper bound, not a floor: a modest request must
        // reach the store unchanged.
        let store = PagingTaskStore::new(MAX_PAGE_SIZE * 3, true);
        let seen = Arc::clone(&store.seen_page_size);
        let handler = DefaultRequestHandler::new(EchoExecutor, store);

        let resp = handler
            .list_tasks(
                &ServiceParams::new(),
                ListTasksRequest {
                    timeline_length: None,
                    context_id: None,
                    status: None,
                    page_size: Some(7),
                    page_token: None,
                    history_length: None,
                    include_artifacts: None,
                    status_timestamp_after: None,
                    tenant: None,
                },
            )
            .await
            .unwrap();

        assert_eq!(seen.load(Ordering::SeqCst), 7);
        assert_eq!(resp.tasks.len(), 7);
    }

    #[tokio::test]
    async fn test_list_push_configs_page_size_cap_bounds_a_custom_store() {
        install_crypto_provider();
        // InMemoryPushConfigStore caps a task at 50 configs, below the page
        // cap of 100, so it can never reach the clamp. A store without that
        // limit can, and is what the clamp exists for.
        let handler = DefaultRequestHandler::new(EchoExecutor, InMemoryTaskStore::new())
            .with_push_config_store(PagingPushConfigStore::new(MAX_PAGE_SIZE * 3));

        let resp = handler
            .list_push_configs(
                &ServiceParams::new(),
                ListTaskPushNotificationConfigsRequest {
                    task_id: "t1".into(),
                    page_size: Some(1_000_000),
                    page_token: None,
                    tenant: None,
                },
            )
            .await
            .unwrap();

        assert_eq!(resp.configs.len(), MAX_PAGE_SIZE);
        // The remainder must stay reachable, or the cap silently truncates.
        assert_eq!(resp.next_page_token.as_deref(), Some("100"));
    }

    #[tokio::test]
    async fn test_list_push_configs_page_size_is_capped() {
        install_crypto_provider();
        let handler = make_handler_with_push_configs();
        let params = ServiceParams::new();
        // Keep the count below InMemoryPushConfigStore's per-task cap
        // (MAX_PUSH_CONFIGS_PER_TASK), which rejects the 51st config.
        for i in 0..40 {
            handler
                .create_push_config(
                    &params,
                    TaskPushNotificationConfig {
                        protocol_version: None,
                        task_id: "t1".into(),
                        url: format!("https://example.com/callback/{i}"),
                        id: Some(format!("cfg-{i:03}")),
                        token: None,
                        authentication: None,
                        tenant: None,
                    },
                )
                .await
                .unwrap();
        }

        // Requesting a huge page size must not error and returns everything
        // available. This does NOT exercise the cap -- 40 < MAX_PAGE_SIZE, so
        // a capped and an uncapped implementation agree here. The cap itself
        // is pinned by test_list_push_configs_page_size_cap_bounds_a_custom_store,
        // which is the only route that can reach it.
        let resp = handler
            .list_push_configs(
                &params,
                ListTaskPushNotificationConfigsRequest {
                    task_id: "t1".into(),
                    page_size: Some(1000),
                    page_token: None,
                    tenant: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(resp.configs.len(), 40);
    }

    #[tokio::test]
    async fn test_push_config_crud_with_store() {
        install_crypto_provider();
        let handler = make_handler_with_push_configs();
        let params = ServiceParams::new();

        let created = handler
            .create_push_config(
                &params,
                TaskPushNotificationConfig {
                    protocol_version: None,
                    task_id: "t1".into(),
                    url: "https://example.com/first".into(),
                    id: Some("cfg-1".into()),
                    token: None,
                    authentication: None,
                    tenant: Some("tenant-a".into()),
                },
            )
            .await
            .unwrap();
        assert_eq!(created.id.as_deref(), Some("cfg-1"));

        handler
            .create_push_config(
                &params,
                TaskPushNotificationConfig {
                    protocol_version: None,
                    task_id: "t1".into(),
                    url: "https://example.com/second".into(),
                    id: Some("cfg-2".into()),
                    token: None,
                    authentication: None,
                    tenant: Some("tenant-a".into()),
                },
            )
            .await
            .unwrap();

        let fetched = handler
            .get_push_config(
                &params,
                GetTaskPushNotificationConfigRequest {
                    task_id: "t1".into(),
                    id: "cfg-1".into(),
                    tenant: Some("tenant-a".into()),
                },
            )
            .await
            .unwrap();
        assert_eq!(fetched.url, "https://example.com/first");

        let first_page = handler
            .list_push_configs(
                &params,
                ListTaskPushNotificationConfigsRequest {
                    task_id: "t1".into(),
                    page_size: Some(1),
                    page_token: Some("0".into()),
                    tenant: Some("tenant-a".into()),
                },
            )
            .await
            .unwrap();
        assert_eq!(first_page.configs.len(), 1);
        assert_eq!(first_page.configs[0].id.as_deref(), Some("cfg-1"));
        assert_eq!(first_page.next_page_token.as_deref(), Some("1"));

        let default_page = handler
            .list_push_configs(
                &params,
                ListTaskPushNotificationConfigsRequest {
                    task_id: "t1".into(),
                    page_size: Some(0),
                    page_token: None,
                    tenant: Some("tenant-a".into()),
                },
            )
            .await
            .unwrap();
        assert_eq!(default_page.configs.len(), 2);
        assert!(default_page.next_page_token.is_none());

        handler
            .delete_push_config(
                &params,
                DeleteTaskPushNotificationConfigRequest {
                    task_id: "t1".into(),
                    id: "cfg-1".into(),
                    tenant: Some("tenant-a".into()),
                },
            )
            .await
            .unwrap();

        let remaining = handler
            .list_push_configs(
                &params,
                ListTaskPushNotificationConfigsRequest {
                    task_id: "t1".into(),
                    page_size: None,
                    page_token: None,
                    tenant: Some("tenant-a".into()),
                },
            )
            .await
            .unwrap();
        assert_eq!(remaining.configs.len(), 1);
        assert_eq!(remaining.configs[0].id.as_deref(), Some("cfg-2"));
    }

    #[tokio::test]
    async fn test_send_message_request_push_config_delivers_webhooks() {
        install_crypto_provider();
        let (url, mut receiver, shutdown_tx, server) = start_push_webhook().await;
        let handler = make_push_delivery_handler();
        let params = ServiceParams::new();
        let mut message = make_message();
        message.task_id = Some("t-push-request".into());
        message.context_id = Some("c-push-request".into());
        seed_task(&handler, "t-push-request", "c-push-request").await;

        let response = handler
            .send_message(
                &params,
                SendMessageRequest {
                    artifact_updates: Vec::new(),
                    message,
                    configuration: Some(SendMessageConfiguration {
                        if_generation_match: None,
                        timeline_length: None,
                        accepted_output_modes: None,
                        task_push_notification_config: Some(TaskPushNotificationConfig {
                            protocol_version: None,
                            task_id: String::new(),
                            url: url.clone(),
                            id: Some("cfg-request".into()),
                            token: Some("notify-token".into()),
                            authentication: Some(AuthenticationInfo {
                                scheme: "bearer".into(),
                                credentials: Some("secret-token".into()),
                            }),
                            tenant: None,
                        }),
                        history_length: None,
                        return_immediately: None,
                    }),
                    metadata: None,
                    tenant: None,
                },
            )
            .await
            .unwrap();

        match response {
            SendMessageResponse::Task(task) => assert_eq!(task.status.state, TaskState::Completed),
            _ => panic!("expected Task response"),
        }

        let saved = handler
            .get_push_config(
                &params,
                GetTaskPushNotificationConfigRequest {
                    task_id: "t-push-request".into(),
                    id: "cfg-request".into(),
                    tenant: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(saved.url, url);

        let first = next_push(&mut receiver).await;
        assert_eq!(first.authorization.as_deref(), Some("Bearer secret-token"));
        assert_eq!(first.notification_token.as_deref(), Some("notify-token"));
        match first.event {
            StreamResponse::StatusUpdate(update) => {
                assert_eq!(update.task_id, "t-push-request");
                assert_eq!(update.status.state, TaskState::Working);
            }
            _ => panic!("expected status update push"),
        }

        let second = next_push(&mut receiver).await;
        assert_eq!(second.authorization.as_deref(), Some("Bearer secret-token"));
        assert_eq!(second.notification_token.as_deref(), Some("notify-token"));
        match second.event {
            StreamResponse::Task(task) => {
                assert_eq!(task.id, "t-push-request");
                assert_eq!(task.status.state, TaskState::Completed);
            }
            _ => panic!("expected final task push"),
        }

        shutdown_tx.send(()).unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn test_send_message_stored_push_config_delivers_webhooks() {
        install_crypto_provider();
        let (url, mut receiver, shutdown_tx, server) = start_push_webhook().await;
        let handler = make_push_delivery_handler();
        let params = ServiceParams::new();

        handler
            .create_push_config(
                &params,
                TaskPushNotificationConfig {
                    protocol_version: None,
                    task_id: "t-push-stored".into(),
                    url: url.clone(),
                    id: Some("cfg-stored".into()),
                    token: Some("stored-token".into()),
                    authentication: None,
                    tenant: None,
                },
            )
            .await
            .unwrap();

        let mut message = make_message();
        message.task_id = Some("t-push-stored".into());
        message.context_id = Some("c-push-stored".into());
        seed_task(&handler, "t-push-stored", "c-push-stored").await;
        let response = handler
            .send_message(
                &params,
                SendMessageRequest {
                    artifact_updates: Vec::new(),
                    message,
                    configuration: None,
                    metadata: None,
                    tenant: None,
                },
            )
            .await
            .unwrap();

        match response {
            SendMessageResponse::Task(task) => assert_eq!(task.status.state, TaskState::Completed),
            _ => panic!("expected Task response"),
        }

        let first = next_push(&mut receiver).await;
        assert_eq!(first.notification_token.as_deref(), Some("stored-token"));
        match first.event {
            StreamResponse::StatusUpdate(update) => {
                assert_eq!(update.task_id, "t-push-stored");
                assert_eq!(update.status.state, TaskState::Working);
            }
            _ => panic!("expected status update push"),
        }

        let second = next_push(&mut receiver).await;
        assert_eq!(second.notification_token.as_deref(), Some("stored-token"));
        match second.event {
            StreamResponse::Task(task) => {
                assert_eq!(task.id, "t-push-stored");
                assert_eq!(task.status.state, TaskState::Completed);
            }
            _ => panic!("expected final task push"),
        }

        shutdown_tx.send(()).unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn test_extended_agent_card_not_configured() {
        let handler = make_handler();
        let params = ServiceParams::new();
        let result = handler
            .get_extended_agent_card(&params, GetExtendedAgentCardRequest { tenant: None })
            .await;
        assert!(result.is_err());
    }

    #[test]
    fn test_with_capabilities() {
        let handler = make_handler().with_capabilities(AgentCapabilities {
            user_artifacts: None,
            streaming: Some(true),
            push_notifications: None,
            extensions: None,
            extended_agent_card: None,
        });
        assert_eq!(handler.capabilities.streaming, Some(true));
    }

    #[test]
    fn test_with_push_config_store_enables_push_capability() {
        install_crypto_provider();
        let handler = make_handler_with_push_configs();
        assert_eq!(handler.capabilities.push_notifications, Some(true));
    }

    // §13.3 (#203): a capability the card does not declare is not served.

    fn caps(streaming: Option<bool>, push: Option<bool>) -> AgentCapabilities {
        AgentCapabilities {
            user_artifacts: None,
            streaming,
            push_notifications: push,
            extensions: None,
            extended_agent_card: None,
        }
    }

    fn send_request() -> SendMessageRequest {
        SendMessageRequest {
            artifact_updates: Vec::new(),
            message: Message::new(Role::User, vec![Part::text("hi")]),
            configuration: None,
            metadata: None,
            tenant: None,
        }
    }

    fn push_config(task_id: &str) -> TaskPushNotificationConfig {
        TaskPushNotificationConfig {
            protocol_version: None,
            task_id: task_id.into(),
            url: "https://example.com/callback".into(),
            id: Some("cfg-1".into()),
            token: None,
            authentication: None,
            tenant: None,
        }
    }

    #[tokio::test]
    async fn test_streaming_refused_when_the_card_declares_it_absent() {
        let handler = make_handler().with_capabilities(caps(Some(false), None));
        let params = ServiceParams::new();

        // The Ok arm is a BoxStream, which is not Debug, so match rather
        // than unwrap_err().
        match handler
            .send_streaming_message(&params, send_request())
            .await
        {
            Err(error) => assert_eq!(error.code, error_code::UNSUPPORTED_OPERATION),
            Ok(_) => panic!("streaming must be refused when the card declares it absent"),
        }

        match handler
            .subscribe_to_task(
                &params,
                SubscribeToTaskRequest {
                    last_seen_generation: None,
                    id: "t1".into(),
                    tenant: None,
                },
            )
            .await
        {
            Err(error) => assert_eq!(error.code, error_code::UNSUPPORTED_OPERATION),
            Ok(_) => panic!("subscribe must be refused when the card declares it absent"),
        }
    }

    #[tokio::test]
    async fn test_push_refused_when_the_card_declares_it_absent() {
        // A store *is* configured; the card declaring `false` is what refuses.
        // Before #203 this combination served push regardless.
        let handler = make_handler_with_push_configs().with_capabilities(caps(None, Some(false)));
        let params = ServiceParams::new();

        let error = handler
            .create_push_config(&params, push_config("t1"))
            .await
            .unwrap_err();
        assert_eq!(error.code, error_code::PUSH_NOTIFICATION_NOT_SUPPORTED);

        let error = handler
            .list_push_configs(
                &params,
                ListTaskPushNotificationConfigsRequest {
                    task_id: "t1".into(),
                    page_size: None,
                    page_token: None,
                    tenant: None,
                },
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, error_code::PUSH_NOTIFICATION_NOT_SUPPORTED);
    }

    /// The regression guard. `AgentCapabilities::default()` leaves every field
    /// `None`, and most embedders never call `with_capabilities`; if `None`
    /// started meaning "refuse", all of them would break at once.
    #[tokio::test]
    async fn test_undeclared_capabilities_stay_permissive() {
        let handler = make_handler_with_push_configs();
        let params = ServiceParams::new();

        assert!(
            handler
                .send_streaming_message(&params, send_request())
                .await
                .is_ok(),
            "an undeclared capability must not be treated as refused"
        );
        assert!(
            handler
                .create_push_config(&params, push_config("t1"))
                .await
                .is_ok()
        );
    }

    /// Supplying a store defaults the capability to `true` but must not
    /// override an explicit `false`, so the outcome does not depend on which
    /// builder method was called last.
    #[tokio::test]
    async fn test_builder_order_does_not_change_the_declared_capability() {
        let params = ServiceParams::new();

        for handler in [
            make_handler()
                .with_capabilities(caps(None, Some(false)))
                .with_push_config_store(InMemoryPushConfigStore::new()),
            make_handler()
                .with_push_config_store(InMemoryPushConfigStore::new())
                .with_capabilities(caps(None, Some(false))),
        ] {
            let error = handler
                .create_push_config(&params, push_config("t1"))
                .await
                .unwrap_err();
            assert_eq!(error.code, error_code::PUSH_NOTIFICATION_NOT_SUPPORTED);
        }
    }

    /// `with_push_notifications` defaults the capability the same way as
    /// `with_push_config_store`, and likewise must not override an explicit
    /// `false`. It had no unit test before #203 — only an example — so the
    /// two builders could have drifted apart unnoticed.
    #[tokio::test]
    async fn test_with_push_notifications_defaults_the_capability() {
        let params = ServiceParams::new();

        let handler = make_handler().with_push_notifications(
            InMemoryPushConfigStore::new(),
            crate::HttpPushSender::new(None),
        );
        assert_eq!(handler.capabilities.push_notifications, Some(true));
        assert!(
            handler
                .create_push_config(&params, push_config("t1"))
                .await
                .is_ok()
        );

        let handler = make_handler()
            .with_capabilities(caps(None, Some(false)))
            .with_push_notifications(
                InMemoryPushConfigStore::new(),
                crate::HttpPushSender::new(None),
            );
        assert_eq!(handler.capabilities.push_notifications, Some(false));
        let error = handler
            .create_push_config(&params, push_config("t1"))
            .await
            .unwrap_err();
        assert_eq!(error.code, error_code::PUSH_NOTIFICATION_NOT_SUPPORTED);
    }

    /// A store that was never supplied still reports the same code — that is
    /// the deployment state, distinct from the card's declaration.
    #[tokio::test]
    async fn test_missing_store_still_reports_push_unsupported() {
        let handler = make_handler();
        let error = handler
            .create_push_config(&ServiceParams::new(), push_config("t1"))
            .await
            .unwrap_err();
        assert_eq!(error.code, error_code::PUSH_NOTIFICATION_NOT_SUPPORTED);
    }

    /// Reported in #161 against a different crate (`a2a-protocol-server`),
    /// but worth pinning here: two tasks sharing a `contextId` must each
    /// carry only their own artifacts. The task store is keyed by task id,
    /// so there is no path by which a context could aggregate them — this
    /// test exists so that stays true.
    #[tokio::test]
    async fn test_artifacts_do_not_aggregate_across_tasks_in_one_context() {
        let handler = make_handler();
        let params = ServiceParams::new();
        let context_id = "ctx-shared";

        for (task_id, artifact_id) in [("t-first", "a-1"), ("t-second", "a-2")] {
            handler
                .task_store
                .create(Task {
                    generation: 0,
                    timeline: None,
                    id: task_id.into(),
                    context_id: context_id.into(),
                    status: TaskStatus {
                        state: TaskState::Working,
                        message: None,
                        timestamp: None,
                    },
                    artifacts: None,
                    history: None,
                    metadata: None,
                })
                .await
                .unwrap();

            apply_event_to_task(
                handler.task_store.as_ref(),
                None,
                &mut StreamResponse::ArtifactUpdate(TaskArtifactUpdateEvent {
                    generation: 0,
                    task_id: task_id.into(),
                    context_id: context_id.into(),
                    artifact: Artifact {
                        end_generation: None,
                        role: None,
                        start_generation: None,
                        artifact_id: artifact_id.into(),
                        name: None,
                        description: None,
                        parts: vec![Part::text("payload")],
                        metadata: None,
                        extensions: None,
                    },
                    append: None,
                    last_chunk: None,
                    metadata: None,
                }),
            )
            .await
            .unwrap();
        }

        for (task_id, expected) in [("t-first", "a-1"), ("t-second", "a-2")] {
            let task = handler
                .get_task(
                    &params,
                    GetTaskRequest {
                        current_generation: None,
                        timeline_length: None,
                        id: task_id.into(),
                        history_length: None,
                        tenant: None,
                    },
                )
                .await
                .unwrap();
            let ids: Vec<&str> = task
                .artifacts
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(|a| a.artifact_id.as_str())
                .collect();
            assert_eq!(
                ids,
                vec![expected],
                "{task_id} must carry only its own artifact"
            );
        }
    }
    // §12.4 / §10.1 (#204): the extended Agent Card.

    fn extended_card(name: &str) -> AgentCard {
        AgentCard {
            name: name.into(),
            description: "extended".into(),
            version: "1.0".into(),
            supported_interfaces: vec![],
            capabilities: AgentCapabilities::default(),
            default_input_modes: vec![],
            default_output_modes: vec![],
            skills: vec![],
            provider: None,
            documentation_url: None,
            icon_url: None,
            security_schemes: None,
            security_requirements: None,
            signatures: None,
        }
    }

    fn extended_request() -> GetExtendedAgentCardRequest {
        GetExtendedAgentCardRequest { tenant: None }
    }

    #[tokio::test]
    async fn test_extended_agent_card_is_served_when_configured() {
        let handler = make_handler().with_extended_agent_card(extended_card("secret skills"));
        let card = handler
            .get_extended_agent_card(&ServiceParams::new(), extended_request())
            .await
            .unwrap();
        assert_eq!(card.name, "secret skills");
    }

    /// Configuring a card also declares the capability, so serving one and
    /// advertising it are not two things to remember.
    #[tokio::test]
    async fn test_configuring_an_extended_card_declares_the_capability() {
        let handler = make_handler().with_extended_agent_card(extended_card("x"));
        assert_eq!(handler.capabilities.extended_agent_card, Some(true));
    }

    /// A2A gives this condition its own code. Before #204 it reported
    /// `UNSUPPORTED_OPERATION`, which says the agent does not do extended
    /// cards at all rather than that this deployment has not configured one.
    #[tokio::test]
    async fn test_unconfigured_extended_card_reports_its_own_code() {
        let handler = make_handler().with_capabilities(AgentCapabilities {
            user_artifacts: None,
            streaming: None,
            push_notifications: None,
            extensions: None,
            extended_agent_card: Some(true),
        });
        let error = handler
            .get_extended_agent_card(&ServiceParams::new(), extended_request())
            .await
            .unwrap_err();
        assert_eq!(error.code, error_code::EXTENDED_CARD_NOT_CONFIGURED);
        assert_ne!(error.code, error_code::UNSUPPORTED_OPERATION);
    }

    /// The capability gate from #203 runs first: a card declaring
    /// `extendedAgentCard: false` refuses before the configured/unconfigured
    /// distinction is even reached.
    #[tokio::test]
    async fn test_capability_gate_precedes_the_not_configured_check() {
        let handler = make_handler()
            .with_extended_agent_card(extended_card("x"))
            .with_capabilities(AgentCapabilities {
                user_artifacts: None,
                streaming: None,
                push_notifications: None,
                extensions: None,
                extended_agent_card: Some(false),
            });
        let error = handler
            .get_extended_agent_card(&ServiceParams::new(), extended_request())
            .await
            .unwrap_err();
        assert_eq!(error.code, error_code::UNSUPPORTED_OPERATION);
    }

    /// §12.4 fetches the extended card under a security scheme, so a
    /// resolver can vary what it reveals by caller.
    #[tokio::test]
    async fn test_resolver_sees_the_caller_params() {
        let handler = make_handler().with_extended_agent_card_resolver(|params: &ServiceParams| {
            let privileged = params.contains_key("x-privileged");
            Ok(extended_card(if privileged { "full" } else { "limited" }))
        });

        let card = handler
            .get_extended_agent_card(&ServiceParams::new(), extended_request())
            .await
            .unwrap();
        assert_eq!(card.name, "limited");

        let mut params = ServiceParams::new();
        params.insert("x-privileged".into(), vec!["yes".into()]);
        let card = handler
            .get_extended_agent_card(&params, extended_request())
            .await
            .unwrap();
        assert_eq!(card.name, "full");
    }
}

#[cfg(test)]
mod lag_coverage_tests {
    use super::*;
    use futures::StreamExt;

    /// The reservation hands back the task's writer already held: whoever
    /// finds the execution registered then waits on a lock that is taken, not
    /// one that is still free between the registration and the first write.
    #[tokio::test]
    async fn test_reserve_holds_the_writer_from_the_moment_it_registers() {
        let manager = ExecutionManager::default();

        let (active, guard) = manager.reserve("t-reserved").await.unwrap();
        assert!(manager.get("t-reserved").await.is_some());
        assert!(
            manager.writer_for("t-reserved").try_lock().is_err(),
            "the writer is held at registration"
        );
        // It is the same lock the execution writes under.
        assert!(Arc::ptr_eq(
            &active.writer,
            &manager.writer_for("t-reserved")
        ));

        drop(guard);
        assert!(manager.writer_for("t-reserved").try_lock().is_ok());
        // Once the execution has its snapshot a second reservation is refused,
        // and does not leave the writer taken.
        active
            .set_snapshot(Task {
                generation: 1,
                timeline: None,
                id: "t-reserved".into(),
                context_id: "c-reserved".into(),
                status: TaskStatus {
                    state: TaskState::Working,
                    message: None,
                    timestamp: None,
                },
                artifacts: None,
                history: None,
                metadata: None,
            })
            .await;
        assert!(manager.reserve("t-reserved").await.is_err());
        assert!(manager.writer_for("t-reserved").try_lock().is_ok());
    }

    #[tokio::test]
    async fn test_subscription_lag_is_reported_as_a_sanitized_internal_error() {
        // A subscriber that falls behind the active execution gets a generic
        // INTERNAL_ERROR rather than the broadcast internals, and the stream
        // terminates instead of spinning on a lost buffer.
        let (sender, receiver) = broadcast::channel(1);

        for sequence in 0..4 {
            let _ = sender.send(ExecutionEvent {
                sequence,
                result: Ok(StreamResponse::Message(a2a::Message {
                    message_id: "lag-probe".to_string(),
                    role: a2a::Role::Agent,
                    parts: Vec::new(),
                    context_id: None,
                    task_id: None,
                    metadata: None,
                    extensions: None,
                    reference_task_ids: None,
                })),
                kind: EventKind::Normal,
            });
        }

        let mut stream = subscription_stream(receiver, None, 0, true, false, false);
        let item = stream.next().await.expect("lagged item");

        let error = item.expect_err("lag must surface as an error");
        assert_eq!(error.code, a2a::error_code::INTERNAL_ERROR);
        assert_eq!(error.message, "Internal error");

        // `done` is set on lag, so the stream ends rather than repeating.
        assert!(stream.next().await.is_none());
    }
}
