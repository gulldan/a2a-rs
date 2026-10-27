// Copyright AGNTCY Contributors (https://github.com/agntcy)
// Copyright A2A Contributors (https://github.com/a2aproject)
// SPDX-License-Identifier: Apache-2.0
//! A new task is created by the executor: its first event creates it, and a
//! direct reply leaves nothing saved (A2A §3.1.1, §3.1.2, §3.2.1).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use a2a::*;
use a2a_server::{
    AgentExecutor, DefaultRequestHandler, ExecutorContext, InMemoryTaskStore, RequestHandler,
    ServiceParams,
};
use futures::{StreamExt, stream, stream::BoxStream};

/// What the executor does depends on the text of the message it is sent.
struct Agent {
    ids: Arc<Mutex<Vec<String>>>,
}

fn status(task: &str, context: &str, state: TaskState, text: &str) -> StreamResponse {
    StreamResponse::StatusUpdate(TaskStatusUpdateEvent {
        task_id: task.into(),
        context_id: context.into(),
        status: TaskStatus {
            state,
            message: Some(Message::new(Role::Agent, vec![Part::text(text)])),
            timestamp: None,
        },
        metadata: None,
        generation: 0,
    })
}

impl AgentExecutor for Agent {
    fn execute(
        &self,
        ctx: ExecutorContext,
    ) -> BoxStream<'static, Result<StreamResponse, A2AError>> {
        self.ids.lock().unwrap().push(ctx.task_id.clone());
        let (task, context) = (ctx.task_id.clone(), ctx.context_id.clone());
        let text = ctx
            .message
            .as_ref()
            .and_then(|m| m.text())
            .unwrap_or("")
            .to_string();
        Box::pin(async_stream::stream! {
            match text.as_str() {
                "reply" => {
                    yield Ok(StreamResponse::Message(Message::new(
                        Role::Agent,
                        vec![Part::text("hi")],
                    )));
                }
                "reply-then-more" => {
                    yield Ok(StreamResponse::Message(Message::new(
                        Role::Agent,
                        vec![Part::text("hi")],
                    )));
                    tokio::time::sleep(Duration::from_millis(600)).await;
                    yield Ok(status(&task, &context, TaskState::Working, "too late"));
                }
                "silent" => {}
                "error" => {
                    yield Err(A2AError::internal("no"));
                }
                "task" => {
                    yield Ok(StreamResponse::Task(Task {
                        id: task.clone(),
                        context_id: context.clone(),
                        status: TaskStatus {
                            state: TaskState::Working,
                            message: None,
                            timestamp: None,
                        },
                        artifacts: None,
                        history: None,
                        metadata: None,
                        generation: 0,
                        timeline: None,
                    }));
                }
                _ => {
                    yield Ok(status(&task, &context, TaskState::Working, "working"));
                    yield Ok(status(&task, &context, TaskState::Completed, "done"));
                }
            }
        })
    }

    fn cancel(&self, ctx: ExecutorContext) -> BoxStream<'static, Result<StreamResponse, A2AError>> {
        Box::pin(stream::once(async move {
            Ok(status(
                &ctx.task_id,
                &ctx.context_id,
                TaskState::Canceled,
                "canceled",
            ))
        }))
    }
}

fn handler() -> (Arc<DefaultRequestHandler>, Arc<Mutex<Vec<String>>>) {
    let ids = Arc::new(Mutex::new(Vec::new()));
    let handler = DefaultRequestHandler::new(Agent { ids: ids.clone() }, InMemoryTaskStore::new())
        .with_capabilities(AgentCapabilities {
            streaming: Some(true),
            ..AgentCapabilities::default()
        });
    (Arc::new(handler), ids)
}

fn params() -> ServiceParams {
    let mut params = ServiceParams::new();
    params.insert(SVC_PARAM_VERSION.to_string(), vec!["1.1".to_string()]);
    params
}

fn send(text: &str) -> SendMessageRequest {
    SendMessageRequest {
        message: Message::new(Role::User, vec![Part::text(text)]),
        configuration: None,
        metadata: None,
        tenant: None,
        artifact_updates: Vec::new(),
    }
}

async fn tasks(handler: &DefaultRequestHandler) -> usize {
    handler
        .list_tasks(
            &params(),
            ListTasksRequest {
                context_id: None,
                status: None,
                page_size: None,
                page_token: None,
                history_length: None,
                timeline_length: None,
                status_timestamp_after: None,
                include_artifacts: None,
                tenant: None,
            },
        )
        .await
        .unwrap()
        .tasks
        .len()
}

#[tokio::test]
async fn a_direct_reply_leaves_no_task() {
    let (handler, ids) = handler();
    let response = handler
        .send_message(&params(), send("reply"))
        .await
        .unwrap();
    assert!(matches!(response, SendMessageResponse::Message(_)));
    assert_eq!(tasks(&handler).await, 0);
    let id = ids.lock().unwrap()[0].clone();
    let error = handler
        .get_task(
            &params(),
            GetTaskRequest {
                id,
                history_length: None,
                tenant: None,
                timeline_length: None,
                current_generation: None,
            },
        )
        .await
        .expect_err("no task was created");
    assert_eq!(error.code, error_code::TASK_NOT_FOUND);
}

#[tokio::test]
async fn a_streamed_direct_reply_is_one_message_and_ends_with_it() {
    let (handler, _) = handler();
    // The executor goes on working for a while after its reply.
    let started = tokio::time::Instant::now();
    let events: Vec<_> = handler
        .send_streaming_message(&params(), send("reply-then-more"))
        .await
        .unwrap()
        .collect()
        .await;
    assert!(
        started.elapsed() < Duration::from_millis(400),
        "the stream waited"
    );
    assert_eq!(events.len(), 1, "{events:?}");
    assert!(matches!(events[0], Ok(StreamResponse::Message(_))));
    // What it says afterwards belongs to no task.
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert_eq!(tasks(&handler).await, 0);
}

#[tokio::test]
async fn an_executor_that_says_nothing_still_has_the_task_the_request_prepared() {
    let (handler, _) = handler();
    let response = handler
        .send_message(&params(), send("silent"))
        .await
        .unwrap();
    let SendMessageResponse::Task(task) = response else {
        panic!("a task is returned");
    };
    assert_eq!(task.status.state, TaskState::Submitted);
    assert_eq!(tasks(&handler).await, 1);
}

#[tokio::test]
async fn an_executor_error_before_anything_leaves_no_task() {
    let (handler, _) = handler();
    handler
        .send_message(&params(), send("error"))
        .await
        .expect_err("the executor failed");
    assert_eq!(tasks(&handler).await, 0);
}

#[tokio::test]
async fn the_first_task_event_creates_the_task_with_the_clients_message() {
    let (handler, _) = handler();
    for text in ["task", "status"] {
        let events: Vec<_> = handler
            .send_streaming_message(&params(), send(text))
            .await
            .unwrap()
            .collect()
            .await;
        // A task lifecycle stream begins with the task.
        let Some(Ok(StreamResponse::Task(first))) = events.first() else {
            panic!("{text}: the stream begins with the task: {events:?}");
        };
        let stored = handler
            .get_task(
                &params(),
                GetTaskRequest {
                    id: first.id.clone(),
                    history_length: None,
                    tenant: None,
                    timeline_length: None,
                    current_generation: None,
                },
            )
            .await
            .unwrap();
        assert!(
            stored.timeline.iter().flatten().any(|entry| matches!(
                &entry.content,
                TimelineContent::Message(m) if m.text() == Some(text)
            )),
            "{text}: the client's message is in the timeline"
        );
    }
}

#[tokio::test]
async fn a_live_exchange_creates_its_task_up_front() {
    use a2a::{StreamRequest, StreamRequestPayload};
    use tokio_stream::wrappers::ReceiverStream;

    let (handler, ids) = handler();
    let (up, rx) = tokio::sync::mpsc::channel(4);
    up.send(StreamRequest {
        tenant: None,
        payload: StreamRequestPayload::SendMessage(send("reply")),
    })
    .await
    .unwrap();
    let mut down = handler
        .send_live_message(&params(), ReceiverStream::new(rx).boxed())
        .await
        .unwrap();
    // The exchange is bound to one task from its first request, so it opens
    // with that task even though the executor only replied.
    let Some(Ok(StreamResponse::Task(task))) = down.next().await else {
        panic!("the exchange opens with the task");
    };
    assert_eq!(task.id, ids.lock().unwrap()[0]);
    assert_eq!(tasks(&handler).await, 1);
}

// ---------------------------------------------------------------------------
// The typed form: an `Agent` returns a reply or a task
// ---------------------------------------------------------------------------

struct Typed;

#[async_trait::async_trait]
impl a2a_server::Agent for Typed {
    async fn start(&self, ctx: ExecutorContext) -> Result<a2a_server::Start, A2AError> {
        let text = ctx
            .message
            .as_ref()
            .and_then(|m| m.text())
            .unwrap_or("")
            .to_string();
        if text == "reply" {
            return Ok(a2a_server::Start::Message(Message::new(
                Role::Agent,
                vec![Part::text("hi")],
            )));
        }
        let (task_id, context_id) = ctx.task_info();
        Ok(a2a_server::Start::Task(
            ctx.new_task(TaskState::Working),
            a2a_server::TaskRun::new(move |inputs| {
                async_stream::stream! {
                    let Some(mut inputs) = inputs else { return };
                    while let Some(input) = inputs.recv().await {
                        let text = input.message.text().unwrap_or("").to_string();
                        if text == "finish" {
                            yield Ok(status(&task_id, &context_id, TaskState::Completed, "done"));
                            return;
                        }
                        yield Ok(status(&task_id, &context_id, TaskState::Working, &format!("got: {text}")));
                    }
                }
            }),
        ))
    }

    fn cancel(&self, ctx: ExecutorContext) -> BoxStream<'static, Result<StreamResponse, A2AError>> {
        Box::pin(stream::once(async move {
            Ok(status(
                &ctx.task_id,
                &ctx.context_id,
                TaskState::Canceled,
                "canceled",
            ))
        }))
    }

    fn accepts_live_input(&self) -> bool {
        true
    }
}

fn typed_handler() -> Arc<DefaultRequestHandler> {
    Arc::new(
        DefaultRequestHandler::from_agent(Typed, InMemoryTaskStore::new()).with_capabilities(
            AgentCapabilities {
                streaming: Some(true),
                ..AgentCapabilities::default()
            },
        ),
    )
}

#[tokio::test]
async fn an_agent_that_replies_creates_no_task() {
    let handler = typed_handler();
    let response = handler
        .send_message(&params(), send("reply"))
        .await
        .unwrap();
    assert!(matches!(response, SendMessageResponse::Message(_)));
    assert_eq!(tasks(&handler).await, 0);
}

#[tokio::test]
async fn an_agent_that_starts_a_task_reads_the_clients_messages_from_then_on() {
    let handler = typed_handler();
    let mut stream = handler
        .send_streaming_message(&params(), send("hello"))
        .await
        .unwrap();
    let Some(Ok(StreamResponse::Task(task))) = stream.next().await else {
        panic!("the stream begins with the task");
    };
    assert_eq!(task.status.state, TaskState::Working);
    assert_eq!(tasks(&handler).await, 1);

    let mut follow_up = send("ping");
    follow_up.message.task_id = Some(task.id.clone());
    let mut second = handler
        .send_streaming_message(&params(), follow_up)
        .await
        .unwrap();
    let _ = second.next().await; // the task, holding the message
    let StreamResponse::StatusUpdate(update) = second.next().await.unwrap().unwrap() else {
        panic!("a status update follows");
    };
    assert_eq!(
        update
            .status
            .message
            .and_then(|m| m.text().map(str::to_owned)),
        Some("got: ping".to_string())
    );
}
