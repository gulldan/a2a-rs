// Copyright AGNTCY Contributors (https://github.com/agntcy)
// Copyright A2A Contributors (https://github.com/a2aproject)
// SPDX-License-Identifier: Apache-2.0
//! `SendLiveMessage` (A2A 1.1 §3.1.12) against the default handler.

use std::sync::Arc;
use std::time::Duration;

use a2a::event::{StreamError, TaskMessageUpdateEvent};
use a2a::*;
use a2a_server::{
    AgentExecutor, DefaultRequestHandler, ExecutorContext, InMemoryTaskStore, LiveResponse,
    RequestHandler, ServiceParams,
};
use futures::{StreamExt, stream, stream::BoxStream};
use tokio::sync::{Notify, mpsc};
use tokio_stream::wrappers::ReceiverStream;

/// Works until told "finish". With `takes_input` it reads the client's
/// messages while it works and says what it got; without it, it ignores them.
struct LiveAgent {
    takes_input: bool,
    release: Arc<Notify>,
}

fn status(ctx_task: &str, ctx_context: &str, state: TaskState, text: &str) -> StreamResponse {
    StreamResponse::StatusUpdate(TaskStatusUpdateEvent {
        task_id: ctx_task.into(),
        context_id: ctx_context.into(),
        status: TaskStatus {
            state,
            message: Some(Message::new(Role::Agent, vec![Part::text(text)])),
            timestamp: None,
        },
        metadata: None,
        generation: 0,
    })
}

impl AgentExecutor for LiveAgent {
    fn execute(
        &self,
        mut ctx: ExecutorContext,
    ) -> BoxStream<'static, Result<StreamResponse, A2AError>> {
        let release = self.release.clone();
        let first = ctx
            .message
            .as_ref()
            .and_then(|m| m.text())
            .map(str::to_owned);
        Box::pin(async_stream::stream! {
            let (task, context) = (ctx.task_id.clone(), ctx.context_id.clone());
            match first.as_deref() {
                Some("ask") => {
                    yield Ok(status(&task, &context, TaskState::InputRequired, "need input"));
                    return;
                }
                Some("quick") => {
                    yield Ok(status(&task, &context, TaskState::Completed, "done"));
                    return;
                }
                _ => {}
            }
            yield Ok(status(&task, &context, TaskState::Working, "working"));
            match ctx.inputs.take() {
                Some(mut inputs) => {
                    while let Some(input) = inputs.recv().await {
                        let text = input.message.text().unwrap_or("").to_string();
                        if text == "finish" {
                            yield Ok(status(&task, &context, TaskState::Completed, "done"));
                            return;
                        }
                        yield Ok(status(&task, &context, TaskState::Working, &format!("got: {text}")));
                    }
                }
                None => {
                    release.notified().await;
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

    fn accepts_live_input(&self) -> bool {
        self.takes_input
    }
}

fn handler(takes_input: bool) -> (Arc<DefaultRequestHandler>, Arc<Notify>) {
    let release = Arc::new(Notify::new());
    let handler = DefaultRequestHandler::new(
        LiveAgent {
            takes_input,
            release: release.clone(),
        },
        InMemoryTaskStore::new(),
    )
    .with_capabilities(AgentCapabilities {
        streaming: Some(true),
        ..AgentCapabilities::default()
    });
    (Arc::new(handler), release)
}

fn params(version: &str) -> ServiceParams {
    let mut params = ServiceParams::new();
    params.insert(SVC_PARAM_VERSION.to_string(), vec![version.to_string()]);
    params
}

fn send(text: &str, task_id: Option<&str>) -> StreamRequest {
    let mut message = Message::new(Role::User, vec![Part::text(text)]);
    message.task_id = task_id.map(Into::into);
    StreamRequest {
        tenant: None,
        payload: StreamRequestPayload::SendMessage(SendMessageRequest {
            message,
            configuration: None,
            metadata: None,
            tenant: None,
            artifact_updates: Vec::new(),
        }),
    }
}

fn subscribe(task_id: &str, last_seen: Option<i64>) -> StreamRequest {
    StreamRequest {
        tenant: None,
        payload: StreamRequestPayload::Subscribe(SubscribeToTaskRequest {
            id: task_id.into(),
            tenant: None,
            last_seen_generation: last_seen,
        }),
    }
}

/// An uplink the test feeds by hand.
fn new_uplink() -> (
    mpsc::Sender<StreamRequest>,
    BoxStream<'static, StreamRequest>,
) {
    let (sender, receiver) = mpsc::channel(8);
    (sender, ReceiverStream::new(receiver).boxed())
}

type Downlink = BoxStream<'static, Result<StreamResponse, A2AError>>;

async fn next(downlink: &mut Downlink) -> StreamResponse {
    tokio::time::timeout(Duration::from_secs(5), downlink.next())
        .await
        .expect("an event arrives")
        .expect("the stream is open")
        .expect("no transport error")
}

async fn ended(downlink: &mut Downlink) {
    let end = tokio::time::timeout(Duration::from_secs(5), downlink.next())
        .await
        .expect("the stream ends");
    assert!(end.is_none(), "expected the stream to end, got {end:?}");
}

fn task_of(event: StreamResponse) -> Task {
    match event {
        StreamResponse::Task(task) => task,
        other => panic!("expected a task, got {other:?}"),
    }
}

fn state_text(event: &StreamResponse) -> (TaskState, String) {
    match event {
        StreamResponse::StatusUpdate(update) => (
            update.status.state.clone(),
            update
                .status
                .message
                .as_ref()
                .and_then(|m| m.text())
                .unwrap_or("")
                .to_string(),
        ),
        other => panic!("expected a status update, got {other:?}"),
    }
}

fn generation_of(event: &StreamResponse) -> i64 {
    match event {
        StreamResponse::Task(task) => task.generation,
        StreamResponse::StatusUpdate(update) => update.generation,
        StreamResponse::ArtifactUpdate(update) => update.generation,
        StreamResponse::MessageUpdate(update) => update.generation,
        other => panic!("no generation on {other:?}"),
    }
}

#[tokio::test]
async fn opening_with_a_message_opens_with_the_task_that_reflects_it() {
    let (handler, _) = handler(true);
    let (up, uplink) = new_uplink();
    up.send(send("quick", None)).await.unwrap();

    let mut down = handler
        .send_live_message(&params("1.1"), uplink)
        .await
        .unwrap();

    let opening = task_of(next(&mut down).await);
    assert_eq!(opening.generation, 1);
    let timeline = opening.timeline.as_ref().unwrap();
    assert!(matches!(
        &timeline[0].content,
        TimelineContent::Message(m) if m.text() == Some("quick")
    ));
    assert_eq!(
        state_text(&next(&mut down).await),
        (TaskState::Completed, "done".into())
    );
    ended(&mut down).await;
}

#[tokio::test]
async fn a_message_sent_while_the_agent_works_reaches_it_and_is_announced() {
    let (handler, _) = handler(true);
    let (up, uplink) = new_uplink();
    up.send(send("start", None)).await.unwrap();
    let mut down = handler
        .send_live_message(&params("1.1"), uplink)
        .await
        .unwrap();
    let opening = task_of(next(&mut down).await);
    let task_id = opening.id.clone();
    assert_eq!(state_text(&next(&mut down).await).1, "working");

    up.send(send("hello", Some(&task_id))).await.unwrap();

    let announced = next(&mut down).await;
    let StreamResponse::MessageUpdate(TaskMessageUpdateEvent {
        message,
        generation,
        ..
    }) = &announced
    else {
        panic!("expected the message to be announced, got {announced:?}");
    };
    assert_eq!(message.text(), Some("hello"));
    assert!(*generation > opening.generation);
    // The agent read it and answered.
    let answer = next(&mut down).await;
    assert_eq!(state_text(&answer).1, "got: hello");
    assert!(generation_of(&answer) > *generation);

    up.send(send("finish", Some(&task_id))).await.unwrap();
    assert!(matches!(
        next(&mut down).await,
        StreamResponse::MessageUpdate(_)
    ));
    assert_eq!(
        state_text(&next(&mut down).await),
        (TaskState::Completed, "done".into())
    );
    ended(&mut down).await;

    let stored = handler
        .get_task(
            &ServiceParams::new(),
            GetTaskRequest {
                id: task_id,
                history_length: None,
                tenant: None,
                timeline_length: None,
                current_generation: None,
            },
        )
        .await
        .unwrap();
    let said: Vec<_> = stored
        .timeline
        .unwrap()
        .iter()
        .filter_map(|entry| match &entry.content {
            TimelineContent::Message(m) => m.text().map(str::to_owned),
            _ => None,
        })
        .collect();
    assert_eq!(said, ["start", "hello", "finish"]);
}

#[tokio::test]
async fn an_agent_that_takes_no_live_input_declines_the_message_and_the_stream_stays_open() {
    let (handler, release) = handler(false);
    let (up, uplink) = new_uplink();
    up.send(send("start", None)).await.unwrap();
    let mut down = handler
        .send_live_message(&params("1.1"), uplink)
        .await
        .unwrap();
    let task_id = task_of(next(&mut down).await).id;
    assert_eq!(state_text(&next(&mut down).await).1, "working");

    let rejected = send("hello", Some(&task_id));
    let message_id = match &rejected.payload {
        StreamRequestPayload::SendMessage(r) => r.message.message_id.clone(),
        _ => unreachable!(),
    };
    up.send(rejected).await.unwrap();

    match next(&mut down).await {
        StreamResponse::Error(StreamError {
            code,
            reference_id,
            task_id: id,
            ..
        }) => {
            assert_eq!(code, "UNAVAILABLE");
            assert_eq!(reference_id.as_deref(), Some(message_id.as_str()));
            assert_eq!(id, task_id);
        }
        other => panic!("expected a StreamError, got {other:?}"),
    }
    // Nothing was recorded for the declined message, and the work goes on.
    release.notify_one();
    assert_eq!(
        state_text(&next(&mut down).await),
        (TaskState::Completed, "done".into())
    );
    ended(&mut down).await;
}

#[tokio::test]
async fn a_second_subscribe_or_another_task_is_refused_without_ending_the_stream() {
    let (handler, _) = handler(true);
    let (up, uplink) = new_uplink();
    up.send(send("start", None)).await.unwrap();
    let mut down = handler
        .send_live_message(&params("1.1"), uplink)
        .await
        .unwrap();
    let task_id = task_of(next(&mut down).await).id;
    next(&mut down).await; // working

    up.send(subscribe(&task_id, None)).await.unwrap();
    match next(&mut down).await {
        StreamResponse::Error(error) => assert_eq!(error.code, "INVALID_ARGUMENT"),
        other => panic!("expected a StreamError, got {other:?}"),
    }
    up.send(send("elsewhere", Some("some-other-task")))
        .await
        .unwrap();
    match next(&mut down).await {
        StreamResponse::Error(error) => assert_eq!(error.code, "INVALID_ARGUMENT"),
        other => panic!("expected a StreamError, got {other:?}"),
    }

    // Still bound to its task and still working.
    up.send(send("finish", Some(&task_id))).await.unwrap();
    assert!(matches!(
        next(&mut down).await,
        StreamResponse::MessageUpdate(_)
    ));
    assert_eq!(state_text(&next(&mut down).await).0, TaskState::Completed);
}

#[tokio::test]
async fn the_stream_stays_open_while_the_task_waits_for_input_and_the_next_message_starts_a_turn() {
    let (handler, _) = handler(true);
    let (up, uplink) = new_uplink();
    up.send(send("ask", None)).await.unwrap();
    let mut down = handler
        .send_live_message(&params("1.1"), uplink)
        .await
        .unwrap();
    let task_id = task_of(next(&mut down).await).id;
    assert_eq!(
        state_text(&next(&mut down).await),
        (TaskState::InputRequired, "need input".into())
    );

    // The turn is over, the exchange is not.
    assert!(
        tokio::time::timeout(Duration::from_millis(150), down.next())
            .await
            .is_err(),
        "the downlink must not close at INPUT_REQUIRED"
    );
    up.send(send("quick", Some(&task_id))).await.unwrap();

    assert!(matches!(
        next(&mut down).await,
        StreamResponse::MessageUpdate(_)
    ));
    assert_eq!(
        state_text(&next(&mut down).await),
        (TaskState::Completed, "done".into())
    );
    ended(&mut down).await;
}

#[tokio::test]
async fn a_1_0_client_is_not_sent_message_updates() {
    let (handler, _) = handler(true);
    let (up, uplink) = new_uplink();
    up.send(send("start", None)).await.unwrap();
    let mut down = handler
        .send_live_message(&params("1.0"), uplink)
        .await
        .unwrap();
    let task_id = task_of(next(&mut down).await).id;
    next(&mut down).await; // working

    up.send(send("hello", Some(&task_id))).await.unwrap();

    // The agent's answer comes; the append is not announced, which shows only
    // as a gap in the generations.
    let answer = next(&mut down).await;
    assert_eq!(state_text(&answer).1, "got: hello");
    up.send(send("finish", Some(&task_id))).await.unwrap();
    assert_eq!(state_text(&next(&mut down).await).0, TaskState::Completed);
}

#[tokio::test]
async fn http_style_calls_open_the_exchange_once_and_later_calls_are_acknowledged() {
    let (handler, _) = handler(true);
    let p = params("1.1");

    let LiveResponse::Stream(mut down) = handler
        .send_live_request(&p, send("start", None))
        .await
        .unwrap()
    else {
        panic!("the first call is the downlink");
    };
    let task_id = task_of(next(&mut down).await).id;
    next(&mut down).await; // working

    let ack = handler
        .send_live_request(&p, send("hello", Some(&task_id)))
        .await
        .unwrap();
    let LiveResponse::Ack(task) = ack else {
        panic!("a later call only acknowledges");
    };
    let task = *task;
    assert_eq!(task.id, task_id);
    let messages = task
        .timeline
        .iter()
        .flatten()
        .filter(|entry| matches!(entry.content, TimelineContent::Message(_)))
        .count();
    assert_eq!(
        messages, 2,
        "the acknowledgement already reflects the message"
    );

    // Its events came on the downlink, once.
    assert!(matches!(
        next(&mut down).await,
        StreamResponse::MessageUpdate(_)
    ));
    assert_eq!(state_text(&next(&mut down).await).1, "got: hello");

    let LiveResponse::Ack(_) = handler
        .send_live_request(&p, send("finish", Some(&task_id)))
        .await
        .unwrap()
    else {
        panic!("still an acknowledgement");
    };
    assert!(matches!(
        next(&mut down).await,
        StreamResponse::MessageUpdate(_)
    ));
    assert_eq!(state_text(&next(&mut down).await).0, TaskState::Completed);
    ended(&mut down).await;
}

#[tokio::test]
async fn subscribing_first_attaches_to_a_running_task_without_sending_input() {
    let (handler, _) = handler(true);
    let p = params("1.1");
    let (up, uplink_a) = new_uplink();
    up.send(send("start", None)).await.unwrap();
    let mut owner = handler.send_live_message(&p, uplink_a).await.unwrap();
    let opening = task_of(next(&mut owner).await);
    next(&mut owner).await; // working

    let (watch_up, uplink_b) = new_uplink();
    watch_up.send(subscribe(&opening.id, None)).await.unwrap();
    let mut watcher = handler.send_live_message(&p, uplink_b).await.unwrap();
    let snapshot = task_of(next(&mut watcher).await);
    assert_eq!(snapshot.id, opening.id);

    up.send(send("finish", Some(&opening.id))).await.unwrap();
    // Both see the same message and the same end.
    assert!(matches!(
        next(&mut owner).await,
        StreamResponse::MessageUpdate(_)
    ));
    assert!(matches!(
        next(&mut watcher).await,
        StreamResponse::MessageUpdate(_)
    ));
    assert_eq!(state_text(&next(&mut owner).await).0, TaskState::Completed);
    assert_eq!(
        state_text(&next(&mut watcher).await).0,
        TaskState::Completed
    );
}

#[tokio::test]
async fn subscribing_with_the_current_generation_opens_without_a_snapshot() {
    let (handler, _) = handler(true);
    let p = params("1.1");
    let (up, uplink_a) = new_uplink();
    up.send(send("start", None)).await.unwrap();
    let mut owner = handler.send_live_message(&p, uplink_a).await.unwrap();
    let opening = task_of(next(&mut owner).await);
    let working = next(&mut owner).await;
    let current = generation_of(&working);

    let (watch_up, uplink_b) = new_uplink();
    watch_up
        .send(subscribe(&opening.id, Some(current)))
        .await
        .unwrap();
    let mut watcher = handler.send_live_message(&p, uplink_b).await.unwrap();

    up.send(send("hello", Some(&opening.id))).await.unwrap();
    // The first thing the watcher is told is what followed what it had seen.
    match next(&mut watcher).await {
        StreamResponse::MessageUpdate(update) => assert!(update.generation > current),
        other => panic!("expected the message, not a snapshot: {other:?}"),
    }
}

#[tokio::test]
async fn an_exchange_without_a_first_request_or_with_an_unknown_task_cannot_open() {
    let (handler, _) = handler(true);
    let p = params("1.1");
    let (_up, empty) = {
        let (sender, receiver) = mpsc::channel::<StreamRequest>(1);
        drop(sender);
        ((), ReceiverStream::new(receiver).boxed())
    };
    let error = handler.send_live_message(&p, empty).await.err().unwrap();
    assert_eq!(error.code, error_code::INVALID_REQUEST);

    let (up, uplink) = new_uplink();
    up.send(subscribe("no-such-task", None)).await.unwrap();
    let error = handler.send_live_message(&p, uplink).await.err().unwrap();
    assert_eq!(error.code, error_code::TASK_NOT_FOUND);

    let (up, uplink) = new_uplink();
    up.send(send("hi", Some("no-such-task"))).await.unwrap();
    let error = handler.send_live_message(&p, uplink).await.err().unwrap();
    assert_eq!(error.code, error_code::TASK_NOT_FOUND);
}

#[tokio::test]
async fn a_handler_that_does_not_override_the_live_methods_refuses_them() {
    struct Bare;
    #[async_trait::async_trait]
    impl RequestHandler for Bare {
        async fn send_message(
            &self,
            _: &ServiceParams,
            _: SendMessageRequest,
        ) -> Result<SendMessageResponse, A2AError> {
            unreachable!()
        }
        async fn send_streaming_message(
            &self,
            _: &ServiceParams,
            _: SendMessageRequest,
        ) -> Result<BoxStream<'static, Result<StreamResponse, A2AError>>, A2AError> {
            unreachable!()
        }
        async fn get_task(&self, _: &ServiceParams, _: GetTaskRequest) -> Result<Task, A2AError> {
            unreachable!()
        }
        async fn list_tasks(
            &self,
            _: &ServiceParams,
            _: ListTasksRequest,
        ) -> Result<ListTasksResponse, A2AError> {
            unreachable!()
        }
        async fn cancel_task(
            &self,
            _: &ServiceParams,
            _: CancelTaskRequest,
        ) -> Result<Task, A2AError> {
            unreachable!()
        }
        async fn subscribe_to_task(
            &self,
            _: &ServiceParams,
            _: SubscribeToTaskRequest,
        ) -> Result<BoxStream<'static, Result<StreamResponse, A2AError>>, A2AError> {
            unreachable!()
        }
        async fn create_push_config(
            &self,
            _: &ServiceParams,
            _: TaskPushNotificationConfig,
        ) -> Result<TaskPushNotificationConfig, A2AError> {
            unreachable!()
        }
        async fn get_push_config(
            &self,
            _: &ServiceParams,
            _: GetTaskPushNotificationConfigRequest,
        ) -> Result<TaskPushNotificationConfig, A2AError> {
            unreachable!()
        }
        async fn list_push_configs(
            &self,
            _: &ServiceParams,
            _: ListTaskPushNotificationConfigsRequest,
        ) -> Result<ListTaskPushNotificationConfigsResponse, A2AError> {
            unreachable!()
        }
        async fn delete_push_config(
            &self,
            _: &ServiceParams,
            _: DeleteTaskPushNotificationConfigRequest,
        ) -> Result<(), A2AError> {
            unreachable!()
        }
        async fn get_extended_agent_card(
            &self,
            _: &ServiceParams,
            _: GetExtendedAgentCardRequest,
        ) -> Result<AgentCard, A2AError> {
            unreachable!()
        }
    }
    let error = Bare
        .send_live_request(&ServiceParams::new(), send("hi", None))
        .await
        .err()
        .unwrap();
    assert_eq!(error.code, error_code::UNSUPPORTED_OPERATION);
}
