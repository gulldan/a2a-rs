// Copyright AGNTCY Contributors (https://github.com/agntcy)
// Copyright A2A Contributors (https://github.com/a2aproject)
// SPDX-License-Identifier: Apache-2.0
//! Round trips for the fields and messages added in A2A 1.1, through both the
//! protobuf and the ProtoJSON representations.
use a2a::event::{StreamError, TaskMessageUpdateEvent};
use a2a::*;
use a2a_pb::{pbconv, protojson_conv};
use serde_json::json;

fn agent_message(text: &str) -> Message {
    Message::new(Role::Agent, vec![Part::text(text)])
}

fn working(message: Option<Message>) -> TaskStatus {
    TaskStatus {
        state: TaskState::Working,
        message,
        timestamp: None,
    }
}

fn sample_task() -> Task {
    Task {
        id: "t1".into(),
        context_id: "c1".into(),
        status: working(None),
        artifacts: Some(vec![Artifact {
            artifact_id: "a1".into(),
            name: None,
            description: None,
            parts: vec![Part::text("out")],
            metadata: None,
            extensions: None,
            start_generation: Some(2),
            end_generation: Some(5),
            role: Some(Role::User),
        }]),
        history: None,
        timeline: Some(vec![
            TimelineEntry {
                content: TimelineContent::Message(Message::new(Role::User, vec![Part::text("hi")])),
                generation: 1,
            },
            TimelineEntry {
                content: TimelineContent::Status(working(Some(agent_message("on it")))),
                generation: 3,
            },
        ]),
        metadata: None,
        generation: 6,
    }
}

fn proto_json_round_trip<T>(value: &T) -> T
where
    T: protojson_conv::ProtoJsonPayload,
{
    let json = protojson_conv::to_value(value).unwrap();
    protojson_conv::from_value(json).unwrap()
}

#[test]
fn task_timeline_and_generation_survive_protobuf_and_protojson() {
    let task = sample_task();

    assert_eq!(pbconv::from_proto_task(&pbconv::to_proto_task(&task)), task);
    assert_eq!(proto_json_round_trip(&task), task);
}

#[test]
fn int64_fields_are_json_strings_on_the_wire() {
    let json = protojson_conv::to_value(&sample_task()).unwrap();

    assert_eq!(json["generation"], json!("6"));
    assert_eq!(json["timeline"][1]["generation"], json!("3"));
    assert_eq!(json["artifacts"][0]["startGeneration"], json!("2"));
    assert_eq!(json["artifacts"][0]["role"], json!("ROLE_USER"));
    assert!(json["timeline"][0].get("message").is_some());
    assert!(json["timeline"][1].get("status").is_some());
}

#[test]
fn native_serde_matches_the_wire_form_for_int64() {
    let task = sample_task();

    let json = serde_json::to_value(&task).unwrap();
    assert_eq!(json["generation"], json!("6"));
    assert_eq!(json["timeline"][0]["generation"], json!("1"));

    let numeric = json!({
        "id": "t1", "contextId": "c1", "status": {"state": "TASK_STATE_WORKING"},
        "generation": 6
    });
    let task: Task = serde_json::from_value(numeric).unwrap();
    assert_eq!(task.generation, 6);
}

#[test]
fn pre_1_1_task_decodes_with_no_timeline_and_generation_zero() {
    let json = json!({
        "id": "t1", "contextId": "c1", "status": {"state": "TASK_STATE_COMPLETED"}
    });

    let task: Task = protojson_conv::from_value(json).unwrap();

    assert_eq!(task.generation, 0);
    assert!(task.timeline.is_none());
    let back = protojson_conv::to_value(&task).unwrap();
    assert!(back.get("generation").is_none(), "{back}");
    assert!(back.get("timeline").is_none(), "{back}");
}

#[test]
fn unknown_timeline_entry_kind_is_dropped() {
    let json = json!({
        "id": "t1", "contextId": "c1", "status": {"state": "TASK_STATE_WORKING"},
        "timeline": [
            {"somethingNew": {"x": 1}, "generation": "1"},
            {"status": {"state": "TASK_STATE_WORKING"}, "generation": "2"}
        ]
    });

    let task: Task = protojson_conv::from_value(json).unwrap();

    let timeline = task.timeline.unwrap();
    assert_eq!(timeline.len(), 1);
    assert_eq!(timeline[0].generation, 2);
}

#[test]
fn request_fields_survive_protobuf_and_protojson() {
    let send = SendMessageRequest {
        message: Message::new(Role::User, vec![Part::text("hi")]),
        configuration: Some(SendMessageConfiguration {
            accepted_output_modes: None,
            task_push_notification_config: Some(TaskPushNotificationConfig {
                url: "https://example.com/hook".into(),
                id: None,
                task_id: String::new(),
                token: None,
                authentication: None,
                tenant: None,
                protocol_version: Some("1.1".into()),
            }),
            history_length: None,
            timeline_length: Some(4),
            return_immediately: None,
            if_generation_match: Some(7),
        }),
        metadata: None,
        tenant: None,
        artifact_updates: vec![event::TaskArtifactUpdateEvent {
            task_id: "t1".into(),
            context_id: "c1".into(),
            artifact: Artifact {
                artifact_id: "a1".into(),
                name: None,
                description: None,
                parts: vec![Part::text("from client")],
                metadata: None,
                extensions: None,
                start_generation: None,
                end_generation: None,
                role: Some(Role::User),
            },
            append: None,
            last_chunk: None,
            metadata: None,
            generation: 0,
        }],
    };
    assert_eq!(
        pbconv::from_proto_send_message_request(&pbconv::to_proto_send_message_request(&send)),
        send
    );
    assert_eq!(proto_json_round_trip(&send), send);

    let get = GetTaskRequest {
        id: "t1".into(),
        history_length: None,
        tenant: None,
        timeline_length: Some(2),
        current_generation: Some(9),
    };
    assert_eq!(
        pbconv::from_proto_get_task_request(&pbconv::to_proto_get_task_request(&get)),
        get
    );
    assert_eq!(proto_json_round_trip(&get), get);

    let subscribe = SubscribeToTaskRequest {
        id: "t1".into(),
        tenant: None,
        last_seen_generation: Some(12),
    };
    assert_eq!(
        pbconv::from_proto_subscribe_to_task_request(&pbconv::to_proto_subscribe_to_task_request(
            &subscribe
        )),
        subscribe
    );
    assert_eq!(proto_json_round_trip(&subscribe), subscribe);

    let list = ListTasksRequest {
        context_id: None,
        status: None,
        page_size: None,
        page_token: None,
        history_length: None,
        timeline_length: Some(3),
        status_timestamp_after: None,
        include_artifacts: None,
        tenant: None,
    };
    assert_eq!(proto_json_round_trip(&list), list);
}

#[test]
fn user_artifacts_capability_survives() {
    let capabilities = AgentCapabilities {
        user_artifacts: Some(true),
        ..AgentCapabilities::default()
    };

    let back =
        pbconv::from_proto_agent_capabilities(&pbconv::to_proto_agent_capabilities(&capabilities));

    assert_eq!(back, capabilities);
    let json = serde_json::to_value(&capabilities).unwrap();
    assert_eq!(json["userArtifacts"], json!(true));
}

#[test]
fn new_stream_payloads_round_trip() {
    let message_update = StreamResponse::MessageUpdate(TaskMessageUpdateEvent {
        task_id: "t1".into(),
        context_id: "c1".into(),
        message: Message::new(Role::User, vec![Part::text("more input")]),
        metadata: None,
        generation: 4,
    });
    let error = StreamResponse::Error(StreamError {
        task_id: "t1".into(),
        code: "INVALID_ARGUMENT".into(),
        message: "bad request".into(),
        reference_id: Some("m1".into()),
        metadata: None,
    });

    for response in [message_update, error] {
        let via_proto =
            pbconv::from_proto_stream_response(&pbconv::to_proto_stream_response(&response));
        assert_eq!(via_proto.as_ref(), Some(&response));
        assert_eq!(proto_json_round_trip(&response), response);
    }

    let json = protojson_conv::to_value(&StreamResponse::MessageUpdate(TaskMessageUpdateEvent {
        task_id: "t1".into(),
        context_id: "c1".into(),
        message: agent_message("x"),
        metadata: None,
        generation: 4,
    }))
    .unwrap();
    assert!(json.get("messageUpdate").is_some(), "{json}");
}

#[test]
fn event_generation_survives() {
    let status = StreamResponse::StatusUpdate(event::TaskStatusUpdateEvent {
        task_id: "t1".into(),
        context_id: "c1".into(),
        status: working(None),
        metadata: None,
        generation: 8,
    });
    let artifact = StreamResponse::ArtifactUpdate(event::TaskArtifactUpdateEvent {
        task_id: "t1".into(),
        context_id: "c1".into(),
        artifact: Artifact {
            artifact_id: "a1".into(),
            name: None,
            description: None,
            parts: vec![],
            metadata: None,
            extensions: None,
            start_generation: None,
            end_generation: None,
            role: None,
        },
        append: None,
        last_chunk: None,
        metadata: None,
        generation: 9,
    });

    for response in [status, artifact] {
        assert_eq!(proto_json_round_trip(&response), response);
    }
}

#[test]
fn stream_request_round_trips() {
    let send = StreamRequest {
        tenant: Some("acme".into()),
        payload: StreamRequestPayload::SendMessage(SendMessageRequest {
            message: Message::new(Role::User, vec![Part::text("hi")]),
            configuration: None,
            metadata: None,
            tenant: None,
            artifact_updates: vec![],
        }),
    };
    let subscribe = StreamRequest {
        tenant: None,
        payload: StreamRequestPayload::Subscribe(SubscribeToTaskRequest {
            id: "t1".into(),
            tenant: None,
            last_seen_generation: Some(3),
        }),
    };

    for request in [send, subscribe] {
        let back = pbconv::from_proto_stream_request(&pbconv::to_proto_stream_request(&request));
        assert_eq!(back.as_ref(), Some(&request));
    }
}

#[test]
fn artifact_only_request_has_no_message_on_the_wire() {
    let mut request = SendMessageRequest {
        message: Message::new(Role::User, vec![]),
        configuration: None,
        metadata: None,
        tenant: None,
        artifact_updates: vec![event::TaskArtifactUpdateEvent {
            task_id: "t1".into(),
            context_id: String::new(),
            artifact: Artifact {
                artifact_id: "a1".into(),
                name: None,
                description: None,
                parts: vec![Part::text("x")],
                metadata: None,
                extensions: None,
                start_generation: None,
                end_generation: None,
                role: None,
            },
            append: None,
            last_chunk: None,
            metadata: None,
            generation: 0,
        }],
    };
    request.message.message_id = String::new();

    let proto = pbconv::to_proto_send_message_request(&request);
    assert!(proto.message.is_none(), "the message is omitted");
    let back = pbconv::from_proto_send_message_request(&proto);
    assert!(
        back.message.message_id.is_empty(),
        "and comes back marked as absent"
    );
    assert_eq!(back.artifact_updates, request.artifact_updates);

    let json = protojson_conv::to_value(&request).unwrap();
    assert!(json.get("message").is_none(), "{json}");
    let from_json: SendMessageRequest = protojson_conv::from_value(json).unwrap();
    assert!(from_json.message.message_id.is_empty());

    // Without artifacts an absent message still decodes as before, with a
    // fresh id, so only the artifact-only case is marked.
    let decoded =
        pbconv::from_proto_send_message_request(&a2a_pb::proto::SendMessageRequest::default());
    assert!(!decoded.message.message_id.is_empty());
}

#[test]
fn native_serde_skips_unknown_timeline_entries_and_null_generations() {
    let json = json!({
        "id": "t1", "contextId": "c1", "status": {"state": "TASK_STATE_WORKING"},
        "generation": null,
        "timeline": [
            {"somethingNew": {"x": 1}, "generation": "1"},
            {"status": {"state": "TASK_STATE_WORKING"}, "generation": null},
            {"message": {"messageId": "m", "role": "ROLE_USER", "parts": []}, "generation": 3}
        ]
    });

    let task: Task = serde_json::from_value(json).unwrap();

    assert_eq!(task.generation, 0);
    let timeline = task.timeline.unwrap();
    assert_eq!(
        timeline.len(),
        2,
        "the unknown kind is skipped: {timeline:?}"
    );
    assert_eq!(timeline[1].generation, 3);
}
