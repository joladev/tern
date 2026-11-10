use serde::Deserialize;
use types::{Did, Profile};

const PROFILE_COLLECTION: &str = "app.bsky.actor.profile";
const PROFILE_RKEY: &str = "self";
const DELETED_STATUS: &str = "deleted";

#[derive(Debug, PartialEq)]
pub enum Frame {
    Event(Event),
    Ignored,
    Failed(String),
}

#[derive(Debug, PartialEq)]
pub struct Event {
    pub seq: u64,
    pub did: Did,
    pub kind: EventKind,
    pub time: Option<jiff::Timestamp>,
}

#[derive(Debug, PartialEq)]
pub enum EventKind {
    Identity,
    Profile(Profile),
    Status { active: bool },
    Deleted,
}

#[derive(Debug, thiserror::Error)]
#[error("failed to parse Jetstream frame")]
pub struct ParseError(#[source] serde_json::Error);

#[derive(Deserialize)]
#[serde(tag = "$type")]
enum RawFrame {
    #[serde(rename = "message")]
    Message { payload: serde_json::Value },
    #[serde(rename = "error")]
    Error { error: String },
}

#[derive(Deserialize)]
#[serde(tag = "$type")]
enum Payload {
    #[serde(rename = "network.bsky.jetstream.subscribeEvents#identity")]
    Identity { seq: u64, did: Did, time: String },
    #[serde(rename = "network.bsky.jetstream.subscribeEvents#account")]
    Account {
        seq: u64,
        did: Did,
        time: String,
        account: AccountStatus,
    },
    #[serde(rename = "network.bsky.jetstream.subscribeEvents#commit")]
    Commit {
        seq: u64,
        did: Did,
        time: String,
        collection: String,
        rkey: String,
        record: Option<ProfileRecord>,
    },
    #[serde(other)]
    Other,
}

#[derive(Deserialize)]
struct AccountStatus {
    active: bool,
    status: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ProfileRecord {
    display_name: Option<String>,
    avatar: Option<Blob>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Blob {
    Current { r#ref: BlobRef },
    Legacy { cid: String },
}

impl Blob {
    fn into_cid(self) -> String {
        match self {
            Blob::Current { r#ref } => r#ref.link,
            Blob::Legacy { cid } => cid,
        }
    }
}

#[derive(Deserialize)]
struct BlobRef {
    #[serde(rename = "$link")]
    link: String,
}

impl Frame {
    pub fn parse(text: &str) -> Result<Frame, ParseError> {
        let raw: RawFrame = serde_json::from_str(text).map_err(ParseError)?;
        let payload = match raw {
            RawFrame::Error { error } => return Ok(Frame::Failed(error)),
            RawFrame::Message { payload } => payload,
        };

        let event = match serde_json::from_value(payload).map_err(ParseError)? {
            Payload::Identity { seq, did, time } => event(seq, did, &time, EventKind::Identity),
            Payload::Account {
                seq,
                did,
                time,
                account,
            } => event(seq, did, &time, account.into_kind()),
            Payload::Commit {
                seq,
                did,
                time,
                collection,
                record,
                rkey,
            } if collection == PROFILE_COLLECTION && rkey == PROFILE_RKEY => {
                let profile = record.map(Profile::from).unwrap_or_default();
                event(seq, did, &time, EventKind::Profile(profile))
            }
            Payload::Commit { .. } | Payload::Other => {
                return Ok(Frame::Ignored);
            }
        };
        Ok(Frame::Event(event))
    }
}

fn event(seq: u64, did: Did, time: &str, kind: EventKind) -> Event {
    Event {
        seq,
        did,
        kind,
        time: time.parse().ok(),
    }
}

impl AccountStatus {
    fn into_kind(self) -> EventKind {
        if self.status.as_deref() == Some(DELETED_STATUS) {
            return EventKind::Deleted;
        }
        EventKind::Status {
            active: self.active,
        }
    }
}

impl From<ProfileRecord> for Profile {
    fn from(record: ProfileRecord) -> Profile {
        Profile {
            display_name: types::display_name(record.display_name),
            avatar_cid: record.avatar.map(Blob::into_cid),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn did() -> Did {
        Did::try_from("did:plc:alice".to_owned()).unwrap()
    }

    fn event(seq: u64, did: Did, time: &str, kind: EventKind) -> Frame {
        Frame::Event(super::event(seq, did, time, kind))
    }

    #[test]
    fn parses_a_profile_commit() {
        let frame = Frame::parse(
            r#"{"$type":"message","payload":{
                "$type":"network.bsky.jetstream.subscribeEvents#commit",
                "cid":"bafyrei","collection":"app.bsky.actor.profile",
                "did":"did:plc:alice","operation":"update",
                "record":{"$type":"app.bsky.actor.profile",
                    "avatar":{"$type":"blob","mimeType":"image/jpeg",
                        "ref":{"$link":"bafkrei"},"size":1},
                    "createdAt":"2024-11-16T16:08:49.713Z",
                    "displayName":"Alice"},
                "rev":"3mzb","rkey":"self","seq":7,
                "time":"t"}}"#,
        )
        .unwrap();

        let profile = Profile {
            display_name: Some("Alice".to_owned()),
            avatar_cid: Some("bafkrei".to_owned()),
        };
        let kind = EventKind::Profile(profile);
        assert_eq!(frame, event(7, did(), "t", kind));
    }

    #[test]
    fn reads_legacy_avatar_blobs() {
        let frame = Frame::parse(
            r#"{"$type":"message","payload":{
                "$type":"network.bsky.jetstream.subscribeEvents#commit",
                "collection":"app.bsky.actor.profile","did":"did:plc:alice",
                "operation":"update","rev":"3mzb","rkey":"self","seq":7,
                "time":"t","record":{"avatar":{
                    "cid":"bafkreilegacy","mimeType":"image/jpeg"}}}}"#,
        )
        .unwrap();

        let profile = Profile {
            avatar_cid: Some("bafkreilegacy".to_owned()),
            ..Profile::default()
        };
        let kind = EventKind::Profile(profile);
        assert_eq!(frame, event(7, did(), "t", kind));
    }

    #[test]
    fn a_deleted_profile_record_clears_the_profile() {
        let frame = Frame::parse(
            r#"{"$type":"message","payload":{
                "$type":"network.bsky.jetstream.subscribeEvents#commit",
                "collection":"app.bsky.actor.profile",
                "did":"did:plc:alice","operation":"delete",
                "rev":"3mzb","rkey":"self","seq":8,"time":"t"}}"#,
        )
        .unwrap();

        let kind = EventKind::Profile(Profile::default());
        assert_eq!(frame, event(8, did(), "t", kind));
    }

    #[test]
    fn parses_an_identity_event_without_a_handle() {
        let frame = Frame::parse(
            r#"{"$type":"message","payload":{
                "$type":"network.bsky.jetstream.subscribeEvents#identity",
                "did":"did:plc:alice","seq":9,"time":"t",
                "identity":{"did":"did:plc:alice","seq":1,"time":"t"}}}"#,
        )
        .unwrap();

        let kind = EventKind::Identity;
        assert_eq!(frame, event(9, did(), "t", kind));
    }

    #[test]
    fn distinguishes_deleted_accounts_from_deactivated_ones() {
        let account = |status: &str| {
            let text = format!(
                r#"{{"$type":"message","payload":{{
                "$type":"network.bsky.jetstream.subscribeEvents#account",
                "did":"did:plc:alice","seq":10,"time":"t",
                "account":{{"active":false,"status":"{status}"}}}}}}"#,
            );
            match Frame::parse(&text).unwrap() {
                Frame::Event(event) => event.kind,
                frame => panic!("unexpected frame {frame:?}"),
            }
        };

        assert_eq!(account("deleted"), EventKind::Deleted);
        assert_eq!(account("deactivated"), EventKind::Status { active: false });
    }

    #[test]
    fn keeps_the_event_time() {
        let frame = Frame::parse(
            r#"{"$type":"message","payload":{
                "$type":"network.bsky.jetstream.subscribeEvents#identity",
                "did":"did:plc:alice","seq":9,
                "time":"2026-11-01T21:23:20.065112Z"}}"#,
        )
        .unwrap();

        let Frame::Event(event) = frame else {
            panic!("expected an event");
        };
        assert_eq!(event.time.unwrap().as_second(), 1_793_568_200);
    }

    #[test]
    fn notices_and_other_kinds_are_ignored() {
        let frame = Frame::parse(
            r#"{"$type":"message","payload":{
                "$type":"network.bsky.jetstream.subscribeEvents#info",
                "name":"FutureCursor"}}"#,
        )
        .unwrap();
        assert_eq!(frame, Frame::Ignored);
    }

    #[test]
    fn error_frames_are_reported() {
        let frame =
            Frame::parse(r#"{"$type":"error","error":"ConsumerTooSlow","message":"m"}"#).unwrap();
        assert_eq!(frame, Frame::Failed("ConsumerTooSlow".to_owned()));
    }
}
