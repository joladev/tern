use reqwest::{Client as HttpClient, StatusCode};
use serde::Deserialize;
use types::{Did, Handle, Profile, ProfileRecord};

const RESOLVE_PATH: &str = "/xrpc/com.bad-example.identity.resolveMiniDoc";
const GET_RECORD_PATH: &str = "/xrpc/com.atproto.repo.getRecord";
const PROFILE_COLLECTION: &str = "app.bsky.actor.profile";
const PROFILE_RKEY: &str = "self";
const RECORD_NOT_FOUND: &str = "RecordNotFound";

#[derive(Clone)]
pub struct Client {
    http: HttpClient,
    base_url: String,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to reach Slingshot")]
    Request(#[source] reqwest::Error),
    #[error("Slingshot returned status {0}")]
    Status(StatusCode),
    #[error("failed to read Slingshot's response")]
    Body(#[source] reqwest::Error),
}

#[derive(Clone, Debug, PartialEq)]
pub enum Resolution {
    Handle(Handle),
    Invalid,
    Unknown,
}

#[derive(Deserialize)]
struct MiniDoc {
    handle: String,
}

#[derive(Deserialize)]
struct Record {
    cid: Option<String>,
    value: ProfileRecord,
}

#[derive(Deserialize)]
struct Failure {
    error: String,
}

impl Client {
    pub fn new(http: HttpClient, base_url: String) -> Client {
        Client { http, base_url }
    }

    pub async fn resolve_handle(&self, did: &Did) -> Result<Resolution, Error> {
        let response = self
            .http
            .get(format!("{}{RESOLVE_PATH}", self.base_url))
            .query(&[("identifier", did.as_str())])
            .send()
            .await
            .map_err(Error::Request)?;

        match response.status() {
            StatusCode::OK => {}
            StatusCode::BAD_REQUEST => return Ok(Resolution::Unknown),
            status => return Err(Error::Status(status)),
        }

        let doc: MiniDoc = response.json().await.map_err(Error::Body)?;
        Ok(Handle::try_from(doc.handle).map_or(Resolution::Invalid, Resolution::Handle))
    }

    pub async fn profile(&self, did: &Did) -> Result<Profile, Error> {
        let response = self
            .http
            .get(format!("{}{GET_RECORD_PATH}", self.base_url))
            .query(&[
                ("repo", did.as_str()),
                ("collection", PROFILE_COLLECTION),
                ("rkey", PROFILE_RKEY),
            ])
            .send()
            .await
            .map_err(Error::Request)?;

        match response.status() {
            StatusCode::OK => {
                let record: Record = response.json().await.map_err(Error::Body)?;
                Ok(Profile::from_record(record.value, record.cid))
            }
            StatusCode::BAD_REQUEST => {
                let failure: Failure = response.json().await.map_err(Error::Body)?;
                profile_without_record(&failure)
            }
            status => Err(Error::Status(status)),
        }
    }
}

fn profile_without_record(failure: &Failure) -> Result<Profile, Error> {
    if failure.error == RECORD_NOT_FOUND {
        return Ok(Profile::default());
    }
    Err(Error::Status(StatusCode::BAD_REQUEST))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_the_profile_from_a_record() {
        let body = r#"{
            "uri": "at://did:plc:x/app.bsky.actor.profile/self",
            "cid": "bafyrei",
            "value": {
                "$type": "app.bsky.actor.profile",
                "displayName": "Alice",
                "avatar": {"$type": "blob", "ref": {"$link": "bafkrei"}, "size": 1}
            }
        }"#;
        let record: Record = serde_json::from_str(body).unwrap();

        let profile = Profile {
            display_name: Some("Alice".to_owned()),
            avatar_cid: Some("bafkrei".to_owned()),
            self_labels: None,
        };
        assert_eq!(Profile::from_record(record.value, record.cid), profile);
    }

    #[test]
    fn an_account_without_a_profile_record_has_an_empty_profile() {
        let missing = Failure {
            error: RECORD_NOT_FOUND.to_owned(),
        };
        assert_eq!(
            profile_without_record(&missing).unwrap(),
            Profile::default()
        );

        let other = Failure {
            error: "InvalidRequest".to_owned(),
        };
        assert!(profile_without_record(&other).is_err());
    }
}
