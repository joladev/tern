use reqwest::{Client as HttpClient, StatusCode};
use serde::Deserialize;
use types::{Did, Enrichment, Handle, Label, Profile};

const GET_PROFILES_PATH: &str = "/xrpc/app.bsky.actor.getProfiles";

pub const MAX_ACCOUNTS_PER_CALL: usize = 25;

#[derive(Clone)]
pub struct Client {
    http: HttpClient,
    base_url: String,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to reach the AppView")]
    Request(#[source] reqwest::Error),
    #[error("the AppView returned status {0}")]
    Status(StatusCode),
    #[error("failed to read the AppView's response")]
    Body(#[source] reqwest::Error),
}

#[derive(Clone, Debug, PartialEq)]
pub struct ProfileView {
    pub did: Did,
    pub handle: Option<Handle>,
    pub profile: Profile,
    pub enrichment: Enrichment,
    pub labels: Vec<Label>,
}

#[derive(Deserialize)]
struct Profiles {
    profiles: Vec<RawProfile>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawProfile {
    did: Did,
    handle: String,
    display_name: Option<String>,
    avatar: Option<String>,
    created_at: Option<String>,
    #[serde(default)]
    followers_count: i64,
    #[serde(default)]
    labels: Vec<RawLabel>,
}

#[derive(Deserialize)]
struct RawLabel {
    src: String,
    uri: String,
    cid: Option<String>,
    val: String,
    cts: String,
    exp: Option<String>,
}

impl RawLabel {
    fn into_label(self) -> Option<Label> {
        Some(Label {
            src: Did::try_from(self.src).ok()?,
            uri: self.uri,
            cid: self.cid,
            val: self.val,
            cts: self.cts,
            exp: self.exp,
        })
    }
}

impl From<RawProfile> for ProfileView {
    fn from(raw: RawProfile) -> ProfileView {
        let profile = Profile {
            display_name: types::display_name(raw.display_name),
            avatar_cid: raw.avatar.as_deref().and_then(avatar_cid),
        };
        let enrichment = Enrichment {
            followers: count(raw.followers_count),
            created_at: raw
                .created_at
                .and_then(|time| time.parse::<jiff::Timestamp>().ok())
                .map(|time| time.as_second()),
        };
        let handle = Handle::try_from(raw.handle).ok();
        let labels = raw
            .labels
            .into_iter()
            .filter_map(RawLabel::into_label)
            .collect();
        ProfileView {
            did: raw.did,
            handle,
            profile,
            enrichment,
            labels,
        }
    }
}

fn count(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0)
}

fn avatar_cid(url: &str) -> Option<String> {
    let name = url.rsplit('/').next()?;
    let cid = name.split('@').next()?;
    (!cid.is_empty()).then(|| cid.to_owned())
}

impl Client {
    pub fn new(http: HttpClient, base_url: String) -> Client {
        Client { http, base_url }
    }

    pub async fn profiles(&self, dids: &[Did]) -> Result<Vec<ProfileView>, Error> {
        let actors: Vec<_> = dids.iter().map(|did| ("actors", did.as_str())).collect();

        let response = self
            .http
            .get(format!("{}{GET_PROFILES_PATH}", self.base_url))
            .query(&actors)
            .send()
            .await
            .map_err(Error::Request)?;

        let status = response.status();
        if !status.is_success() {
            return Err(Error::Status(status));
        }

        let body: Profiles = response.json().await.map_err(Error::Body)?;
        Ok(body.profiles.into_iter().map(ProfileView::from).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_labels_from_a_profile() {
        let profile = r#"{
            "did": "did:plc:x",
            "handle": "x.test",
            "labels": [{
                "src": "did:plc:ar7c4by46qjdydhdevvrndac",
                "uri": "at://did:plc:x/app.bsky.actor.profile/self",
                "cid": "bafyrei",
                "val": "porn",
                "cts": "2026-03-09T14:53:09.930Z",
                "ver": 1
            }]
        }"#;
        let raw: RawProfile = serde_json::from_str(profile).unwrap();

        let view = ProfileView::from(raw);

        let label = Label {
            src: Did::try_from("did:plc:ar7c4by46qjdydhdevvrndac".to_owned()).unwrap(),
            uri: "at://did:plc:x/app.bsky.actor.profile/self".to_owned(),
            cid: Some("bafyrei".to_owned()),
            val: "porn".to_owned(),
            cts: "2026-03-09T14:53:09.930Z".to_owned(),
            exp: None,
        };
        assert_eq!(view.labels, [label]);
    }

    #[test]
    fn reads_the_cid_from_an_avatar_url() {
        let url = "https://cdn.bsky.app/img/avatar/plain/did:plc:x/bafkrei@jpeg";
        assert_eq!(avatar_cid(url).as_deref(), Some("bafkrei"));
        let plain = "https://cdn.bsky.app/img/avatar/plain/did:plc:x/bafkrei";
        assert_eq!(avatar_cid(plain).as_deref(), Some("bafkrei"));
    }
}
