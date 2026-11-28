mod slurs;

use std::collections::HashMap;
use std::fmt;

use serde::{Deserialize, Serialize};

const INVALID_HANDLE: &str = "handle.invalid";
const AT_SCHEME: &str = "at://";
const PROFILE_PATH: &str = "/app.bsky.actor.profile/self";
const EPOCH: &str = "1970-01-01T00:00:00.000Z";
const MAX_DISPLAY_NAME_BYTES: usize = 640;

#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(try_from = "String", into = "String")]
pub struct Did(String);

impl Did {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for Did {
    type Error = ParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.starts_with("did:") && value.is_ascii() {
            return Ok(Did(value));
        }
        Err(ParseError::Did(value))
    }
}

impl std::str::FromStr for Did {
    type Err = ParseError;

    fn from_str(text: &str) -> Result<Did, ParseError> {
        Did::try_from(text.to_owned())
    }
}

impl From<Did> for String {
    fn from(did: Did) -> String {
        did.0
    }
}

impl fmt::Display for Did {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "String", into = "String")]
pub struct Handle(String);

impl Handle {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for Handle {
    type Error = ParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let handle = value.to_ascii_lowercase();
        if handle.is_ascii() && handle.contains('.') && handle != INVALID_HANDLE {
            return Ok(Handle(handle));
        }
        Err(ParseError::Handle(value))
    }
}

impl From<Handle> for String {
    fn from(handle: Handle) -> String {
        handle.0
    }
}

impl fmt::Display for Handle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(try_from = "String", into = "String")]
pub struct Datetime {
    text: String,
    timestamp: jiff::Timestamp,
}

impl Datetime {
    pub fn as_str(&self) -> &str {
        &self.text
    }

    pub fn timestamp(&self) -> jiff::Timestamp {
        self.timestamp
    }
}

impl TryFrom<String> for Datetime {
    type Error = ParseError;

    fn try_from(text: String) -> Result<Self, Self::Error> {
        match text.parse() {
            Ok(timestamp) => Ok(Datetime { text, timestamp }),
            Err(_) => Err(ParseError::Datetime(text)),
        }
    }
}

impl From<Datetime> for String {
    fn from(datetime: Datetime) -> String {
        datetime.text
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("invalid DID `{0}`")]
    Did(String),
    #[error("invalid handle `{0}`")]
    Handle(String),
    #[error("invalid date and time `{0}`")]
    Datetime(String),
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Account {
    pub handle: Option<Handle>,
    pub profile: Profile,
    pub active: bool,
    pub verification: Option<AppviewVerification>,
}

impl Default for Account {
    fn default() -> Account {
        Account {
            handle: None,
            profile: Profile::default(),
            active: true,
            verification: None,
        }
    }
}

impl Account {
    pub fn searchable_handle(&self) -> Option<&Handle> {
        self.handle.as_ref().filter(|handle| {
            self.active && (self.is_verified() || !slurs::contains_slur(handle.as_str()))
        })
    }

    pub fn current_verification(&self) -> Option<&VerificationState> {
        let verification = self.verification.as_ref()?;
        let unchanged = verification.handle == self.handle
            && verification.display_name == self.profile.display_name;
        unchanged.then_some(&verification.state)
    }

    fn is_verified(&self) -> bool {
        self.current_verification()
            .is_some_and(VerificationState::is_verified)
    }
}

/// A verification is for one handle and one display name. If the account changes either,
/// the verification no longer applies. We store that handle and name next to the
/// verification, and only show it while the account still has both.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct AppviewVerification {
    pub handle: Option<Handle>,
    pub display_name: Option<String>,
    pub state: VerificationState,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct VerificationState {
    pub verifications: Vec<Verification>,
    pub verified_status: String,
    pub trusted_verifier_status: String,
}

impl VerificationState {
    pub fn is_verified(&self) -> bool {
        self.verified_status == "valid"
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Verification {
    pub issuer: Did,
    pub issuer_display_name: Option<String>,
    pub issuer_handle: Option<String>,
    pub uri: String,
    pub is_valid: bool,
    pub created_at: Datetime,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct Profile {
    pub display_name: Option<String>,
    pub avatar_cid: Option<String>,
    pub self_labels: Option<SelfLabels>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct SelfLabels {
    pub values: Vec<String>,
    pub cid: Option<String>,
    pub created_at: Option<Datetime>,
}

impl SelfLabels {
    pub fn labels(&self, did: &Did) -> Vec<Label> {
        // Self-labels don't have a timestamp, but the external label format expects it.
        // Bluesky handle this by setting it to the createdAt of the profile, and if it
        // doesn't exist, defaults to `1970-01-01`.
        let cts = self.created_at.clone().unwrap_or_else(|| {
            Datetime::try_from(EPOCH.to_owned()).expect("the epoch is a valid time")
        });
        let label = |val: &String| Label {
            src: did.clone(),
            uri: profile_uri(did),
            cid: self.cid.clone(),
            val: val.clone(),
            cts: cts.clone(),
            exp: None,
        };
        self.values.iter().map(label).collect()
    }
}

fn profile_uri(did: &Did) -> String {
    format!("{AT_SCHEME}{did}{PROFILE_PATH}")
}

pub fn display_name(name: Option<String>) -> Option<String> {
    let mut name = name.filter(|name| !name.is_empty())?;
    name.truncate(name.floor_char_boundary(MAX_DISPLAY_NAME_BYTES));
    Some(name)
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProfileRecord {
    display_name: Option<String>,
    avatar: Option<Blob>,
    labels: Option<RecordLabels>,
    created_at: Option<String>,
}

#[derive(Deserialize)]
struct RecordLabels {
    #[serde(default)]
    values: Vec<RecordLabel>,
}

#[derive(Deserialize)]
struct RecordLabel {
    val: String,
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

impl Profile {
    pub fn from_record(record: ProfileRecord, cid: Option<String>) -> Profile {
        let labels = record.labels.map(|labels| labels.values);
        let values: Vec<String> = labels
            .unwrap_or_default()
            .into_iter()
            .map(|label| label.val)
            .collect();
        let created_at = record
            .created_at
            .and_then(|time| Datetime::try_from(time).ok());
        let self_labels = (!values.is_empty()).then_some(SelfLabels {
            values,
            cid,
            created_at,
        });
        Profile {
            display_name: display_name(record.display_name),
            avatar_cid: record.avatar.map(Blob::into_cid),
            self_labels,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Label {
    pub src: Did,
    pub uri: String,
    pub cid: Option<String>,
    pub val: String,
    pub cts: Datetime,
    pub exp: Option<Datetime>,
}

impl Label {
    pub fn is_same_label(&self, other: &Label) -> bool {
        (&self.src, &self.uri, &self.cid, &self.val)
            == (&other.src, &other.uri, &other.cid, &other.val)
    }

    pub fn account(&self) -> Option<Did> {
        let profile = self.uri.strip_prefix(AT_SCHEME);
        let owner = profile.and_then(|rest| rest.strip_suffix(PROFILE_PATH));
        Did::try_from(owner.unwrap_or(&self.uri).to_owned()).ok()
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct LabelRecord {
    pub label: Label,
    pub withdrawn: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct LabelJob {
    pub did: Did,
    pub records: Vec<LabelRecord>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct Enrichment {
    pub followers: u64,
    pub created_at: Option<Datetime>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum HandleFact {
    Resolved(Handle),
    Invalid,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
pub enum Status {
    Active,
    Inactive,
    Deleted,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct Facts {
    pub handle: Option<HandleFact>,
    pub profile: Option<Profile>,
    pub status: Option<Status>,
    pub enrichment: Option<Enrichment>,
    pub verification: Option<Option<AppviewVerification>>,
}

impl Facts {
    pub fn or(self, other: Facts) -> Facts {
        Facts {
            handle: self.handle.or(other.handle),
            profile: self.profile.or(other.profile),
            status: self.status.or(other.status),
            enrichment: self.enrichment.or(other.enrichment),
            verification: self.verification.or(other.verification),
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Job {
    pub did: Did,
    pub seen: Facts,
}

#[derive(Debug, Default, PartialEq)]
pub struct Fetched {
    pub facts: HashMap<Did, Facts>,
    pub failed: Vec<Did>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handles_are_normalized_to_lowercase() {
        let handle = Handle::try_from("Alice.BSKY.social".to_owned()).unwrap();
        assert_eq!(handle.as_str(), "alice.bsky.social");
    }

    #[test]
    fn the_invalid_handle_marker_is_rejected() {
        assert!(Handle::try_from("handle.invalid".to_owned()).is_err());
    }

    #[test]
    fn display_names_are_cut_to_the_lexicon_limit() {
        let name = display_name(Some("東".repeat(300))).unwrap();
        assert_eq!(name, "東".repeat(213));
        assert_eq!(display_name(Some(String::new())), None);
    }

    #[test]
    fn self_labels_become_labels_issued_by_the_account() {
        let did = Did::try_from("did:plc:alice".to_owned()).unwrap();
        let self_labels = SelfLabels {
            values: vec!["porn".to_owned()],
            cid: Some("bafyrei".to_owned()),
            created_at: None,
        };

        let labels = self_labels.labels(&did);

        assert_eq!(labels.len(), 1);
        assert_eq!(labels[0].src, did);
        assert_eq!(
            labels[0].uri,
            "at://did:plc:alice/app.bsky.actor.profile/self"
        );
        assert_eq!(labels[0].val, "porn");
        assert_eq!(labels[0].cts.as_str(), "1970-01-01T00:00:00.000Z");
    }

    #[test]
    fn a_label_on_an_account_or_its_profile_is_about_that_account() {
        let alice = Did::try_from("did:plc:alice".to_owned()).unwrap();
        let label = |uri: &str| Label {
            src: alice.clone(),
            uri: uri.to_owned(),
            cid: None,
            val: "spam".to_owned(),
            cts: Datetime::try_from(EPOCH.to_owned()).unwrap(),
            exp: None,
        };

        let on_account = label("did:plc:alice");
        let on_profile = label("at://did:plc:alice/app.bsky.actor.profile/self");
        let on_post = label("at://did:plc:alice/app.bsky.feed.post/3k");

        assert_eq!(on_account.account(), Some(alice.clone()));
        assert_eq!(on_profile.account(), Some(alice.clone()));
        assert_eq!(on_post.account(), None);
    }

    #[test]
    fn an_account_whose_handle_has_a_slur_is_not_searchable() {
        let account = Account {
            handle: Some(Handle::try_from("coon.example".to_owned()).unwrap()),
            ..Account::default()
        };
        assert!(account.searchable_handle().is_none());
    }

    #[test]
    fn a_verified_account_is_searchable_until_its_identity_changes() {
        let handle = Handle::try_from("coons.senate.gov".to_owned()).unwrap();
        let verification = AppviewVerification {
            handle: Some(handle.clone()),
            display_name: None,
            state: VerificationState {
                verifications: Vec::new(),
                verified_status: "valid".to_owned(),
                trusted_verifier_status: "none".to_owned(),
            },
        };
        let mut account = Account {
            handle: Some(handle),
            verification: Some(verification),
            ..Account::default()
        };
        assert!(account.searchable_handle().is_some());

        account.profile.display_name = Some("Someone Else".to_owned());
        assert!(account.current_verification().is_none());
        assert!(account.searchable_handle().is_none());
    }

    #[test]
    fn only_active_accounts_with_handles_are_searchable() {
        let handle = Handle::try_from("alice.test".to_owned()).unwrap();
        let mut account = Account {
            handle: Some(handle),
            ..Account::default()
        };
        assert!(account.searchable_handle().is_some());

        account.active = false;
        assert!(account.searchable_handle().is_none());
    }
}
