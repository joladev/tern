mod slurs;

use std::collections::HashMap;
use std::fmt;

use serde::{Deserialize, Serialize};

const INVALID_HANDLE: &str = "handle.invalid";
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

#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("invalid DID `{0}`")]
    Did(String),
    #[error("invalid handle `{0}`")]
    Handle(String),
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

/// The AppView judges a verification against the account's handle and display
/// name, so its verdict only holds while both are unchanged.
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
    pub created_at: String,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct Profile {
    pub display_name: Option<String>,
    pub avatar_cid: Option<String>,
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

impl From<ProfileRecord> for Profile {
    fn from(record: ProfileRecord) -> Profile {
        Profile {
            display_name: display_name(record.display_name),
            avatar_cid: record.avatar.map(Blob::into_cid),
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Label {
    pub src: Did,
    pub uri: String,
    pub cid: Option<String>,
    pub val: String,
    pub cts: String,
    pub exp: Option<String>,
}

impl Label {
    pub fn has_valid_times(&self) -> bool {
        parse_datetime(&self.cts).is_some()
            && self
                .exp
                .as_deref()
                .is_none_or(|exp| parse_datetime(exp).is_some())
    }
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct Enrichment {
    pub followers: u64,
    pub created_at: Option<String>,
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
    pub labels: Option<Vec<Label>>,
    pub verification: Option<Option<AppviewVerification>>,
}

impl Facts {
    pub fn or(self, other: Facts) -> Facts {
        Facts {
            handle: self.handle.or(other.handle),
            profile: self.profile.or(other.profile),
            status: self.status.or(other.status),
            enrichment: self.enrichment.or(other.enrichment),
            labels: self.labels.or(other.labels),
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

pub fn parse_datetime(value: &str) -> Option<jiff::Timestamp> {
    value.parse().ok()
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
