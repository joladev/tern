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
}

impl Default for Account {
    fn default() -> Account {
        Account {
            handle: None,
            profile: Profile::default(),
            active: true,
        }
    }
}

impl Account {
    pub fn searchable_handle(&self) -> Option<&Handle> {
        self.handle.as_ref().filter(|_| self.active)
    }
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct Profile {
    pub display_name: Option<String>,
    pub avatar_cid: Option<String>,
    pub created_at: Option<i64>,
}

pub fn display_name(name: Option<String>) -> Option<String> {
    let mut name = name.filter(|name| !name.is_empty())?;
    name.truncate(name.floor_char_boundary(MAX_DISPLAY_NAME_BYTES));
    Some(name)
}

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct Enrichment {
    pub followers: u64,
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
