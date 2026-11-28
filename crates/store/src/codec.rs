use std::any::type_name;

use serde::Serialize;
use serde::de::DeserializeOwned;
use types::{Account, Did, Job, LabelJob, LabelRecord};

use crate::{BackfillProgress, EnrichmentRow, Error};

pub(crate) trait Stored: Serialize + DeserializeOwned {
    // To add a field: put it last and make it an Option, a bool, a string, a list or a number.
    // Then add an entry to this list saying how many fields you added. Old rows don't have the
    // new field, so it's read as None, false, empty or 0.
    const FIELDS_ADDED: &'static [usize] = &[];
    const VERSION: u8 = Self::FIELDS_ADDED.len() as u8 + 1;
}

impl Stored for Account {}
impl Stored for EnrichmentRow {}
impl Stored for Vec<LabelRecord> {}
impl Stored for Job {}
impl Stored for LabelJob {}
impl Stored for BackfillProgress {}
impl Stored for Option<Did> {}
impl Stored for u64 {}

pub(crate) fn encode<T: Stored>(value: &T) -> Result<Vec<u8>, Error> {
    Ok(postcard::to_extend(value, vec![T::VERSION])?)
}

pub(crate) fn decode<T: Stored>(bytes: &[u8]) -> Result<T, Error> {
    let (&version, row) = bytes
        .split_first()
        .ok_or(postcard::Error::DeserializeUnexpectedEnd)?;
    let added = usize::from(version)
        .checked_sub(1)
        .and_then(|versions| T::FIELDS_ADDED.get(versions..))
        .ok_or(Error::UnsupportedVersion {
            kind: type_name::<T>(),
            version,
        })?;
    let missing: usize = added.iter().sum();
    if missing == 0 {
        return Ok(postcard::from_bytes(row)?);
    }
    let padded = [row, &vec![0; missing]].concat();
    Ok(postcard::from_bytes(&padded)?)
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::*;

    #[derive(Debug, Deserialize, PartialEq, Serialize)]
    struct Before {
        name: String,
    }

    #[derive(Debug, Deserialize, PartialEq, Serialize)]
    struct After {
        name: String,
        nickname: Option<String>,
        hidden: bool,
    }

    impl Stored for Before {}

    impl Stored for After {
        const FIELDS_ADDED: &'static [usize] = &[2];
    }

    #[test]
    fn a_row_from_an_older_version_gets_its_new_fields_empty() {
        let name = "alice".to_owned();
        let row = encode(&Before { name: name.clone() }).unwrap();

        let after = After {
            name,
            nickname: None,
            hidden: false,
        };
        assert_eq!(decode::<After>(&row).unwrap(), after);
    }

    #[test]
    fn a_row_from_a_newer_version_is_refused() {
        let after = After {
            name: "alice".to_owned(),
            nickname: None,
            hidden: false,
        };
        let row = encode(&after).unwrap();

        let refused = decode::<Before>(&row).unwrap_err();
        assert!(matches!(
            refused,
            Error::UnsupportedVersion { version: 2, .. }
        ));
    }
}
