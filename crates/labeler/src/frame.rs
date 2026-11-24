use serde::Deserialize;
use types::{Datetime, Did, Label, LabelRecord};

const MESSAGE: i64 = 1;
const ERROR: i64 = -1;
const LABELS_TYPE: &str = "#labels";

#[derive(Debug, PartialEq)]
pub enum Frame {
    Labels(Labels),
    Ignored,
    Failed(String),
}

#[derive(Debug, PartialEq)]
pub struct Labels {
    pub seq: u64,
    pub records: Vec<LabelRecord>,
}

#[derive(Debug, thiserror::Error)]
#[error("failed to decode labeler frame")]
pub struct ParseError(#[source] ciborium::de::Error<std::io::Error>);

#[derive(Deserialize)]
struct Header {
    op: i64,
    t: Option<String>,
}

#[derive(Deserialize)]
struct LabelsBody {
    seq: u64,
    labels: Vec<RawLabel>,
}

#[derive(Deserialize)]
struct ErrorBody {
    error: String,
}

#[derive(Deserialize)]
struct RawLabel {
    src: String,
    uri: String,
    cid: Option<String>,
    val: String,
    #[serde(default)]
    neg: bool,
    cts: String,
    exp: Option<String>,
}

impl RawLabel {
    fn into_record(self) -> Option<LabelRecord> {
        let label = Label {
            src: Did::try_from(self.src).ok()?,
            uri: self.uri,
            cid: self.cid,
            val: self.val,
            cts: Datetime::try_from(self.cts).ok()?,
            exp: self.exp.map(Datetime::try_from).transpose().ok()?,
        };
        Some(LabelRecord {
            label,
            withdrawn: self.neg,
        })
    }
}

impl Frame {
    pub fn parse(mut bytes: &[u8]) -> Result<Frame, ParseError> {
        let header: Header = ciborium::from_reader(&mut bytes).map_err(ParseError)?;
        match (header.op, header.t.as_deref()) {
            (MESSAGE, Some(LABELS_TYPE)) => {
                let body: LabelsBody = ciborium::from_reader(&mut bytes).map_err(ParseError)?;
                let records = body.labels.into_iter();
                Ok(Frame::Labels(Labels {
                    seq: body.seq,
                    records: records.filter_map(RawLabel::into_record).collect(),
                }))
            }
            (ERROR, _) => {
                let body: ErrorBody = ciborium::from_reader(&mut bytes).map_err(ParseError)?;
                Ok(Frame::Failed(body.error))
            }
            _ => Ok(Frame::Ignored),
        }
    }
}

#[cfg(test)]
mod tests {
    use ciborium::cbor;

    use super::*;

    fn frame(header: ciborium::Value, body: ciborium::Value) -> Vec<u8> {
        let mut bytes = Vec::new();
        ciborium::into_writer(&header, &mut bytes).unwrap();
        ciborium::into_writer(&body, &mut bytes).unwrap();
        bytes
    }

    #[test]
    fn parses_a_label_and_its_withdrawal() {
        let header = cbor!({ "op" => 1, "t" => "#labels" }).unwrap();
        let body = cbor!({ "seq" => 7, "labels" => [
            { "ver" => 1, "src" => "did:plc:labeler", "uri" => "did:plc:alice",
              "val" => "spam", "cts" => "2026-01-01T00:00:00.000Z" },
            { "ver" => 1, "src" => "did:plc:labeler", "uri" => "did:plc:alice",
              "val" => "spam", "neg" => true, "cts" => "2026-02-01T00:00:00.000Z" },
        ]})
        .unwrap();

        let parsed = Frame::parse(&frame(header, body)).unwrap();

        let Frame::Labels(labels) = parsed else {
            panic!("unexpected frame {parsed:?}");
        };
        assert_eq!(labels.seq, 7);
        let withdrawn: Vec<_> = labels.records.iter().map(|r| r.withdrawn).collect();
        assert_eq!(withdrawn, [false, true]);
        assert_eq!(labels.records[0].label.val, "spam");
        assert_eq!(labels.records[0].label.uri, "did:plc:alice");
    }

    #[test]
    fn error_frames_are_reported() {
        let header = cbor!({ "op" => -1 }).unwrap();
        let body = cbor!({ "error" => "FutureCursor", "message" => "m" }).unwrap();

        let parsed = Frame::parse(&frame(header, body)).unwrap();

        assert_eq!(parsed, Frame::Failed("FutureCursor".to_owned()));
    }

    #[test]
    fn other_kinds_are_ignored() {
        let header = cbor!({ "op" => 1, "t" => "#info" }).unwrap();
        let body = cbor!({ "name" => "OutdatedCursor" }).unwrap();

        assert_eq!(Frame::parse(&frame(header, body)).unwrap(), Frame::Ignored);
    }

    #[test]
    fn bytes_that_are_not_cbor_are_an_error() {
        assert!(Frame::parse(&[0xff, 0x00]).is_err());
    }
}
