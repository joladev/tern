use std::fs::{self, File};
use std::io::{ErrorKind, Write as _};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arc_swap::ArcSwap;
use serde::{Deserialize, Serialize};
use tantivy::directory::MmapDirectory;
use tantivy::tokenizer::{PreTokenizedString, Token};
use tantivy::{
    Index, IndexBuilder, IndexReader, IndexSettings, IndexSortByField, IndexWriter, Order,
    ReloadPolicy, TantivyDocument, Term,
};
use types::Did;

use crate::reader::Snapshot;
use crate::schema::{self, Fields, SCORE_FIELD};
use crate::{Actor, Error, FORMAT, terms};

const WRITER_THREADS: usize = 1;
const WRITER_MEMORY_BYTES: usize = 64_000_000;

pub struct Writer {
    slots: Option<Slots>,
    live: Side,
    next: Option<Side>,
    snapshot: Arc<ArcSwap<Snapshot>>,
    fields: Fields,
    cursor: Option<u64>,
}

impl Writer {
    pub(crate) fn open(path: &Path) -> Result<(Writer, Arc<ArcSwap<Snapshot>>, Fields), Error> {
        let slots = Slots::new(path);
        let live_path = slots.live()?;
        remove_dir(slots.other(&live_path))?;

        let (live, payload) = Side::open(&live_path)?;
        Writer::new(Some(slots), live, payload.cursor)
    }

    pub(crate) fn open_in_memory() -> Result<(Writer, Arc<ArcSwap<Snapshot>>, Fields), Error> {
        let live = Side::from_index(&memory_index()?, FORMAT)?;
        Writer::new(None, live, None)
    }

    fn new(
        slots: Option<Slots>,
        live: Side,
        cursor: Option<u64>,
    ) -> Result<(Writer, Arc<ArcSwap<Snapshot>>, Fields), Error> {
        let snapshot = Arc::new(ArcSwap::from_pointee(Snapshot::load(&live.reader)?));
        let (_, fields) = schema::build();
        let writer = Writer {
            slots,
            live,
            next: None,
            snapshot: Arc::clone(&snapshot),
            fields,
            cursor,
        };
        Ok((writer, snapshot, fields))
    }

    pub fn cursor(&self) -> Option<u64> {
        self.cursor
    }

    pub fn set_cursor(&mut self, cursor: u64) {
        self.cursor = Some(cursor);
    }

    pub fn num_docs(&self) -> u64 {
        self.live.reader.searcher().num_docs()
    }

    pub fn needs_rebuild(&self) -> bool {
        self.live.format < FORMAT
    }

    pub fn start_rebuild(&mut self) -> Result<(), Error> {
        let index = match &self.slots {
            Some(slots) => {
                let path = slots.other(&slots.live()?).to_owned();
                remove_dir(&path)?;
                open_index(&path)?
            }
            None => memory_index()?,
        };
        self.next = Some(Side::from_index(&index, FORMAT)?);
        Ok(())
    }

    pub fn upsert(&self, actor: &Actor, score: u64) -> Result<(), Error> {
        for side in self.sides() {
            side.replace(self.did_term(&actor.did), self.document(actor, score))?;
        }
        Ok(())
    }

    pub fn upsert_rebuilt(&self, actor: &Actor, score: u64) -> Result<(), Error> {
        let next = self.next.as_ref().ok_or(Error::NotRebuilding)?;
        next.replace(self.did_term(&actor.did), self.document(actor, score))
    }

    pub fn remove(&self, did: &Did) {
        for side in self.sides() {
            side.writer.delete_term(self.did_term(did));
        }
    }

    pub fn commit(&mut self) -> Result<(), Error> {
        for side in [Some(&mut self.live), self.next.as_mut()]
            .into_iter()
            .flatten()
        {
            side.commit(self.cursor)?;
        }
        self.live.reader.reload()?;
        self.publish()
    }

    pub fn finish_rebuild(&mut self) -> Result<(), Error> {
        self.commit()?;
        let next = self.next.take().ok_or(Error::NotRebuilding)?;

        drop(std::mem::replace(&mut self.live, next));
        self.live.reader.reload()?;
        if let Some(slots) = &self.slots {
            let old_path = slots.live()?;
            slots.set_live(slots.other(&old_path))?;
            remove_dir(&old_path)?;
        }
        self.publish()
    }

    fn publish(&self) -> Result<(), Error> {
        let snapshot = Snapshot::load(&self.live.reader)?;
        self.snapshot.store(Arc::new(snapshot));
        Ok(())
    }

    fn sides(&self) -> impl Iterator<Item = &Side> {
        std::iter::once(&self.live).chain(self.next.as_ref())
    }

    fn did_term(&self, did: &Did) -> Term {
        Term::from_field_text(self.fields.did, did.as_str())
    }

    fn document(&self, actor: &Actor, score: u64) -> TantivyDocument {
        let fields = self.fields;
        let mut document = TantivyDocument::new();
        document.add_text(fields.did, actor.did.as_str());
        document.add_text(fields.handle, actor.handle.as_str());
        if let Some(name) = &actor.display_name {
            document.add_text(fields.display_name, name);
        }
        if let Some(cid) = &actor.avatar_cid {
            document.add_text(fields.avatar_cid, cid);
        }
        if !actor.labels.is_empty() {
            let labels = serde_json::to_string(&actor.labels).expect("labels always serialize");
            document.add_text(fields.labels, labels);
        }
        document.add_u64(fields.score, score);

        let terms = terms::account_terms(actor.handle.as_str(), actor.display_name.as_deref());
        document.add_pre_tokenized_text(fields.terms, pre_tokenized(terms));
        document
    }

    #[cfg(test)]
    pub(crate) fn use_older_format(&mut self) -> Result<(), Error> {
        self.live.format = FORMAT - 1;
        self.publish()
    }
}

struct Side {
    writer: IndexWriter,
    reader: IndexReader,
    format: u32,
}

impl Side {
    fn open(path: &Path) -> Result<(Side, Payload), Error> {
        let index = open_index(path)?;
        let payload = Payload::read(&index)?;
        let side = Side::from_index(&index, payload.format)?;
        Ok((side, payload))
    }

    fn from_index(index: &Index, format: u32) -> Result<Side, Error> {
        let writer = index.writer_with_num_threads(WRITER_THREADS, WRITER_MEMORY_BYTES)?;
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;
        Ok(Side {
            writer,
            reader,
            format,
        })
    }

    fn replace(&self, did: Term, document: TantivyDocument) -> Result<(), Error> {
        self.writer.delete_term(did);
        self.writer.add_document(document)?;
        Ok(())
    }

    fn commit(&mut self, cursor: Option<u64>) -> Result<(), Error> {
        let mut commit = self.writer.prepare_commit()?;
        let payload = Payload {
            cursor,
            format: self.format,
        };
        commit.set_payload(&payload.encode());
        commit.commit()?;
        Ok(())
    }
}

fn open_index(path: &Path) -> Result<Index, Error> {
    fs::create_dir_all(path).map_err(Error::CreateDirectory)?;
    Ok(index_builder().open_or_create(MmapDirectory::open(path)?)?)
}

fn memory_index() -> Result<Index, Error> {
    Ok(index_builder().create_in_ram()?)
}

fn index_builder() -> IndexBuilder {
    let (schema, _) = schema::build();
    let settings = IndexSettings {
        sort_by_field: Some(IndexSortByField {
            field: SCORE_FIELD.to_owned(),
            order: Order::Desc,
        }),
        ..IndexSettings::default()
    };
    Index::builder().schema(schema).settings(settings)
}

#[derive(Deserialize, Serialize)]
struct Payload {
    cursor: Option<u64>,
    format: u32,
}

impl Payload {
    fn encode(&self) -> String {
        serde_json::to_string(self).expect("the payload always serializes")
    }

    fn read(index: &Index) -> Result<Payload, Error> {
        let Some(text) = index.load_metas()?.payload else {
            return Ok(Payload {
                cursor: None,
                format: 0,
            });
        };
        serde_json::from_str(&text).map_err(|_| Error::BadPayload(text))
    }
}

struct Slots {
    primary: PathBuf,
    secondary: PathBuf,
    pointer: PathBuf,
}

impl Slots {
    fn new(path: &Path) -> Slots {
        let name = path.file_name().unwrap_or_default().to_string_lossy();
        Slots {
            primary: path.to_owned(),
            secondary: path.with_file_name(format!("{name}-b")),
            pointer: path.with_file_name(format!("{name}.live")),
        }
    }

    fn live(&self) -> Result<PathBuf, Error> {
        match fs::read_to_string(&self.pointer) {
            Ok(name) => Ok(self.primary.with_file_name(name.trim())),
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(self.primary.clone()),
            Err(err) => Err(Error::Slots(err)),
        }
    }

    fn other(&self, live: &Path) -> &Path {
        if live == self.primary {
            &self.secondary
        } else {
            &self.primary
        }
    }

    fn set_live(&self, live: &Path) -> Result<(), Error> {
        let name = live.file_name().unwrap_or_default().to_string_lossy();
        let temporary = self.pointer.with_extension("tmp");
        let mut file = File::create(&temporary).map_err(Error::Slots)?;
        file.write_all(name.as_bytes()).map_err(Error::Slots)?;
        file.sync_all().map_err(Error::Slots)?;
        fs::rename(&temporary, &self.pointer).map_err(Error::Slots)?;
        let directory = self.pointer.parent().unwrap_or(Path::new("."));
        File::open(directory)
            .and_then(|directory| directory.sync_all())
            .map_err(Error::Slots)
    }
}

fn remove_dir(path: &Path) -> Result<(), Error> {
    match fs::remove_dir_all(path) {
        Err(err) if err.kind() != ErrorKind::NotFound => Err(Error::Slots(err)),
        _ => Ok(()),
    }
}

fn pre_tokenized(terms: Vec<String>) -> PreTokenizedString {
    let tokens = terms
        .into_iter()
        .map(|text| Token {
            offset_from: 0,
            offset_to: text.len(),
            position: 0,
            text,
            position_length: 1,
        })
        .collect();
    PreTokenizedString {
        text: String::new(),
        tokens,
    }
}
