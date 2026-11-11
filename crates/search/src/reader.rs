use std::cmp::Reverse;
use std::sync::Arc;

use crate::intersection::BoundedIntersection;
use arc_swap::ArcSwap;
use tantivy::columnar::Column;
use tantivy::query::{BooleanQuery, EnableScoring, Query, TermQuery};
use tantivy::schema::{Field, IndexRecordOption, Value};
use tantivy::{
    DocAddress, DocId, DocSet, IndexReader, Searcher, TERMINATED, TantivyDocument, Term,
};
use types::{Did, Handle};

use crate::schema::{Fields, SCORE_FIELD};
use crate::terms::{self, Kind};
use crate::{Actor, Error};

const HANDLE_BONUS: u64 = 150;
const LABEL_BONUS: u64 = 500;
const CANDIDATES_PER_RESULT: usize = 200;

pub(crate) struct Snapshot {
    searcher: Searcher,
    scores: Vec<Column<u64>>,
}

#[derive(Clone, Copy)]
struct Match {
    score: u64,
    address: DocAddress,
}

impl Snapshot {
    pub(crate) fn load(reader: &IndexReader) -> Result<Snapshot, Error> {
        let searcher = reader.searcher();
        let scores = searcher
            .segment_readers()
            .iter()
            .map(|segment| segment.fast_fields().u64(SCORE_FIELD))
            .collect::<tantivy::Result<_>>()?;
        Ok(Snapshot { searcher, scores })
    }

    fn top_matches(&self, query: &dyn Query, limit: usize) -> Result<Vec<Match>, Error> {
        let scoring = EnableScoring::disabled_from_searcher(&self.searcher);
        let weight = query.weight(scoring)?;

        let mut matches = Vec::new();
        let segments = self.searcher.segment_readers().iter();
        for (ordinal, segment) in (0u32..).zip(segments) {
            let alive = segment.alive_bitset();
            let mut docs = weight.scorer(segment, 1.0)?;
            let mut found = 0;
            let mut doc = docs.doc();
            while doc != TERMINATED && found < limit {
                if alive.is_none_or(|alive| alive.is_alive(doc)) {
                    let address = DocAddress::new(ordinal, doc);
                    matches.push(Match {
                        score: self.score(ordinal, doc),
                        address,
                    });
                    found += 1;
                }
                doc = docs.advance();
            }
        }

        matches.sort_by_key(|found| Reverse(found.score));
        matches.truncate(limit);
        Ok(matches)
    }

    fn score(&self, segment: u32, doc: DocId) -> u64 {
        self.scores[segment as usize]
            .first(doc)
            .expect("every document is written with a score")
    }
}

#[derive(Clone)]
pub struct Reader {
    snapshot: Arc<ArcSwap<Snapshot>>,
    fields: Fields,
}

impl Reader {
    pub(crate) fn new(snapshot: Arc<ArcSwap<Snapshot>>, fields: Fields) -> Reader {
        Reader { snapshot, fields }
    }

    pub fn num_docs(&self) -> u64 {
        self.snapshot.load().searcher.num_docs()
    }

    pub fn search(&self, query: &str, limit: usize) -> Result<Vec<Actor>, Error> {
        let words = terms::query_words(query);
        if words.is_empty() {
            return Ok(Vec::new());
        }

        let snapshot = self.snapshot.load();
        let matches = if let [word] = words.as_slice() {
            self.word_matches(&snapshot, word, limit)?
        } else {
            self.phrase_matches(&snapshot, &words, limit)?
        };

        distinct(matches, limit)
            .into_iter()
            .map(|address| self.actor(&snapshot.searcher, address))
            .collect()
    }

    fn word_matches(
        &self,
        snapshot: &Snapshot,
        word: &str,
        limit: usize,
    ) -> Result<Vec<Match>, Error> {
        let depth = limit * CANDIDATES_PER_RESULT;
        let label = word.trim_end_matches('.');
        let lookups = [
            (
                self.term_query(&Kind::Label.term(label)),
                LABEL_BONUS + HANDLE_BONUS,
            ),
            (self.term_query(&Kind::Handle.term(word)), HANDLE_BONUS),
            (self.name_or_domain_query(word, depth), 0),
        ];

        let mut ranked = Vec::new();
        for (query, bonus) in lookups {
            let found = snapshot.top_matches(query.as_ref(), limit)?;
            ranked.extend(found.into_iter().map(|found| Match {
                score: found.score + bonus,
                ..found
            }));
        }
        ranked.sort_by_key(|found| Reverse(found.score));

        let exact = self.term_query(&Kind::Exact.term(word));
        let mut matches = snapshot.top_matches(exact.as_ref(), 1)?;
        matches.extend(ranked);
        Ok(matches)
    }

    fn phrase_matches(
        &self,
        snapshot: &Snapshot,
        words: &[String],
        limit: usize,
    ) -> Result<Vec<Match>, Error> {
        let depth = limit * CANDIDATES_PER_RESULT;
        let all = BoundedIntersection::new(self.word_queries(words, depth), depth);
        let mut matches = snapshot.top_matches(&all, limit)?;
        if matches.len() < limit {
            let some = BooleanQuery::union(self.word_queries(words, depth));
            matches.extend(snapshot.top_matches(&some, limit)?);
        }
        Ok(matches)
    }

    fn word_queries(&self, words: &[String], depth: usize) -> Vec<Box<dyn Query>> {
        words
            .iter()
            .map(|word| {
                let queries = vec![
                    self.term_query(&Kind::Handle.term(word)),
                    self.name_or_domain_query(word, depth),
                ];
                Box::new(BooleanQuery::union(queries)) as Box<dyn Query>
            })
            .collect()
    }

    fn term_query(&self, text: &str) -> Box<dyn Query> {
        let term = Term::from_field_text(self.fields.terms, text);
        Box::new(TermQuery::new(term, IndexRecordOption::Basic))
    }

    fn name_or_domain_query(&self, word: &str, depth: usize) -> Box<dyn Query> {
        if is_domain(word) {
            self.term_query(&Kind::Domain.term(word))
        } else {
            self.name_query(word, depth)
        }
    }

    fn name_query(&self, word: &str, depth: usize) -> Box<dyn Query> {
        let parts = terms::name_words(Some(word))
            .iter()
            .map(|part| self.term_query(&Kind::Word.term(part)))
            .collect();
        Box::new(BoundedIntersection::new(parts, depth))
    }

    fn actor(&self, searcher: &Searcher, address: DocAddress) -> Result<Actor, Error> {
        let doc: TantivyDocument = searcher.doc(address)?;
        let text = |field: Field| doc.get_first(field).and_then(|value| value.as_str());

        let did = text(self.fields.did)
            .and_then(|did| Did::try_from(did.to_owned()).ok())
            .ok_or(Error::BadDocument("did"))?;
        let handle = text(self.fields.handle)
            .and_then(|handle| Handle::try_from(handle.to_owned()).ok())
            .ok_or(Error::BadDocument("handle"))?;
        let labels = match text(self.fields.labels) {
            Some(labels) => {
                serde_json::from_str(labels).map_err(|_| Error::BadDocument("labels"))?
            }
            None => Vec::new(),
        };

        Ok(Actor {
            did,
            handle,
            display_name: text(self.fields.display_name).map(str::to_owned),
            avatar_cid: text(self.fields.avatar_cid).map(str::to_owned),
            labels,
        })
    }
}

fn is_domain(word: &str) -> bool {
    word.contains('.')
}

fn distinct(matches: Vec<Match>, limit: usize) -> Vec<DocAddress> {
    let mut addresses = Vec::with_capacity(limit);
    for found in matches {
        if addresses.len() == limit {
            break;
        }
        if !addresses.contains(&found.address) {
            addresses.push(found.address);
        }
    }
    addresses
}
