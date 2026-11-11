use tantivy::query::{EmptyScorer, EnableScoring, Explanation, Query, Scorer, Weight};
use tantivy::{DocId, DocSet, Score, SegmentReader, TERMINATED, TantivyError};

#[derive(Debug)]
pub(crate) struct BoundedIntersection {
    clauses: Vec<Box<dyn Query>>,
    depth: usize,
}

impl BoundedIntersection {
    pub(crate) fn new(clauses: Vec<Box<dyn Query>>, depth: usize) -> BoundedIntersection {
        BoundedIntersection { clauses, depth }
    }
}

impl Clone for BoundedIntersection {
    fn clone(&self) -> BoundedIntersection {
        let clauses = self
            .clauses
            .iter()
            .map(|clause| clause.box_clone())
            .collect();
        BoundedIntersection::new(clauses, self.depth)
    }
}

impl Query for BoundedIntersection {
    fn weight(&self, scoring: EnableScoring<'_>) -> tantivy::Result<Box<dyn Weight>> {
        let weights = self
            .clauses
            .iter()
            .map(|clause| clause.weight(scoring))
            .collect::<tantivy::Result<_>>()?;
        Ok(Box::new(BoundedWeight {
            weights,
            depth: self.depth,
        }))
    }
}

struct BoundedWeight {
    weights: Vec<Box<dyn Weight>>,
    depth: usize,
}

impl Weight for BoundedWeight {
    fn scorer(&self, reader: &SegmentReader, boost: Score) -> tantivy::Result<Box<dyn Scorer>> {
        let mut scorers = self
            .weights
            .iter()
            .map(|weight| weight.scorer(reader, boost))
            .collect::<tantivy::Result<Vec<_>>>()?;
        match scorers.len() {
            0 => Ok(Box::new(EmptyScorer)),
            1 => Ok(scorers.swap_remove(0)),
            _ => {
                scorers.sort_by_key(|scorer| scorer.cost());
                Ok(Box::new(BoundedScorer::new(scorers, self.depth)))
            }
        }
    }

    fn explain(&self, reader: &SegmentReader, doc: DocId) -> tantivy::Result<Explanation> {
        let mut scorer = self.scorer(reader, 1.0)?;
        if scorer.doc() > doc || scorer.seek(doc) != doc {
            let message = format!("document {doc} does not match");
            return Err(TantivyError::InvalidArgument(message));
        }
        Ok(Explanation::new("BoundedIntersection", scorer.score()))
    }
}

struct BoundedScorer {
    scorers: Vec<Box<dyn Scorer>>,
    budget: usize,
    doc: DocId,
}

impl BoundedScorer {
    fn new(scorers: Vec<Box<dyn Scorer>>, depth: usize) -> BoundedScorer {
        let mut scorer = BoundedScorer {
            scorers,
            budget: depth,
            doc: TERMINATED,
        };
        let first = scorer.scorers[0].doc();
        scorer.doc = scorer.align(first);
        scorer
    }

    fn align(&mut self, mut candidate: DocId) -> DocId {
        let (lead, others) = self
            .scorers
            .split_first_mut()
            .expect("an intersection has at least two clauses");
        'candidates: while candidate != TERMINATED && self.budget > 0 {
            self.budget -= 1;
            for other in others.iter_mut() {
                let doc = if other.doc() < candidate {
                    other.seek(candidate)
                } else {
                    other.doc()
                };
                if doc > candidate {
                    candidate = lead.seek(doc);
                    continue 'candidates;
                }
            }
            return candidate;
        }
        TERMINATED
    }
}

impl DocSet for BoundedScorer {
    fn advance(&mut self) -> DocId {
        let candidate = self.scorers[0].advance();
        self.doc = self.align(candidate);
        self.doc
    }

    fn seek(&mut self, target: DocId) -> DocId {
        let candidate = self.scorers[0].seek(target);
        self.doc = self.align(candidate);
        self.doc
    }

    fn doc(&self) -> DocId {
        self.doc
    }

    fn size_hint(&self) -> u32 {
        self.scorers[0].size_hint()
    }
}

impl Scorer for BoundedScorer {
    fn score(&mut self) -> Score {
        self.scorers.iter_mut().map(|scorer| scorer.score()).sum()
    }
}

#[cfg(test)]
mod tests {
    use tantivy::collector::DocSetCollector;
    use tantivy::query::TermQuery;
    use tantivy::schema::{IndexRecordOption, STRING, Schema};
    use tantivy::{Index, TantivyDocument, Term};

    use super::*;

    #[test]
    fn gives_up_after_the_scan_depth() {
        let mut schema = Schema::builder();
        let field = schema.add_text_field("terms", STRING);
        let index = Index::create_in_ram(schema.build());
        let mut writer = index.writer_with_num_threads(1, 15_000_000).unwrap();
        for terms in [&["a"][..], &["b"], &["a", "b"]] {
            let mut document = TantivyDocument::new();
            for term in terms {
                document.add_text(field, term);
            }
            writer.add_document(document).unwrap();
        }
        writer.commit().unwrap();
        let searcher = index.reader().unwrap().searcher();
        let term = |text: &str| -> Box<dyn Query> {
            let term = Term::from_field_text(field, text);
            Box::new(TermQuery::new(term, IndexRecordOption::Basic))
        };
        let found = |depth| {
            let query = BoundedIntersection::new(vec![term("a"), term("b")], depth);
            searcher.search(&query, &DocSetCollector).unwrap().len()
        };

        assert_eq!(found(1), 0);
        assert_eq!(found(2), 1);
    }
}
