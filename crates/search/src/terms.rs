use tantivy::tokenizer::{AsciiFoldingFilter, LowerCaser, RawTokenizer, TextAnalyzer};

const MAX_TERM_BYTES: usize = 32;

const MAX_QUERY_WORDS: usize = 8;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Kind {
    Exact,
    Label,
    Handle,
    Domain,
    Word,
}

impl Kind {
    pub fn term(self, text: &str) -> String {
        match self {
            Kind::Exact => format!("={text}"),
            Kind::Label => format!("l:{text}"),
            Kind::Handle => format!("h:{}", truncate(text)),
            Kind::Domain => format!("d:{}", truncate(text)),
            Kind::Word => format!("w:{}", truncate(text)),
        }
    }
}

pub fn account_terms(handle: &str, display_name: Option<&str>) -> Vec<String> {
    let first_label = first_label(handle);
    let mut terms = vec![Kind::Exact.term(handle), Kind::Label.term(first_label)];
    push_prefixes(&mut terms, Kind::Handle, handle);

    for part in first_label.split('-').skip(1) {
        push_prefixes(&mut terms, Kind::Word, part);
    }
    for (dot, _) in handle.match_indices('.') {
        push_prefixes(&mut terms, Kind::Domain, &handle[dot + 1..]);
    }
    for word in name_words(display_name) {
        push_prefixes(&mut terms, Kind::Word, &word);
        let cjk_starts = word.char_indices().skip(1).filter(|(_, c)| is_cjk(*c));
        for (start, _) in cjk_starts {
            push_prefixes(&mut terms, Kind::Word, &word[start..]);
        }
    }

    terms.sort_unstable();
    terms.dedup();
    terms
}

pub fn query_words(query: &str) -> Vec<String> {
    normalize(query)
        .split(|c: char| !(c.is_alphanumeric() || c == '.' || c == '-'))
        .filter(|word| !word.is_empty())
        .take(MAX_QUERY_WORDS)
        .map(str::to_owned)
        .collect()
}

pub(crate) fn first_label(handle: &str) -> &str {
    handle.split('.').next().unwrap_or(handle)
}

pub(crate) fn name_words(display_name: Option<&str>) -> Vec<String> {
    fold(display_name.unwrap_or_default())
        .split(|c: char| !c.is_alphanumeric())
        .filter(|word| !word.is_empty())
        .map(str::to_owned)
        .collect()
}

fn is_cjk(c: char) -> bool {
    matches!(
        c,
        '\u{3040}'..='\u{30FF}'
            | '\u{3400}'..='\u{4DBF}'
            | '\u{4E00}'..='\u{9FFF}'
            | '\u{AC00}'..='\u{D7AF}'
            | '\u{F900}'..='\u{FAFF}'
    )
}

fn push_prefixes(terms: &mut Vec<String>, kind: Kind, word: &str) {
    let word = truncate(word);
    for (end, _) in word.char_indices().skip(1) {
        terms.push(kind.term(&word[..end]));
    }
    if !word.is_empty() {
        terms.push(kind.term(word));
    }
}

pub(crate) fn truncate(text: &str) -> &str {
    let end = (0..=MAX_TERM_BYTES.min(text.len()))
        .rev()
        .find(|end| text.is_char_boundary(*end))
        .unwrap_or(0);
    &text[..end]
}

fn normalize(query: &str) -> String {
    fold(query.trim_start().trim_start_matches('@'))
}

fn fold(text: &str) -> String {
    let mut analyzer = TextAnalyzer::builder(RawTokenizer::default())
        .filter(LowerCaser)
        .filter(AsciiFoldingFilter)
        .build();
    let mut folded = String::new();
    analyzer.token_stream(text).process(&mut |token| {
        folded.push_str(&token.text);
    });
    folded
}

#[cfg(test)]
mod tests {
    use super::*;

    fn has(terms: &[String], term: &str) -> bool {
        terms.iter().any(|t| t == term)
    }

    #[test]
    fn indexes_the_handle_as_exact_label_and_prefixes() {
        let terms = account_terms("ab.cd", None);
        for term in ["=ab.cd", "l:ab", "h:a", "h:ab.", "h:ab.cd"] {
            assert!(has(&terms, term), "missing {term}");
        }
    }

    #[test]
    fn indexes_the_handle_after_each_dot_as_domains() {
        let terms = account_terms("safety.bsky.app", None);
        for term in ["d:bsky.app", "d:bsky.a", "d:app"] {
            assert!(has(&terms, term), "missing {term}");
        }
        assert!(!has(&terms, "d:safety"));
        assert!(!has(&terms, "w:bsky"));
    }

    #[test]
    fn indexes_hyphenated_parts_and_name_words() {
        let terms = account_terms("x-yz.test", Some("Émile Zola"));
        for term in ["w:yz", "w:emile", "w:zo", "w:zola"] {
            assert!(has(&terms, term), "missing {term}");
        }
    }

    #[test]
    fn keeps_non_latin_names_searchable() {
        let terms = account_terms("x.test", Some("東京 太郎"));
        for term in ["w:東", "w:東京", "w:太郎"] {
            assert!(has(&terms, term), "missing {term}");
        }
    }

    #[test]
    fn cjk_names_are_searchable_from_any_character() {
        let terms = account_terms("x.test", Some("東京創元社 ソウル서울부부"));
        for term in ["w:創元社", "w:元社", "w:社", "w:서울부부", "w:부부"] {
            assert!(has(&terms, term), "missing {term}");
        }
    }

    #[test]
    fn prefixes_are_cut_at_the_term_limit_but_exact_terms_are_not() {
        let handle = format!("{}.test", "a".repeat(40));
        let terms = account_terms(&handle, None);
        let longest_prefix = terms
            .iter()
            .filter_map(|term| term.strip_prefix("h:"))
            .map(str::len)
            .max();
        assert_eq!(longest_prefix, Some(MAX_TERM_BYTES));
        assert!(has(&terms, &Kind::Exact.term(&handle)));
        assert_eq!(Kind::Handle.term(&handle), Kind::Handle.term(&handle[..32]));
    }

    #[test]
    fn long_queries_are_cut_at_the_word_limit() {
        assert_eq!(query_words(&"a ".repeat(100)).len(), MAX_QUERY_WORDS);
    }

    #[test]
    fn queries_are_normalized_like_accounts() {
        assert_eq!(query_words("  @Alice  SMITH"), ["alice", "smith"]);
        assert!(query_words("   ").is_empty());
    }

    #[test]
    fn punctuation_splits_query_words_but_handles_stay_whole() {
        assert_eq!(query_words("p(aul)"), ["p", "aul"]);
        assert_eq!(query_words("jola.dev"), ["jola.dev"]);
        assert_eq!(query_words("x-yz, \"foo\""), ["x-yz", "foo"]);
    }
}
