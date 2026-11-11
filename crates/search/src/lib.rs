mod intersection;
mod reader;
mod schema;
mod terms;
mod writer;

use std::path::Path;

use types::{Did, Handle, Label};

pub use reader::Reader;
pub use writer::Writer;

const FORMAT: u32 = 6;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("search index operation failed")]
    Index(#[from] tantivy::TantivyError),
    #[error("failed to create the index directory")]
    CreateDirectory(#[source] std::io::Error),
    #[error("failed to open the index directory")]
    OpenDirectory(#[from] tantivy::directory::error::OpenDirectoryError),
    #[error("failed to read or switch the live index directory")]
    Slots(#[source] std::io::Error),
    #[error("found a stored document with a missing or invalid `{0}`")]
    BadDocument(&'static str),
    #[error("found an invalid commit payload: `{0}`")]
    BadPayload(String),
    #[error("no rebuild is running")]
    NotRebuilding,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Actor {
    pub did: Did,
    pub handle: Handle,
    pub display_name: Option<String>,
    pub avatar_cid: Option<String>,
    pub labels: Vec<Label>,
}

pub fn open(path: &Path) -> Result<(Reader, Writer), Error> {
    let (writer, snapshot, fields) = Writer::open(path)?;
    Ok((Reader::new(snapshot, fields), writer))
}

pub fn open_in_memory() -> Result<(Reader, Writer), Error> {
    let (writer, snapshot, fields) = Writer::open_in_memory()?;
    Ok((Reader::new(snapshot, fields), writer))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn actor(name: &str, display_name: Option<&str>) -> Actor {
        Actor {
            did: Did::try_from(format!("did:plc:{name}")).unwrap(),
            handle: Handle::try_from(format!("{name}.bsky.social")).unwrap(),
            display_name: display_name.map(str::to_owned),
            avatar_cid: None,
            labels: Vec::new(),
        }
    }

    fn actor_with_handle(name: &str, handle: &str, display_name: Option<&str>) -> Actor {
        Actor {
            handle: Handle::try_from(handle.to_owned()).unwrap(),
            ..actor(name, display_name)
        }
    }

    fn index_path(dir: &tempfile::TempDir) -> std::path::PathBuf {
        dir.path().join("index")
    }

    fn open_index() -> (Reader, Writer) {
        open_in_memory().unwrap()
    }

    fn handles(reader: &Reader, query: &str, limit: usize) -> Vec<String> {
        let actors = reader.search(query, limit).unwrap();
        actors
            .iter()
            .map(|actor| actor.handle.to_string())
            .collect()
    }

    #[test]
    fn results_are_ordered_by_score_across_segments() {
        let (reader, mut writer) = open_index();
        writer.upsert(&actor("alice", None), 10).unwrap();
        writer.upsert(&actor("alicia", None), 30).unwrap();
        writer.commit().unwrap();
        writer.upsert(&actor("alina", None), 20).unwrap();
        writer.commit().unwrap();

        assert_eq!(
            handles(&reader, "al", 10),
            [
                "alicia.bsky.social",
                "alina.bsky.social",
                "alice.bsky.social"
            ]
        );
    }

    #[test]
    fn an_exact_handle_comes_first_whatever_its_score() {
        let (reader, mut writer) = open_index();
        writer.upsert(&actor("alice", None), 1).unwrap();
        let mut bigger = actor("big", None);
        bigger.handle = Handle::try_from("alice.bsky.social.mirror".to_owned()).unwrap();
        writer.upsert(&bigger, 100).unwrap();
        writer.commit().unwrap();

        assert_eq!(
            handles(&reader, "@alice.bsky.social", 10),
            ["alice.bsky.social", "alice.bsky.social.mirror"]
        );
    }

    #[test]
    fn matches_display_names_regardless_of_case_or_accents() {
        let (reader, mut writer) = open_index();
        writer.upsert(&actor("xyz", Some("Zoë Müller")), 1).unwrap();
        writer.commit().unwrap();

        assert_eq!(handles(&reader, "ZOE", 10), ["xyz.bsky.social"]);
        assert_eq!(handles(&reader, "mull", 10), ["xyz.bsky.social"]);
    }

    #[test]
    fn accounts_matching_every_word_come_before_those_matching_some() {
        let (reader, mut writer) = open_index();
        writer
            .upsert(&actor("one", Some("Ada Lovelace")), 1)
            .unwrap();
        writer.upsert(&actor("two", Some("Ada Palmer")), 2).unwrap();
        writer.commit().unwrap();

        assert_eq!(
            handles(&reader, "ada lov", 10),
            ["one.bsky.social", "two.bsky.social"]
        );
    }

    #[test]
    fn a_full_name_finds_an_account_named_with_only_part_of_it() {
        let (reader, mut writer) = open_index();
        let jay = actor_with_handle("jay", "jay.bsky.team", Some("Jay"));
        writer.upsert(&jay, 1).unwrap();
        writer.commit().unwrap();

        assert_eq!(handles(&reader, "jay graber", 10), ["jay.bsky.team"]);
    }

    #[test]
    fn punctuation_in_a_query_matches_a_name_split_at_it() {
        let (reader, mut writer) = open_index();
        let paul = actor_with_handle("p", "pfrazee.com", Some("P(aul) Frazee"));
        writer.upsert(&paul, 1).unwrap();
        writer.commit().unwrap();

        assert_eq!(handles(&reader, "p(aul)", 10), ["pfrazee.com"]);
    }

    #[test]
    fn a_word_inside_a_cjk_name_finds_the_account() {
        let (reader, mut writer) = open_index();
        let tokyo = actor_with_handle("t", "tokyo.test", Some("東京創元社"));
        writer.upsert(&tokyo, 1).unwrap();
        writer.commit().unwrap();

        assert_eq!(handles(&reader, "創元社", 10), ["tokyo.test"]);
    }

    #[test]
    fn a_handle_whose_first_label_is_the_query_beats_bigger_prefixes() {
        let (reader, mut writer) = open_index();
        writer
            .upsert(&actor_with_handle("j", "jola.dev", None), 1000)
            .unwrap();
        writer.upsert(&actor("jolantru", None), 1400).unwrap();
        writer.commit().unwrap();

        assert_eq!(
            handles(&reader, "jola", 10),
            ["jola.dev", "jolantru.bsky.social"]
        );
    }

    #[test]
    fn the_first_label_bonus_does_not_beat_a_much_bigger_account() {
        let (reader, mut writer) = open_index();
        writer.upsert(&actor("pfr", None), 100).unwrap();
        writer
            .upsert(&actor_with_handle("p", "pfrazee.com", None), 2000)
            .unwrap();
        writer.commit().unwrap();

        assert_eq!(handles(&reader, "pfr", 1), ["pfrazee.com"]);
    }

    #[test]
    fn a_handle_match_beats_a_slightly_bigger_name_match() {
        let (reader, mut writer) = open_index();
        let rain = actor_with_handle("rain", "sunshowers.io", Some("rain"));
        writer.upsert(&rain, 1000).unwrap();
        writer.upsert(&actor("x1", Some("Sunshine")), 1100).unwrap();
        writer.commit().unwrap();

        assert_eq!(
            handles(&reader, "sunsh", 10),
            ["sunshowers.io", "x1.bsky.social"]
        );
    }

    #[test]
    fn the_handle_after_a_dot_matches() {
        let (reader, mut writer) = open_index();
        let safety = actor_with_handle("safety", "safety.bsky.app", None);
        writer.upsert(&safety, 1).unwrap();
        writer.commit().unwrap();

        assert_eq!(handles(&reader, "bsky.app", 10), ["safety.bsky.app"]);
    }

    #[test]
    fn a_domain_only_matches_a_query_with_a_dot() {
        let (reader, mut writer) = open_index();
        writer.upsert(&actor("aoc", None), 2000).unwrap();
        writer
            .upsert(&actor_with_handle("s", "socrates.example", None), 100)
            .unwrap();
        writer.commit().unwrap();

        assert_eq!(handles(&reader, "soc", 10), ["socrates.example"]);
        assert_eq!(handles(&reader, "bsky.s", 10), ["aoc.bsky.social"]);
    }

    #[test]
    fn a_name_with_a_hyphen_is_found_as_typed() {
        let (reader, mut writer) = open_index();
        let aoc = actor_with_handle("aoc", "aoc.bsky.social", Some("Alexandria Ocasio-Cortez"));
        writer.upsert(&aoc, 1).unwrap();
        writer.commit().unwrap();

        for query in ["ocasio-", "ocasio-c", "ocasio-cortez"] {
            assert_eq!(handles(&reader, query, 10), ["aoc.bsky.social"], "{query}");
        }
    }

    #[test]
    fn a_query_with_a_dot_matches_handles_not_names() {
        let (reader, mut writer) = open_index();
        writer
            .upsert(&actor_with_handle("n", "nel.pet", Some("nel")), 1)
            .unwrap();
        writer
            .upsert(&actor("x", Some("Nel Parker")), 1000)
            .unwrap();
        writer.commit().unwrap();

        for query in ["nel.", "nel.p"] {
            assert_eq!(handles(&reader, query, 10), ["nel.pet"], "{query}");
        }
    }

    #[test]
    fn upserting_replaces_the_old_document() {
        let (reader, mut writer) = open_index();
        let mut account = actor("alice", None);
        writer.upsert(&account, 1).unwrap();
        writer.commit().unwrap();

        account.handle = Handle::try_from("bob.test".to_owned()).unwrap();
        writer.upsert(&account, 1).unwrap();
        writer.commit().unwrap();

        assert!(handles(&reader, "alice", 10).is_empty());
        assert_eq!(handles(&reader, "bob", 10), ["bob.test"]);
    }

    #[test]
    fn removed_accounts_disappear_from_results() {
        let (reader, mut writer) = open_index();
        let account = actor("alice", None);
        writer.upsert(&account, 1).unwrap();
        writer.commit().unwrap();

        writer.remove(&account.did);
        writer.commit().unwrap();

        assert!(handles(&reader, "alice", 10).is_empty());
    }

    #[test]
    fn limit_caps_the_results() {
        let (reader, mut writer) = open_index();
        for (score, name) in (0u64..).zip(["aa", "ab", "ac"]) {
            writer.upsert(&actor(name, None), score).unwrap();
        }
        writer.commit().unwrap();

        assert_eq!(handles(&reader, "a", 2).len(), 2);
    }

    #[test]
    fn the_cursor_survives_a_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let (_reader, mut writer) = open(&index_path(&dir)).unwrap();
        assert_eq!(writer.cursor(), None);

        writer.set_cursor(42);
        writer.commit().unwrap();
        drop(writer);

        let (_reader, writer) = open(&index_path(&dir)).unwrap();
        assert_eq!(writer.cursor(), Some(42));
    }

    fn open_older_index(dir: &tempfile::TempDir) -> (Reader, Writer) {
        let (_reader, mut writer) = open(&index_path(dir)).unwrap();
        writer.use_older_format().unwrap();
        writer
            .upsert(&actor("alice", Some("Alice Liddell")), 1)
            .unwrap();
        writer.set_cursor(7);
        writer.commit().unwrap();
        drop(writer);
        open(&index_path(dir)).unwrap()
    }

    #[test]
    fn a_new_index_needs_no_rebuild() {
        let (_reader, writer) = open_index();
        assert!(!writer.needs_rebuild());
    }

    #[test]
    fn an_index_never_committed_is_rebuilt() {
        let dir = tempfile::tempdir().unwrap();
        let (_reader, writer) = open(&index_path(&dir)).unwrap();
        assert!(writer.needs_rebuild());
    }

    #[test]
    fn an_older_index_keeps_serving_searches_through_a_rebuild() {
        let dir = tempfile::tempdir().unwrap();
        let (reader, mut writer) = open_older_index(&dir);
        assert!(writer.needs_rebuild());
        assert_eq!(handles(&reader, "liddell", 10), ["alice.bsky.social"]);

        writer.start_rebuild().unwrap();
        writer
            .upsert_rebuilt(&actor("alice", Some("Alice Liddell")), 1)
            .unwrap();
        writer.upsert(&actor("bob", None), 2).unwrap();
        writer.commit().unwrap();
        assert_eq!(handles(&reader, "bob", 10), ["bob.bsky.social"]);

        writer.finish_rebuild().unwrap();
        assert!(!writer.needs_rebuild());
        assert_eq!(handles(&reader, "liddell", 10), ["alice.bsky.social"]);
        assert_eq!(handles(&reader, "bob", 10), ["bob.bsky.social"]);
        drop(writer);

        let (reader, writer) = open(&index_path(&dir)).unwrap();
        assert!(!writer.needs_rebuild());
        assert_eq!(writer.cursor(), Some(7));
        assert_eq!(handles(&reader, "bob", 10), ["bob.bsky.social"]);
    }

    #[test]
    fn an_unfinished_rebuild_is_discarded_on_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let (_reader, mut writer) = open_older_index(&dir);
        writer.start_rebuild().unwrap();
        writer
            .upsert_rebuilt(&actor("alice", Some("Alice Liddell")), 1)
            .unwrap();
        writer.commit().unwrap();
        drop(writer);

        let (reader, writer) = open(&index_path(&dir)).unwrap();
        assert!(writer.needs_rebuild());
        assert!(!dir.path().join("index-b").exists());
        assert_eq!(handles(&reader, "liddell", 10), ["alice.bsky.social"]);
    }
}
