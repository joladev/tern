use integration::{CHANGE_TIMEOUT, Harness};
use reqwest::StatusCode;

#[test]
fn accounts_from_the_stream_become_searchable_quickly() {
    let harness = Harness::connect();
    harness.create_account("did:plc:fresh", "freshness.test", "Fresh Account");

    let elapsed = harness.wait_for_top_result("freshn", "freshness.test");

    assert!(elapsed < CHANGE_TIMEOUT);
    let actor = harness.search("freshness.test").remove(0);
    assert_eq!(actor.did, "did:plc:fresh");
    assert_eq!(actor.display_name.as_deref(), Some("Fresh Account"));
    assert_eq!(
        actor.avatar.as_deref(),
        Some("https://cdn.bsky.app/img/avatar/plain/did:plc:fresh/bafkreimock"),
    );
}

#[test]
fn fixture_accounts_are_searchable() {
    let harness = Harness::connect();
    let fixture = include_str!("../../fixtures/accounts.jsonl");
    let first: serde_json::Value = serde_json::from_str(fixture.lines().next().unwrap()).unwrap();
    let handle = first["handle"].as_str().unwrap();

    harness.wait_for_top_result(handle, handle);
}

#[test]
fn display_names_are_searchable() {
    let harness = Harness::connect();
    harness.create_account("did:plc:named", "xq9.test", "Wilhelmina Quark");

    harness.wait_for_top_result("wilhelmina qua", "xq9.test");
}

#[test]
fn a_handle_change_replaces_the_old_handle() {
    let harness = Harness::connect();
    harness.create_account("did:plc:renamed", "oldname.test", "Renamed");
    harness.wait_for_top_result("oldname", "oldname.test");

    harness.change_handle("did:plc:renamed", "newname.test");

    harness.wait_for_top_result("newname", "newname.test");
    harness.wait_until_gone("oldname");
}

#[test]
fn deactivated_accounts_disappear_and_can_return() {
    let harness = Harness::connect();
    harness.create_account("did:plc:paused", "paused.test", "Paused");
    harness.wait_for_top_result("paused", "paused.test");

    harness.deactivate("did:plc:paused");
    harness.wait_until_gone("paused");

    harness.reactivate("did:plc:paused");
    harness.wait_for_top_result("paused", "paused.test");
}

#[test]
fn deleted_accounts_disappear() {
    let harness = Harness::connect();
    harness.create_account("did:plc:deleted", "goner.test", "Goner");
    harness.wait_for_top_result("goner", "goner.test");

    harness.delete("did:plc:deleted");

    harness.wait_until_gone("goner");
}

#[test]
fn enrichment_changes_the_ranking() {
    let harness = Harness::connect();
    harness.create_account("did:plc:rank1", "rankfirst.test", "Rank");
    harness.create_account("did:plc:rank2", "ranksecond.test", "Rank");
    harness.set_followers("did:plc:rank1", 100);
    harness.set_followers("did:plc:rank2", 1);
    harness.wait_for_top_result("rank", "rankfirst.test");

    harness.set_followers("did:plc:rank2", 100_000);

    harness.wait_for_top_result("rank", "ranksecond.test");
}

#[test]
fn an_exact_handle_beats_a_more_popular_prefix_match() {
    let harness = Harness::connect();
    harness.create_account("did:plc:exact", "exact.test", "Exact");
    harness.create_account("did:plc:loud", "exact.test.loud", "Loud");
    harness.set_followers("did:plc:loud", 100_000);
    harness.wait_for_top_result("exact.test.l", "exact.test.loud");

    harness.wait_for_top_result("exact.test", "exact.test");
}

#[test]
fn state_survives_a_restart_and_the_stream_resumes_from_the_cursor() {
    let harness = Harness::connect();
    harness.create_account("did:plc:durable", "durable.test", "Durable");
    harness.wait_for_top_result("durable", "durable.test");

    harness.restart_service();

    harness.wait_for_top_result("durable", "durable.test");
    let cursors = harness.jetstream_cursors();
    assert!(cursors.last().unwrap().is_some(), "{cursors:?}");
}

#[test]
fn a_failed_lookup_keeps_a_known_handle() {
    let harness = Harness::connect();
    harness.create_account("did:plc:steady", "steadfast.test", "Steady");
    harness.wait_for_top_result("steadfast", "steadfast.test");

    harness.fail_lookup("did:plc:steady");
    harness.create_account("did:plc:later", "laterone.test", "Later");
    harness.wait_for_top_result("laterone", "laterone.test");

    harness.wait_for_top_result("steadfast", "steadfast.test");
}

#[test]
fn invalid_limits_get_xrpc_errors() {
    let harness = Harness::connect();
    for limit in ["0", "101", "abc"] {
        let (status, body) = harness.search_with_limit(limit);
        assert_eq!(status, StatusCode::BAD_REQUEST, "limit={limit}");
        assert_eq!(body["error"], "InvalidRequest", "limit={limit}");
    }
}

#[test]
fn metrics_are_exposed() {
    let harness = Harness::connect();
    harness.create_account("did:plc:counted", "counted.test", "Counted");
    harness.wait_for_top_result("counted", "counted.test");
    harness.search("counted");

    let metrics = harness.metrics();

    for name in [
        "tern_events_total",
        "tern_jetstream_lag_seconds",
        "tern_handle_lookup_seconds_bucket",
        "tern_commit_seconds_bucket",
        "tern_indexed_accounts",
        "tern_search_seconds_bucket",
        "process_resident_memory_bytes",
    ] {
        assert!(metrics.contains(name), "missing {name}");
    }
}

#[test]
fn backfill_finds_accounts_that_sent_no_events() {
    let harness = Harness::connect();

    harness.wait_for_top_result("quiet archiv", "quietarchivist.test");
    harness.wait_for_top_result("dormantlurker", "dormantlurker.test");
    let actor = harness.search("quietarchivist.test").remove(0);
    assert_eq!(actor.display_name.as_deref(), Some("Quiet Archivist"));
    assert!(actor.avatar.unwrap().ends_with("/bafkreibackfill"));
}

#[test]
fn backfill_finishes() {
    let harness = Harness::connect();
    harness.wait_for_metric("tern_backfill_finished", 1.0);

    let metrics = harness.metrics();
    for name in ["tern_backfill_listed", "tern_backfill_filled"] {
        assert!(metrics.contains(name), "missing {name}");
    }
}

#[test]
fn metrics_are_not_on_the_public_port() {
    let harness = Harness::connect();
    assert_eq!(harness.public_metrics_status(), StatusCode::NOT_FOUND);
}

#[test]
fn bluesky_s_name_for_search_answers_too() {
    let harness = Harness::connect();
    harness.create_account("did:plc:aliased", "aliased.test", "Aliased");
    harness.wait_for_top_result("aliased.test", "aliased.test");

    let actor = harness.search_as_bluesky("aliased.test").remove(0);
    assert_eq!(actor.handle, "aliased.test");
}

#[test]
fn stats_count_indexed_accounts() {
    let harness = Harness::connect();
    harness.create_account("did:plc:statscounted", "statscounted.test", "Counted");
    harness.wait_for_top_result("statscounted.test", "statscounted.test");

    let accounts = harness.stats()["index"]["accounts"].as_u64().unwrap();
    assert!(accounts > 0);
}

#[test]
fn any_web_origin_can_call_the_api() {
    let harness = Harness::connect();
    assert_eq!(harness.allowed_origin().as_deref(), Some("*"));
}

#[test]
fn browsers_may_send_atproto_headers() {
    let harness = Harness::connect();
    let allowed = harness.preflight_allowed_headers("atproto-accept-labelers");
    assert_eq!(allowed.as_deref(), Some("atproto-accept-labelers"));
}
