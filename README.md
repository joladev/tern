# tern

tern is a single binary atproto account typeahead service.

[cove.town](https://cove.town) has a live production instance, try it out: `curl https://typeahead.cove.town/xrpc/town.cove.actor.searchActorsTypeahead?q=alice&limit=6`. You can also play around with it live at [https://typeahead.cove.town](https://typeahead.cove.town) and see the latest statistics from the running service. It's hosted in Finland and it has bunny.net sitting in front of it. It already powers [shelf.cafe](https://shelf.cafe) and [annot.at](https://annot.at).

This is not the first atproto typeahead, and I used [waow.tech](https://typeahead.waow.tech/) and of course Bluesky's typeahead as inspiration.

[Read the blog post for more background and information.](https://jola.dev/posts/tern-atproto-typeahead)

## How it works

tern uses an embedded search engine called [tantivy](https://github.com/quickwit-oss/tantivy) to build an index of atproto accounts, with a basic scoring algorithm for ranking. It uses [fjall](https://github.com/fjall-rs/fjall) for the account storage, which allows for more efficient disk storage out of the box than something like sqlite.

Account updates are automatically discovered through the jetstream and a handle change should get reflected within seconds, using slingshot for identity resolution. It also supports an optional one-off backfill to fill the account store, powered by Hubble. Account metadata is refreshed on a weekly schedule.

Ranking uses a mix of account age and Bluesky follower count, with bonuses for prefix matches and exact handles. I've tried to aim for supporting display name search, but my assumption is that most people type their handle, not their name, so that's what I've optimized for.

### API

Exposes a single public unauthenticated endpoint: `GET /xrpc/town.cove.actor.searchActorsTypeahead` with CORS enabled.

Query params:
* `q` - the search query, eg `lewis` or `nel.pet`.
* `limit` - defaults to 10, max 100

Example response

```json
{
  "actors": [
    {
      "did": "did:plc:j67yqsbcjczp73i3b73ei2kj",
      "handle": "hayleigh.dev",
      "displayName": "hayleigh",
      "avatar": "https://cdn.bsky.app/img/avatar/plain/did:plc:j67yqsbcjczp73i3b73ei2kj/bafkreidm57rmcqtl3djyijs57u7wlfdvxojclj2zgogur6fdmelaqwayfe"
    }
  ]
}
```

There's also an `app.bsky.actor.searchActorsTypeahead` alias.

### Diagram

tern discovers new accounts and account changes through jetstream or backfill. The jetstream events just have dids, so tern uses Slingshot to find the handle. This means search should reflect handle changes within seconds. New accounts discovered on the jetstream, or through backfill, are scheduled for enrichment, aka fetching data for scoring. Currently this uses the Bluesky appview and relies on things like account age and follower counts. You can override the appview. A future version may also utilize constellation to count backlinks as a secondary metric for accounts that don't use Bluesky.

```text
Jetstream ──> ingest ────────────────────────> writer ──> fjall
               │ asks                            ▲   └──> Tantivy ──> HTTP API
               ▼                                 │
           Slingshot                             │
backfill ───────────┐                            │
refresh  ───────────┴──> lookup queue ──> lookups┘
                                             │ asks
                                             ▼
                                          AppView
```

An account is considered stale after a week and scheduled for refresh, which captures changes in follower counts.

## Development

Relevant commands:

* `just test`
* `just test-integration`
* `just lint`
* `just dev`

## License

Copyright 2026 Johanna Larsson. Licensed under the [Apache License 2.0](LICENSE).
