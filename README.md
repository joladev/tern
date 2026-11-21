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
      "avatar": "https://cdn.bsky.app/img/avatar/plain/did:plc:j67yqsbcjczp73i3b73ei2kj/bafkreighzctmijy5hm5f7o6ghauu6cko5bbjzs75vm24islnz2qm7cwfnu",
      "labels": [],
      "createdAt": "2023-04-25T13:42:13.109Z"
    }
  ]
}
```

There's also an `app.bsky.actor.searchActorsTypeahead` alias.

## Design

I'll try to note down some of the design decisions here.

* Use multiple data sources to reduce reliance on Bluesky.
* Extensibility in regards to additional data sources.
* Run with relatively few resources.
* Readability is important.

The core concept in tern is an identity. An identity is represented by a did, and may have information such as a `createdAt` timestamp and a handle. tern is designed to use multiple sources for account information. A simpler implementation would be to just use the Bluesky AppView, but tern aims to function even if Bluesky were to go away, and so most data has a redundant source to fall back on. Each identity is kept in the fjall data store, together with any related account information that has been fetched, and a timestamp for when it was last refreshed.

tern is built to be extensible with additional data sources. The data processing pipeline, described below, takes inputs from multiple sources and schedules them all in a job queue with two priority lanes: a slow one for the weekly refresh, and a fast one for changes discovered through the jetstream. The reason for the split is that a user who changes their account handle should see it reflected in the service immediately, while a change in follower count overall will only have a minimal impact on scoring and can wait.

All data flows through the same processing pipeline, with each identity being inserted into the job queue for processing. The refresh worker takes batches of identities and fetches data from the available data sources. There is overlap in the data, so it's merged with some basic rules. As an example, an account that is missing in the Bluesky AppView, but resolved through Slingshot, is recorded.

```text
Jetstream ──────┐
backfill (once) ┼─ add jobs ─> work queue ─> refresh ── asks all at once ──> Slingshot identity
stale scan ─────┘              (in fjall)       │                           Slingshot profile record
                                                │                           Bluesky AppView
                                                ▼
                                             writer ──> fjall
                                                   └──> Tantivy ──> HTTP API
```

Every update to an identity also triggers a reindexing, so the updated data is surfaced within seconds. The scoring algorithm uses account age and followers, with a bonus for prefix matches, and an exact handle match always wins.

```text
rank = 100 · log2(1 + followers) + 10 · log2(1 + age in days) + bonus

bonus = 650   if the first part of the handle equals the query
        150   if the handle starts with the query
        0     otherwise
```

## Independence from Bluesky

Bluesky provides great infrastructure and is the easiest to use source of data for a service like tern, but it is ultimately designed for the needs of Bluesky. One way in which this plays out is that an account banned by Bluesky, but that lives on an independent PDS, stops being returned by the Bluesky AppView. Relying on that data would be at odds with the goals of the decentralized protocol. A Bluesky ban should not be a global ban for that account.

Instead tern treats Bluesky's labels as just that, labels, and resolves identities and profiles through Slingshot, fetching the data directly from the user's PDS. tern returns the accounts and the labels, and it is up to each client to decide which labelers and labels to apply, and whether to hide any accounts.

## Development

Relevant commands:

* `just test`
* `just integration`
* `just lint`
* `just dev`

## Roadmap

- [x] Surface labels
- [x] Filter bad words
- [ ] Offer a getProfile endpoint
- [ ] Subscribe to labelers
- [ ] Record reason why an account is inactive
- [ ] Retries with full jitter exponential backoff
- [ ] Paginating hubble as an alternative to looking for stale accounts
- [ ] Constellation backlinks as a fallback for counting followers

## License

Copyright 2026 Johanna Larsson. Licensed under the [Apache License 2.0](LICENSE).
