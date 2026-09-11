# toy-neki-router

A ~260-line Postgres **wire-compatible sharding proxy**, built to understand how [Neki](https://neki.dev/) (sharded Postgres by PlanetScale) routes queries. It speaks enough of the Postgres wire protocol that `psql` or any `pg` driver can connect to it, parses each simple query, routes by shard key, and merges partial aggregates on scatter queries.

Three real Postgres shards run in Docker behind it. The routing topology mirrors the shape of a [Neki data topology document](https://planetscale.com/docs/neki/data-topology).

![Architecture](diagrams/toy-router-architecture.png)

## What it demonstrates

- **Point lookups hit one shard.** `select count(*) from events where tenant_id = 'acme'` - the router parses the predicate, hashes the key with xxhash64, maps the hash into the key range, and talks to exactly one Postgres.
- **No shard key means scatter-gather.** `select tenant_id, count(*) from events group by tenant_id` fans out to every shard in parallel and merges the partial aggregates.
- **The routing decision is visible.** Every routed query emits a notice: `router: tenant_id='acme' -> xxhash -> 0xbb -> shard-c`.

## Run it

```bash
docker compose up -d   # 3 Postgres shards on 5433, 5434, 5435
npm install
npm run seed           # 50k rows across shards, deliberately skewed
npm start              # router listens on :54320
npm run demo           # connect like any pg client, show routing
```

Or connect directly, since it speaks the wire protocol:

```bash
psql "postgres://postgres:postgres@localhost:54320/postgres" \
  -c "select count(*) from events where tenant_id = 'acme'"
```

Demo output:

```
> select count(*) from events where tenant_id = 'acme'
router: tenant_id='acme' -> xxhash -> 0xbb -> shard-c
  point lookup: 1 rows [ { count: '10000' } ]

> select tenant_id, count(*) from events group by tenant_id order by 2 desc
router: no shard-key predicate -> scatter across shard-a, shard-b, shard-c
router: merged partial aggregates from 3 shards
  scatter: 20003 rows [...]
```

## How routing works

`src/topology.ts` mirrors the Neki data topology shape: tables map to shard groups, shard groups map to hash key ranges, key ranges map to shards.

1. Extract `WHERE <shard_key> = <literal>` from the query text.
2. Hash the literal: `h64(key)`, take the first hex byte (`00` to `ff`).
3. Linear-scan the group's key ranges to find the owning shard.
4. Open a real connection to that Postgres and replay the query.

Queries without the shard key scatter to every shard in the group; the router concatenates rows and, for `group by` queries, merges counts by key.

![Query lifecycle](diagrams/toy-router-sequence.png)

## What's deliberately fake

- **Simple query protocol only** - no extended protocol, no prepared statements.
- **Regex query parsing** - real Neki has a full Postgres query parser and distributed planner.
- **Merge is naive** - it merges `count` aggregates only; no sort merge, no limit re-application after merge (`limit 3` returns up to 3 rows per shard), no joins across shards.
- **XXH64, not XXH3-64** - Neki specifies XXH3-64; `xxhash-wasm` ships XXH64. Same pipeline shape, different digests, so shard assignments won't match a real Neki cluster's.
- **Single router process** - real Neki routers are horizontally scalable and pool connections via sidecars.
- **Writes aren't routed** - every INSERT/UPDATE/DELETE goes to shard-a; only SELECT carries a shard-key predicate. That's why the seed script talks to the shards directly.
- **No resharding** - Neki's headline feature is zero-downtime splits; here you'd have to rewrite the topology JSON and move rows by hand.

![When to shard](diagrams/toy-router-when-to-shard.png)

## When to shard

The toy makes the cost visible: any query missing the shard key touches every shard. Shard when you've measured a ceiling that vertical scale and caching can't fix **and** every hot query can carry the shard key. Otherwise: bigger instance, read replicas, partitioning, caching.

The blog post walking through the build: [A toy Neki router to understand sharded Postgres](https://danieljohnmorris.com/writing/toy-neki-router-sharded-postgres/).

## License

MIT
