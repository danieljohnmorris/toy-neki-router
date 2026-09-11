import { Client } from "pg";
import { allShards, shardConn, topology } from "../src/topology.js";
import { route } from "../src/topology.js";
import xxhash from "xxhash-wasm";

const { h64 } = await xxhash();

async function shardClient(uid: string) {
  const conn = shardConn(uid);
  const c = new Client({ host: conn.host, port: conn.port, user: "postgres", password: "postgres", database: "postgres" });
  await c.connect();
  return c;
}

const shards = allShards("tenant_data");
for (const uid of shards) {
  const c = await shardClient(uid);
  await c.query("drop table if exists events");
  await c.query("create table events (id bigint, tenant_id text, kind text, payload text)");
  await c.end();
  console.log(`created events on ${uid}`);
}

// Seed 30k rows: 3 tenants + 20k orphan rows that belong to no hot tenant,
// so scatter vs point-lookup contrast is visible in row counts.
const tenants = ["acme", "globex", "initech"];
const perShard: Record<string, number> = {};
for (const uid of shards) perShard[uid] = 0;

let id = 1;
for (const tenant of tenants) {
  const target = await route("events", tenant);
  const c = await shardClient(target);
  const rows: string[] = [];
  for (let i = 0; i < 10_000; i++) {
    rows.push(`(${id++}, '${tenant}', 'page_view', '{}')`);
  }
  await c.query(`insert into events values ${rows.join(",")}`);
  perShard[target] += 10_000;
  await c.end();
}

// Noise rows spread by hash across all shards
for (let i = 0; i < 20_000; i++) {
  const tenant = `t${i}`;
  const target = await route("events", tenant);
  const c = await shardClient(target);
  await c.query(`insert into events values (${id++}, '${tenant}', 'signup', '{}')`);
  perShard[target]++;
  await c.end();
}

console.log("rows per shard:", perShard);
console.log("total:", Object.values(perShard).reduce((a, b) => a + b, 0));
