#!/usr/bin/env node
// Demo: connect to the toy router like any Postgres client, run point
// lookups vs scatter queries, and print the routing decisions.
import { Client } from "pg";

const c = new Client({ host: "localhost", port: 54320, user: "postgres", password: "postgres", database: "postgres" });
const notices: string[] = [];
c.on("notice", (n) => notices.push(String(n.message).trim()));
await c.connect();

async function q(label: string, sql: string) {
  console.log(`\n> ${sql}`);
  notices.length = 0;
  const res = await c.query(sql);
  for (const n of notices) console.log(n);
  console.log(`  ${label}: ${res.rowCount ?? res.rows.length} rows`, res.rows.slice(0, 3));
}

await q("point lookup", "select count(*) from events where tenant_id = 'acme'");
await q("point lookup, different tenant, different shard", "select count(*) from events where tenant_id = 'globex'");
await q("scatter", "select tenant_id, count(*) from events group by tenant_id order by 2 desc");
await c.end();
