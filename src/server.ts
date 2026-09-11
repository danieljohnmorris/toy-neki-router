import net from "node:net";
import { Client } from "pg";
import xxhash from "xxhash-wasm";
import { route, shardConn, shardGroup, allShards, topology } from "./topology.js";

const { h64 } = await xxhash();

// --- Minimal Postgres wire protocol (simple query protocol only) ---

// Extract `WHERE <col> = <literal>` equality predicate values for routing.
function extractPredicates(sql: string): Record<string, string> {
  const preds: Record<string, string> = {};
  const re = /(\w+)\s*=\s*'((?:[^'\\]|\\.)*)'|(\w+)\s*=\s*(\d+(?:\.\d+)?)/gi;
  for (const m of sql.matchAll(re)) {
    preds[m[1] ?? m[3]] = m[2] ?? m[4];
  }
  return preds;
}

function shardKeyColumn(table: string): string {
  const db = Object.values(topology.databases)[0];
  for (const schema of Object.values(db.schemas)) {
    const t = schema.tables[table];
    if (t) {
      const group = topology.shard_groups.find((g) => g.uid === t.shard_group)!;
      return topology.shard_indexes[group.default_shard_index].columns[0];
    }
  }
  throw new Error(`table ${table} not in topology`);
}

// First byte of the XXH64 digest - the routing key matched against key ranges.
// (Real Neki specifies XXH3-64; xxhash-wasm ships XXH64, so hashes differ.)
function routingKey(value: string): string {
  return h64(value).toString(16).padStart(16, "0").slice(0, 2);
}

async function runOnShard(shardUid: string, sql: string): Promise<{ columns: string[]; rows: unknown[][] } | null> {
  const conn = shardConn(shardUid);
  const client = new Client({ host: conn.host, port: conn.port, user: "postgres", password: "postgres", database: "postgres" });
  await client.connect();
  try {
    const res = await client.query(sql);
    if (!res.fields?.length) return null; // DDL/DML: no result set to merge
    return {
      columns: res.fields.map((f) => f.name),
      rows: res.rows.map((r) => res.fields.map((f) => r[f.name])),
    };
  } finally {
    await client.end();
  }
}

function readCString(buf: Buffer, offset: number): [string, number] {
  const end = buf.indexOf(0, offset);
  return [buf.subarray(offset, end).toString("utf8"), end + 1];
}

function short(n: number): Buffer {
  const b = Buffer.alloc(2);
  b.writeInt16BE(n);
  return b;
}

function framed(type: string, body: Buffer): Buffer {
  const len = Buffer.alloc(4);
  len.writeInt32BE(body.length + 4);
  return Buffer.concat([Buffer.from([type.charCodeAt(0)]), len, body]);
}

function readyForQuery(): Buffer {
  return Buffer.from([0x5a, 0, 0, 0, 5, 0x49]); // ReadyForQuery, idle
}

function notice(line: string): Buffer {
  const body = Buffer.concat([Buffer.from([0x4d]), Buffer.from(`${line}\0`), Buffer.from([0])]);
  return framed("N", body); // NoticeResponse so psql renders routing decisions
}

function errorResponse(msg: string): Buffer {
  const body = Buffer.concat([
    Buffer.from([0x53]), Buffer.from("ERROR\0"),
    Buffer.from([0x56]), Buffer.from("XX000\0"),
    Buffer.from([0x4d]), Buffer.from(`${msg.replace(/\0/g, "")}\0`),
    Buffer.from([0]),
  ]);
  return framed("E", body);
}

function rowDescription(columns: string[]): Buffer {
  // Per field: name\0, tableOID(4), attnum(2), typeOID(4)=705 unknown,
  // typlen(2)=-1, atttypmod(4)=-1, format(2)=0 text.
  const fields = columns.map((c) => {
    const meta = Buffer.alloc(18);
    meta.writeInt32BE(705, 6);
    meta.writeInt16BE(-1, 10);
    meta.writeInt32BE(-1, 12);
    return Buffer.concat([Buffer.from(`${c}\0`), meta]);
  });
  const body = Buffer.concat([short(columns.length), ...fields]);
  return framed("T", body); // RowDescription
}

function dataRow(cells: unknown[]): Buffer {
  const enc = cells.map((v) => {
    const b = Buffer.from(v === null ? "" : String(v), "utf8");
    const l = Buffer.alloc(4);
    l.writeInt32BE(b.length);
    return Buffer.concat([l, b]);
  });
  const body = Buffer.concat([short(cells.length), ...enc]);
  return framed("D", body); // DataRow
}

function commandComplete(tag: string): Buffer {
  return framed("C", Buffer.from(`${tag}\0`));
}

// Sum the trailing numeric aggregate per group key (leading columns).
function mergeGroups(columns: string[], rows: unknown[][]): unknown[][] {
  const keyCols = columns.length - 1;
  const merged = new Map<string, { key: unknown[]; total: number }>();
  for (const row of rows) {
    const key = row.slice(0, keyCols);
    const k = JSON.stringify(key);
    const entry = merged.get(k) ?? { key, total: 0 };
    const v = row[keyCols];
    entry.total += typeof v === "number" ? v : Number(v);
    merged.set(k, entry);
  }
  return [...merged.values()]
    .sort((a, b) => b.total - a.total)
    .map((e) => [...e.key, String(e.total)]);
}

async function handleQuery(socket: net.Socket, sql: string): Promise<Buffer[]> {
  const out: Buffer[] = [];
  const selectMatch = /select\s+.+?\s+from\s+(\w+)/i.exec(sql);

  if (selectMatch && shardGroup(selectMatch[1]) !== "authoritative") {
    const table = selectMatch[1];
    const preds = extractPredicates(sql);
    const keyCol = shardKeyColumn(table);
    const keyVal = preds[keyCol];

    if (keyVal !== undefined) {
      const target = await route(table, keyVal);
      out.push(notice(`router: ${keyCol}='${keyVal}' -> xxhash -> 0x${routingKey(keyVal)} -> ${target}`));
      const res = await runOnShard(target, sql);
      if (res) {
        out.push(rowDescription(res.columns), ...res.rows.map((r) => dataRow(r)), commandComplete(`SELECT ${res.rows.length}`));
      } else {
        out.push(commandComplete("OK"));
      }
    } else {
      // Scatter-gather: fan out to every shard in the group.
      const shards = allShards(shardGroup(table));
      out.push(notice(`router: no shard-key predicate -> scatter across ${shards.join(", ")}`));
      const results = await Promise.all(shards.map((s) => runOnShard(s, sql)));
      let columns: string[] = [];
      let rows: unknown[][] = [];
      for (const r of results) {
        if (!r) continue;
        columns = r.columns;
        rows.push(...r.rows);
      }
      if (/group\s+by/i.test(sql)) {
        // Partial aggregation: each shard already grouped locally; sum the
        // numeric aggregate column across shards keyed by the group columns.
        rows = mergeGroups(columns, rows);
        out.push(notice(`router: merged partial aggregates from ${results.filter(Boolean).length} shards`));
      }
      if (columns.length) {
        out.push(rowDescription(columns), ...rows.map((r) => dataRow(r)), commandComplete(`SELECT ${rows.length}`));
      } else {
        out.push(commandComplete("OK"));
      }
    }
  } else {
    // Unsharded / DDL / DML: everything authoritative lands on shard-a.
    const res = await runOnShard("shard-a", sql);
    if (res) {
      out.push(rowDescription(res.columns), ...res.rows.map((r) => dataRow(r)), commandComplete(`SELECT ${res.rows.length}`));
    } else {
      out.push(commandComplete("OK"));
    }
  }
  return out;
}

const server = net.createServer((socket) => {
  let authenticated = false;
  let pending = Buffer.alloc(0);

  socket.on("error", () => socket.destroy());

  socket.on("data", (chunk) => {
    pending = Buffer.concat([pending, chunk]);
    while (true) {
      const msg = nextMessage();
      if (!msg) return;
      handle(msg);
    }
  });

  function nextMessage(): Buffer | null {
    if (!authenticated) {
      // Pre-auth frames (SSLRequest, GSSENCRequest, StartupMessage) are
      // untyped: Int32 total length, Int32 code, then payload.
      if (pending.length < 8) return null;
      const len = pending.readInt32BE(0);
      if (pending.length < len) return null;
      return pending.subarray(0, len);
    }
    if (pending.length < 5) return null;
    const len = pending.readInt32BE(1);
    if (pending.length < 1 + len) return null;
    return pending.subarray(0, 1 + len);
  }

  function handle(buf: Buffer) {
    pending = pending.subarray(buf.length);
    if (!authenticated) {
      const code = buf.readInt32BE(4);
      if (code === 80877103 || code === 80877104) {
        socket.write(Buffer.from([0x4e])); // SSL/GSSENC declined; client retries cleartext
        return;
      }
      authenticated = true;
      socket.write(Buffer.concat([
        framed("R", Buffer.from([0, 0, 0, 0])), // AuthenticationOk
        readyForQuery(),
      ]));
      return;
    }

    const type = String.fromCharCode(buf[0]);
    if (type === "X") {
      socket.end();
      return;
    }
    if (type !== "Q") {
      socket.write(errorResponse("toy router only speaks the simple query protocol"));
      socket.write(readyForQuery());
      return;
    }

    const [sql] = readCString(buf, 5);
    handleQuery(socket, sql)
      .then((msgs) => socket.write(Buffer.concat([...msgs, readyForQuery()])))
      .catch((err: Error) => {
        socket.write(errorResponse(err.message));
        socket.write(readyForQuery());
      });
  }
});

const PORT = Number(process.env.PORT ?? 54320);
server.listen(PORT, () => console.log(`toy-neki-router listening on :${PORT}`));
