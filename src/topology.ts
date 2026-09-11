import xxhash from "xxhash-wasm";

// Mirrors the shape of a Neki data topology document:
// https://planetscale.com/docs/neki/data-topology
export interface DataTopology {
  databases: Record<string, {
    schemas: Record<string, {
      tables: Record<string, { shard_group: string }>;
    }>;
  }>;
  shard_groups: Array<{
    uid: string;
    default_shard_index: string;
    key_ranges: Array<{ shard_uid: string; start?: string; end?: string }>;
  }>;
  shard_indexes: Record<string, { type: "xxhash" | "modulo" | "range"; columns: string[] }>;
  authoritative_shard_group: string;
}

export const topology: DataTopology = {
  databases: {
    analytics: {
      schemas: {
        public: {
          tables: {
            events: { shard_group: "tenant_data" },
          },
        },
      },
    },
  },
  shard_groups: [
    {
      uid: "tenant_data",
      default_shard_index: "xxhash_tenant_id",
      // Hex keyspace [00, ff). Three shards split it unevenly —
      // shard-c takes the hot tail, like a real skewed workload.
      key_ranges: [
        { shard_uid: "shard-a", end: "40" },
        { shard_uid: "shard-b", start: "40", end: "80" },
        { shard_uid: "shard-c", start: "80" },
      ],
    },
  ],
  shard_indexes: {
    xxhash_tenant_id: { type: "xxhash", columns: ["tenant_id"] },
  },
  authoritative_shard_group: "authoritative",
};

const SHARD_CONNS: Record<string, { host: string; port: number }> = {
  "shard-a": { host: "localhost", port: 5433 },
  "shard-b": { host: "localhost", port: 5434 },
  "shard-c": { host: "localhost", port: 5435 },
};

export function shardGroup(table: string) {
  const db = Object.values(topology.databases)[0];
  for (const schema of Object.values(db.schemas)) {
    if (schema.tables[table]) return schema.tables[table].shard_group;
  }
  return topology.authoritative_shard_group;
}

// XXH3-64 of the shard key, rendered as hex, matched against the group's
// key ranges — same pipeline the Neki router runs.
export async function route(table: string, shardKeyValue: string): Promise<string> {
  const groupUid = shardGroup(table);
  const group = topology.shard_groups.find((g) => g.uid === groupUid);
  if (!group) throw new Error(`no shard group for table ${table}`);
  const index = topology.shard_indexes[group.default_shard_index];
  if (index.type !== "xxhash") throw new Error(`toy router only implements xxhash, not ${index.type}`);

  const { h64 } = await xxhash();
  const hash = h64(shardKeyValue).toString(16).padStart(16, "0");
  const routingKey = hash.slice(0, 2); // first byte of the digest

  for (const range of group.key_ranges) {
    const start = range.start ?? "00";
    if (routingKey >= start && (range.end === undefined || routingKey < range.end)) return range.shard_uid;
  }
  throw new Error(`routing key ${routingKey} matched no range in ${groupUid}`);
}

export function shardConn(shardUid: string) {
  return SHARD_CONNS[shardUid];
}

export function allShards(groupUid: string): string[] {
  const group = topology.shard_groups.find((g) => g.uid === groupUid)!;
  return group.key_ranges.map((r) => r.shard_uid);
}
