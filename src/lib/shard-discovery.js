/**
 * Shared shard/member discovery logic for sharded clusters, used by both
 * collector.js and diagnose.js so they never drift out of sync.
 */

/**
 * Extracts just the credentials + authSource we need to build direct
 * connection URIs to individual shard members, from whatever MONGODB_URI
 * was given (mongodb:// or mongodb+srv://, doesn't matter -- we don't need
 * DNS here since listShards gives us real hosts directly).
 */
function parseCredentials(uri) {
  const match = uri.match(/^mongodb(?:\+srv)?:\/\/([^@]+)@([^/?]+)(\/[^?]*)?(\?.*)?$/);
  if (!match) {
    throw new Error(
      "Could not parse MONGODB_URI. Expected mongodb:// or mongodb+srv:// with credentials included."
    );
  }
  const [, userInfo, , , queryString] = match;
  const params = new URLSearchParams(queryString ? queryString.slice(1) : "");
  const authSource = params.get("authSource") || "admin";
  return { userInfo, authSource };
}

/**
 * Confirms MONGODB_URI actually points at a mongos router. Throws a clear,
 * actionable error otherwise rather than silently misbehaving.
 */
async function assertShardedCluster(client) {
  const hello = await client.db("admin").command({ hello: 1 });
  if (hello.msg !== "isdbgrid") {
    throw new Error(
      "MONGODB_URI does not appear to point at a sharded cluster (mongos). " +
        "This project is specifically for sharded clusters. If you're targeting " +
        "a plain replica set, use the non-sharded index-usage-tracker project instead."
    );
  }
}

/**
 * Enumerates every shard and every replica member of every shard via the
 * listShards admin command (a live query, not DNS), and builds one direct
 * connection URI per member.
 *
 * Returns: [{ shardName, hostPort, uri }]
 */
async function resolveShardMemberEntries(client, uri) {
  const { userInfo, authSource } = parseCredentials(uri);
  const { shards } = await client.db("admin").command({ listShards: 1 });

  if (!shards || shards.length === 0) {
    throw new Error("listShards returned no shards -- is this really a sharded cluster?");
  }

  const entries = [];
  for (const shard of shards) {
    const shardName = shard._id;
    // host is typically "replSetName/host1:port,host2:port,host3:port".
    // Fall back to treating the whole value as a host list if there's no
    // replSetName prefix (rare, e.g. an unreplicated shard).
    const slashIdx = shard.host.indexOf("/");
    const hostList = slashIdx >= 0 ? shard.host.slice(slashIdx + 1) : shard.host;
    const hosts = hostList
      .split(",")
      .map((h) => h.trim())
      .filter(Boolean);

    for (const hostPort of hosts) {
      const params = new URLSearchParams();
      params.set("authSource", authSource);
      params.set("tls", "true");
      params.set("directConnection", "true");

      entries.push({
        shardName,
        hostPort,
        uri: `mongodb://${userInfo}@${hostPort}/?${params.toString()}`,
      });
    }
  }

  return entries;
}

module.exports = { parseCredentials, assertShardedCluster, resolveShardMemberEntries };
