/**
 * One-shot diagnostic dump for a single index -- SHARDED CLUSTER VARIANT.
 * Run this and paste the full output back for debugging.
 *
 * Usage:
 *   node src/diagnose.js <db> <collection> <indexName>
 */

require("dotenv").config();
const { MongoClient } = require("mongodb");
const { assertShardedCluster, resolveShardMemberEntries } = require("./lib/shard-discovery");

const MONGODB_URI = process.env.MONGODB_URI;
const MONITOR_DB_NAME = process.env.MONITOR_DB_NAME || "index_usage_monitoring";

const [, , dbName, collectionName, indexName] = process.argv;

if (!dbName || !collectionName || !indexName) {
  console.error("Usage: node src/diagnose.js <db> <collection> <indexName>");
  process.exit(1);
}

function stringify(obj) {
  return JSON.stringify(
    obj,
    (key, value) => {
      if (value && value.constructor && value.constructor.name === "Long") {
        return value.toString();
      }
      return value;
    },
    2
  );
}

async function main() {
  const client = new MongoClient(MONGODB_URI, { readPreference: "primary" });
  await client.connect();

  try {
    await assertShardedCluster(client);
    const nodeEntries = await resolveShardMemberEntries(client, MONGODB_URI);

    console.log("=".repeat(70));
    console.log(
      `1. LIVE $indexStats on EVERY member of EVERY shard (${nodeEntries.length} member(s))`
    );
    console.log("=".repeat(70));

    for (const entry of nodeEntries) {
      const nodeClient = new MongoClient(entry.uri, {
        readPreference: "secondaryPreferred",
        serverSelectionTimeoutMS: 10000,
      });
      try {
        await nodeClient.connect();
        const stats = await nodeClient
          .db(dbName)
          .collection(collectionName)
          .aggregate([{ $indexStats: {} }])
          .toArray();
        const match = stats.find((s) => s.name === indexName);
        console.log(
          `\n[${entry.shardName} / ${entry.hostPort}]\n` +
            (match ? stringify(match) : "  (index not found on this member)")
        );
      } catch (err) {
        console.log(`\n[${entry.shardName} / ${entry.hostPort}]\n  ERROR: ${err.message}`);
      } finally {
        await nodeClient.close();
      }
    }

    console.log("\n" + "=".repeat(70));
    console.log("2. Current summary document for this index");
    console.log("=".repeat(70));
    const summaryId = `${dbName}.${collectionName}.${indexName}`;
    const summary = await client
      .db(MONITOR_DB_NAME)
      .collection("index_usage_summary")
      .findOne({ _id: summaryId });
    console.log(summary ? stringify(summary) : `No summary doc found for _id="${summaryId}"`);

    console.log("\n" + "=".repeat(70));
    console.log("3. All raw samples for this index, oldest to newest");
    console.log("=".repeat(70));
    const rawSamples = await client
      .db(MONITOR_DB_NAME)
      .collection("index_usage_raw")
      .find(
        {
          "metadata.db": dbName,
          "metadata.collection": collectionName,
          "metadata.indexName": indexName,
        },
        {
          projection: {
            "metadata.host": 1,
            "metadata.shardName": 1,
            ops: 1,
            since: 1,
            timestamp: 1,
            _id: 0,
          },
        }
      )
      .sort({ timestamp: 1 })
      .toArray();
    console.log(stringify(rawSamples));
  } finally {
    await client.close();
  }
}

main().catch((err) => {
  console.error("Diagnose failed:", err);
  process.exit(1);
});
