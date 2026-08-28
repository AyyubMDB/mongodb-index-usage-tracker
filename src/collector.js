/**
 * Index usage collector for sharded clusters.
 *
 * MONGODB_URI should point at your sharded cluster's mongos routers. This
 * matters even though no collection here is actually sharded: unsharded
 * databases still live entirely on one "primary shard" internally, which
 * is itself just an ordinary N-node replica set underneath, subject to the
 * exact same per-node $indexStats resets (elections, restarts, maintenance)
 * as any other replica set.
 *
 * Run this hourly via cron. Each run:
 *   1. Confirms MONGODB_URI actually points at mongos (fails fast with a
 *      clear error otherwise).
 *   2. Runs the `listShards` admin command to enumerate every shard and
 *      every replica member of every shard -- NOT via DNS, via a live
 *      query, so it stays correct even if shards are added/removed later.
 *   3. Opens a DIRECT connection (directConnection=true) to every single
 *      one of those members, bypassing mongos entirely for reads. mongos
 *      itself only talks to one member per shard per command, so going
 *      through it would reintroduce the exact single-node blind spot this
 *      whole project exists to solve -- just one layer deeper.
 *   4. Enumerates every application database/collection and runs
 *      `{ $indexStats: {} }` against each member. A shard that doesn't
 *      physically hold a given (unsharded) collection will simply error on
 *      that collection -- caught and skipped gracefully, no special-casing
 *      of "which shard owns this database" required. This also means it
 *      keeps working correctly if a collection is later actually sharded
 *      across multiple shards -- each shard's local contribution just gets
 *      summed in like any other node.
 *   5. Writes raw samples + reset-aware cumulative summaries. A reset
 *      (node restart, election landing on a different member, index
 *      rebuild) is detected via the `accesses.since` timestamp changing --
 *      never just naively summed, so history survives elections correctly.
 *
 * Writes (raw inserts + summary upserts) go through the normal mongos
 * connection, not a direct one -- mongos correctly routes those to
 * whichever shard is primary for the monitoring database.
 *
 * Required permissions: clusterMonitor + readAnyDatabase -- clusterMonitor
 * already includes the `listShards` action, no extra grants needed.
 *
 * Safe to run concurrently across multiple invocations only if you avoid
 * overlap (see the flock example in the cron setup notes) -- the summary
 * update is read-then-write, not atomic, and assumes single-writer.
 */

require("dotenv").config();
const { MongoClient, Long } = require("mongodb");
const { assertShardedCluster, resolveShardMemberEntries } = require("./lib/shard-discovery");

const MONGODB_URI = process.env.MONGODB_URI;
const MONITOR_DB_NAME = process.env.MONITOR_DB_NAME || "index_usage_monitoring";
const RAW_COLLECTION = "index_usage_raw";
const SUMMARY_COLLECTION = "index_usage_summary";

if (!MONGODB_URI) {
  console.error("Missing MONGODB_URI env var. Copy .env.example to .env and fill it in.");
  process.exit(1);
}

const EXCLUDED_DATABASES = new Set(["admin", "local", "config", MONITOR_DB_NAME]);

function toNumberSafe(value) {
  if (value == null) return 0;
  if (Long.isLong(value)) return value.toNumber();
  if (typeof value === "number") return value;
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
}

// Mongo doesn't allow "." in a field name used as a nested path segment
// reliably across all drivers/versions, so sanitize host keys used in
// dot-notation update paths (e.g. perHost.<host>).
function sanitizeHostKey(host) {
  return String(host).replace(/\./g, "_").replace(/\$/g, "_");
}

async function ensureRawTimeSeriesCollection(monitorDb) {
  const existing = await monitorDb
    .listCollections({ name: RAW_COLLECTION })
    .toArray();
  if (existing.length > 0) return;

  await monitorDb.createCollection(RAW_COLLECTION, {
    timeseries: {
      timeField: "timestamp",
      metaField: "metadata",
      granularity: "hours",
    },
  });
  console.log(`Created time-series collection "${RAW_COLLECTION}".`);
}

async function ensureSummaryIndexes(monitorDb) {
  const summaryColl = monitorDb.collection(SUMMARY_COLLECTION);
  await summaryColl.createIndex({ db: 1, collection: 1 });
  await summaryColl.createIndex({ totalOpsSinceTrackingStarted: 1 });
}

async function getAppDatabaseNames(client) {
  const admin = client.db().admin();
  const { databases } = await admin.listDatabases({ nameOnly: true });
  return databases
    .map((d) => d.name)
    .filter((name) => !EXCLUDED_DATABASES.has(name));
}

async function getScannableCollections(db) {
  const infos = await db.listCollections({}, { nameOnly: false }).toArray();
  return infos.filter(
    (info) => info.type !== "view" && !info.name.startsWith("system.")
  );
}

async function getIndexStats(db, collectionName, nodeLabel) {
  try {
    return await db
      .collection(collectionName)
      .aggregate([{ $indexStats: {} }])
      .toArray();
  } catch (err) {
    console.error(
      `  ! $indexStats failed on ${db.databaseName}.${collectionName} (${nodeLabel}): ${err.message}`
    );
    return [];
  }
}

/**
 * Reset-aware delta computation for a single (index, host) counter.
 * Mirrors how Prometheus/monitoring systems handle monotonic counters that
 * can reset to zero (process restart / index rebuild).
 */
function computeDelta(existingPerHostEntry, currentOps, currentSinceMs) {
  if (!existingPerHostEntry) {
    // First time we've ever seen this (index, host) pair. Count everything
    // accumulated so far as "known usage" -- for the purpose of "was this
    // index ever used", that's the correct behavior.
    return { delta: currentOps, wasReset: false };
  }

  const prevSinceMs = new Date(existingPerHostEntry.lastSince).getTime();
  const prevOps = existingPerHostEntry.lastOps;

  const counterReset = currentSinceMs !== prevSinceMs || currentOps < prevOps;
  if (counterReset) {
    return { delta: currentOps, wasReset: true };
  }

  return { delta: currentOps - prevOps, wasReset: false };
}

async function upsertSummary(summaryColl, sample, now) {
  const {
    summaryKey,
    db,
    collection,
    indexName,
    keyPattern,
    host,
    shardName,
    ops,
    since,
  } = sample;
  const sinceMs = new Date(since).getTime();
  const hostKey = sanitizeHostKey(host);

  const existing = await summaryColl.findOne(
    { _id: summaryKey },
    { projection: { [`perHost.${hostKey}`]: 1 } }
  );
  const existingPerHostEntry = existing?.perHost?.[hostKey] || null;

  const { delta, wasReset } = computeDelta(existingPerHostEntry, ops, sinceMs);

  const update = {
    $set: {
      db,
      collection,
      indexName,
      keyPattern,
      lastSeenAt: now,
      [`perHost.${hostKey}`]: {
        host,
        shardName,
        lastOps: ops,
        lastSince: since,
        lastPolledAt: now,
      },
    },
    $inc: {
      totalOpsSinceTrackingStarted: delta,
      pollCount: 1,
      ...(wasReset ? { resetCount: 1 } : {}),
    },
    $setOnInsert: {
      firstSeenAt: now,
    },
  };

  // Audit trail only -- does NOT feed into the total. Records exactly what
  // got folded into totalOpsSinceTrackingStarted right before each detected
  // reset, so the cumulative number can be verified/traced later instead of
  // just trusted. Capped at the most recent 50 entries per index.
  if (wasReset && existingPerHostEntry) {
    update.$push = {
      resetHistory: {
        $each: [
          {
            host,
            shardName,
            detectedAt: now,
            opsCapturedBeforeReset: existingPerHostEntry.lastOps,
            previousSince: existingPerHostEntry.lastSince,
            newSince: since,
          },
        ],
        $slice: -50,
      },
    };
  }

  await summaryColl.updateOne({ _id: summaryKey }, update, { upsert: true });
}

async function pollNode(nodeEntry, nodeLabel, dbNames, rawColl, summaryColl, now) {
  const { uri: nodeUri, shardName } = nodeEntry;
  const client = new MongoClient(nodeUri, {
    readPreference: "secondaryPreferred", // node's own role doesn't matter, we're direct-connected
    serverSelectionTimeoutMS: 10000,
  });

  const rawDocs = [];
  let indexSampleCount = 0;
  let collectionCount = 0;

  try {
    await client.connect();

    for (const dbName of dbNames) {
      const db = client.db(dbName);
      const collections = await getScannableCollections(db);

      for (const collInfo of collections) {
        collectionCount += 1;
        const stats = await getIndexStats(db, collInfo.name, nodeLabel);

        for (const stat of stats) {
          const host = stat.host || nodeLabel;
          const ops = toNumberSafe(stat.accesses?.ops);
          const since = stat.accesses?.since || now;

          rawDocs.push({
            timestamp: now,
            metadata: {
              host,
              shardName,
              db: dbName,
              collection: collInfo.name,
              indexName: stat.name,
              keyPattern: stat.key,
            },
            ops,
            since,
          });

          await upsertSummary(
            summaryColl,
            {
              summaryKey: `${dbName}.${collInfo.name}.${stat.name}`,
              db: dbName,
              collection: collInfo.name,
              indexName: stat.name,
              keyPattern: stat.key,
              host,
              shardName,
              ops,
              since,
            },
            now
          );

          indexSampleCount += 1;
        }
      }
    }

    if (rawDocs.length > 0) {
      await rawColl.insertMany(rawDocs);
    }

    console.log(
      `  - ${nodeLabel} [${shardName} / ${nodeEntry.hostPort}]: OK - ${collectionCount} collection(s), ${indexSampleCount} index sample(s).`
    );
    return { ok: true, collectionCount, indexSampleCount };
  } catch (err) {
    console.error(`  - ${nodeLabel} [${shardName} / ${nodeEntry.hostPort}]: FAILED - ${err.message}`);
    return { ok: false, collectionCount: 0, indexSampleCount: 0 };
  } finally {
    await client.close();
  }
}

async function main() {
  const now = new Date();

  // IMPORTANT: writes (raw inserts + summary upserts) go through the normal
  // mongos-routed client -- NOT a direct-to-one-shard-member connection --
  // so mongos automatically routes writes to whichever shard/node is
  // currently primary for the monitoring database. Only *reading*
  // $indexStats needs direct per-node connections (see pollNode), since
  // that's node-local diagnostic data that mongos would otherwise hide
  // behind its own single-member-per-shard routing.
  const writerClient = new MongoClient(MONGODB_URI, {
    readPreference: "primary",
  });
  await writerClient.connect();

  try {
    await assertShardedCluster(writerClient);

    const nodeEntries = await resolveShardMemberEntries(writerClient, MONGODB_URI);
    const shardCount = new Set(nodeEntries.map((e) => e.shardName)).size;
    console.log(
      `[${now.toISOString()}] Discovered ${nodeEntries.length} member(s) across ${shardCount} shard(s) to poll.`
    );

    const dbNames = await getAppDatabaseNames(writerClient);

    const monitorDb = writerClient.db(MONITOR_DB_NAME);
    await ensureRawTimeSeriesCollection(monitorDb);
    await ensureSummaryIndexes(monitorDb);

    const rawCollection = monitorDb.collection(RAW_COLLECTION);
    const summaryCollection = monitorDb.collection(SUMMARY_COLLECTION);

    let totalSamples = 0;
    let failedNodes = 0;

    for (let i = 0; i < nodeEntries.length; i += 1) {
      const nodeLabel = `node-${i + 1}`;
      const result = await pollNode(
        nodeEntries[i],
        nodeLabel,
        dbNames,
        rawCollection,
        summaryCollection,
        now
      );
      if (!result.ok) failedNodes += 1;
      totalSamples += result.indexSampleCount;
    }

    console.log(
      `[${now.toISOString()}] DONE - polled ${dbNames.length} db(s) across ` +
        `${nodeEntries.length} member(s)/${shardCount} shard(s) (${failedNodes} failed), ` +
        `${totalSamples} total index sample(s) recorded.`
    );
  } finally {
    await writerClient.close();
  }
}

main().catch((err) => {
  console.error("Collector run failed:", err);
  process.exit(1);
});
