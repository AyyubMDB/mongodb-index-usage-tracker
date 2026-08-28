# MongoDB Index Usage Tracker

A lightweight tool that tracks how often each index on your MongoDB Atlas
cluster is actually used — reliably, over days or weeks, even across primary
elections and node restarts — so you can confidently identify indexes that
are safe to remove.

## Why this exists

MongoDB Atlas can already show you index usage stats via `$indexStats`, but
that data lives **per node**, not per cluster, and it **resets** whenever a
node restarts or a primary election happens (routine maintenance, patching,
scaling events). In practice this means:

- An index that looks "unused" in the Atlas UI right now might just be
  unused _since the last restart_ — it could have years of real usage
  history that's now invisible.
- On a sharded cluster specifically, `mongos` (the query router) only talks
  to one member per shard per command, so even querying through the normal
  connection string can miss usage happening on other nodes.

This tool solves both problems by polling **every replica member of every
shard directly** on a schedule, and maintaining a running total that
survives resets — so after a tracking period, you get a trustworthy answer
to "has this index actually been used at all," not just "was it used since
the last time something restarted."

It never modifies or deletes anything on your cluster. It only reads
`$indexStats` and writes to its own small tracking collection.

## What you get

- A **report** (command line or web dashboard) listing every index across
  every database, classified as:
  - **Keep (in use)** — has recorded usage, leave it alone.
  - **Candidate for removal** — zero recorded usage over the tracking
    window, safe to consider dropping (after the safety steps below).
  - **Review manually** — zero recorded query usage, but enforces a unique
    constraint; verify before touching.
  - **Do not drop (TTL)** — a TTL index; these expire documents rather than
    serve queries, so zero query usage is expected and normal.
  - **Too early** — not enough tracking data yet to draw a conclusion.
- A full audit trail: every time a node's counter resets (restart,
  election), it's logged with a timestamp and exactly how much usage was
  captured right before the reset — so the final numbers are traceable, not
  just trusted.

## How it works (brief technical overview)

1. **`collector.js`** runs on a schedule (hourly is the default recommendation)
   and:
   - Enumerates every shard and every replica member of every shard via the
     `listShards` command.
   - Connects directly to each member and runs `{ $indexStats: {} }` against
     every collection.
   - Records a running, reset-aware cumulative total per index — a restart
     or election never silently erases history, it's detected and handled
     explicitly.
2. **`report.js`** / the web dashboard (**`npm run ui`**) read that tracked
   data at any time and classify every index as above. Nothing here writes
   to your application databases — only to its own small tracking database.

## Prerequisites

- A machine that can run Node.js 18+ (either an existing app server, or
  even a laptop for a quick ad-hoc check — see "Running it" below)
- Network access from that machine to the cluster (standard Atlas IP
  Access List entry — no VPN/peering changes needed)
- A dedicated, limited-privilege database user (created below) — this tool
  never needs write access to your application data, only read access plus
  write access to its own tracking database

## Setup

### 1. Create a dedicated monitoring database user

In `mongosh` (connected as an admin) or via the Atlas UI → **Database
Access** → **Add New Database User**:

```js
db.getSiblingDB("admin").createUser({
  user: "index_usage_monitor",
  pwd: "<choose-a-strong-password>",
  roles: [
    { role: "clusterMonitor", db: "admin" },
    { role: "readAnyDatabase", db: "admin" },
    { role: "readWrite", db: "index_usage_monitoring" },
  ],
});
```

This user can read everywhere and write only to its own
`index_usage_monitoring` database — it cannot modify or delete anything in
your application data.

### 2. Add the running machine's IP to Atlas Network Access

Atlas UI → **Network Access** → **Add IP Address**.

### 3. Install

```bash
npm install
```

### 4. Configure

```bash
cp .env.example .env
```

Edit `.env`:

```
MONGODB_URI=mongodb+srv://index_usage_monitor:<password>@<your-cluster>.mongodb.net/?retryWrites=true&w=majority
MONITOR_DB_NAME=index_usage_monitoring
MIN_POLLS_FOR_CONFIDENCE=100
```

`MONGODB_URI` is the cluster's normal connection string (the one that
resolves to `mongos`) — the same kind of string an application would use.
`MONITOR_DB_NAME` is created automatically on first run; nothing to set up
manually.

## Running it

You have two options — pick whichever fits your infrastructure. Both do
exactly the same thing; the only difference is where/how often it runs.

### Option A: Scheduled via cron on an app server (recommended)

Best if you have any existing always-on Linux server or container where a
small scheduled job can live.

```bash
crontab -e
```

Add (adjust the path; the `flock` guard prevents overlapping runs if a poll
ever takes longer than an hour):

```
0 * * * * /usr/bin/flock -n /tmp/idx-collector.lock node /path/to/this/project/src/collector.js >> /var/log/idx-collector.log 2>&1
```

This polls once per hour, continuously, with no manual intervention needed.
Let it run for the recommended tracking window (see below), then generate
the report whenever convenient.

### Option B: Run manually from a local machine

If you'd rather not set up a scheduled job right away, you can simply run
it by hand whenever convenient:

```bash
node src/collector.js
```

Each run polls once and exits — printing a summary like:

```
[timestamp] Discovered 4 member(s) across 1 shard(s) to poll.
  - node-1 [shard-0 / host-00...:27017]: OK - 309 collection(s), 588 index sample(s).
  - node-2 [shard-0 / host-01...:27017]: OK - 309 collection(s), 588 index sample(s).
  ...
[timestamp] DONE - polled N db(s) across 4 member(s)/1 shard(s) (0 failed), ... index sample(s) recorded.
```

The tradeoff: usage data only gets captured at the moments you happen to
run it, so you'll build a less complete picture than an always-on hourly
cron job — but it's a fine way to get started immediately, or to do a
quick spot-check, without needing a server set up first. You can always
switch to Option A later; both write to the same tracking data.

## Viewing results

### Command line

```bash
node src/report.js            # table view
node src/report.js --csv > index_usage_report.csv
node src/report.js --json
```

### Web dashboard

```bash
npm run ui
```

Then open `http://localhost:4000` in a browser. It shows summary cards
(candidates for removal / review manually / do-not-drop / too-early /
keep), lets you search, filter by database or recommendation, sort any
column, and export the currently-filtered view to CSV. Click "Refresh"
after the collector has run again to see updated numbers. This is
read-only and never writes to your cluster.

### Debugging a specific index

```bash
node src/diagnose.js <database> <collection> <indexName>
```

Dumps live `$indexStats` from every shard member individually, plus the
full tracked history for that one index — useful if any number ever looks
surprising and you want to see exactly where it came from.

## How long to track before deciding

We recommend tracking for **at least 3–4 weeks** before treating a
"candidate for removal" as final. If any part of your business has
month-end, quarter-end, or other periodic batch/reporting jobs that only
touch certain indexes occasionally, extend the window to cover at least one
full cycle — an index that's silent for two weeks could still be load-
bearing on day 30.

## Before you drop anything

This tool never deletes anything automatically, and we don't recommend
dropping a "candidate" index directly either. For each one:

```js
// 1. Hide it first -- reversible, no rebuild needed to undo
db.collection.hideIndex("indexName");

// 2. Monitor application/query performance for about a week

// 3. If nothing broke, drop it for real
db.collection.dropIndex("indexName");
```

Hiding an index makes MongoDB stop considering it for queries without
actually removing it — if something unexpected breaks, un-hiding is
instant. Dropping is the only step that requires a rebuild to undo, so
treat it as the final, deliberate step, not the first one.

Note also that `_id_` (the default index every collection has) never
appears in the report — MongoDB doesn't allow dropping or hiding it, so
there's nothing to decide about it.

## Troubleshooting

- **`MongoServerError: bad auth`** — check the password in `.env` and that
  the user was created on the `admin` database.
- **Connection timeout** — check that the running machine's IP is in Atlas
  Network Access.
- **A specific member's log line says `FAILED`** — usually a temporary
  network blip or that member being down during an election. As long as
  other members still succeed, no data is lost.
- **`$indexStats failed on <db>.<coll>` for some members but not others** —
  expected and harmless when a shard doesn't physically hold that
  (unsharded) collection.
- **Everything shows `TOO EARLY`** — normal early on; wait for more polls
  to accumulate (governed by `MIN_POLLS_FOR_CONFIDENCE` in `.env`).

## Questions

Reach out to ayyub.kolsawala@mongodb.com with any questions about setup,
results, or before acting on any specific recommendation.
