# Which database can keep up with a thousand agents on one branch?

Benchmarks behind the project page at https://terramind.org/agent-scale-database/.

We are designing **swarm**, open-source source control for AI agents, starting from the design
Anthropic published in *Redesigning source control*
(https://www.anthropic.com/engineering/redesigning-source-control). Before choosing where its
metadata lives, we measured four candidate databases on the workloads that decide it. Everything
here is our own measurement on one laptop and is not peer-reviewed.

## What is measured

| Workload | What one operation does |
|---|---|
| `h1` hot ref | Land one change on the branch `main`: lock its row, bump the version, append a ref-log row and a landed-index row, commit. **naive** = one transaction per land. **coalesced** = every land goes to one owner task, which commits whatever is queued as one transaction. |
| `h2` ingest | Insert-or-ignore 1,000 content-addressed object rows per batch, 30% of them repeats; 8 writers, 5,000,000 rows offered. |
| `h3` point reads | Read one object row by id; 32 readers for 15 seconds. |

Each hot-ref cell runs for a fixed 10-second window after every connection is open, at 1, 16,
64, 256 and 1,000 concurrent landers.

## Setup

One machine: Apple M4 Pro, 14 cores, 24 GB, internal SSD, macOS 27.0.1. Databases and load generator
share it; no network, no replicas.

| Database | Version | Settings |
|---|---|---|
| Postgres | 17.11 (Homebrew) | single node; `synchronous_commit = on`, `fsync = on`, `wal_sync_method = fsync_writethrough`, `shared_buffers = 2GB` |
| YugabyteDB | 2025.2.0.0 (macOS arm64 build) | one node, replication factor 1, `durable_wal_write = true` |
| SQLite | bundled with rusqlite 0.40 | WAL, `synchronous = FULL`, `fullfsync = ON` |
| FoundationDB | 7.3.77 (macOS arm64 package) | one process, `single` redundancy, `ssd-redwood-1` |

**Durability.** On macOS, `fsync()` does not flush the drive's cache; `fcntl(F_FULLFSYNC)` does.
`code/fsync_probe.py` times both; our recorded run (`results/fsync_probe.txt`) gave a median of
0.051 ms for `fsync` and 4.624 ms for `F_FULLFSYNC`. FoundationDB issues `F_FULLFSYNC` by default,
Postgres and SQLite do not, so we configured both to flush fully. YugabyteDB is set to sync its
WAL on every write; we could not confirm that this is a full flush on macOS, and its single-lander
commit time is below the flush time we measured, which suggests it is not.

## Results

`results/final/results.jsonl` is the dataset (one JSON line per run); `results/final/summary.json`
is the same numbers condensed; `results/final/env.txt` says which run each row came from.

- Postgres, YugabyteDB and SQLite: run `20261017T120757Z-final`.
- FoundationDB hot ref: run `20261018T001641Z-fdb-h1-pipelined`, on a fresh cluster with
  `--memory 3GiB --cache-memory 1GiB`.
- FoundationDB ingest and reads: run `20261017T235654Z-final-fdb`, with `--memory 6GiB`.

Runs we did not use are kept: `discarded-first-attempt` counted connection setup inside the
measured window; `discarded-concurrent-runs` overlapped with another run. The FoundationDB hot-ref
rows of `20261017T235654Z-final-fdb` were replaced because our first version read each change's
landed key one after another inside the transaction, which held FoundationDB's coalesced result
to 4,882 lands a second at 1,000 landers; reading them concurrently gave 21,695.

The code in `code/` is the version that produced the final dataset. The only change after the
Postgres, YugabyteDB and SQLite run was the FoundationDB fix above, which touches only
FoundationDB's code path.

## Settings and notes

- `settings/`: the exact Postgres configuration, YugabyteDB and FoundationDB start commands, and
  SQLite pragmas used.
- `NOTES.md`: what happened during the session, in order, including the discarded runs, the
  FoundationDB bug, and the memory and disk readings.
- `code/site_table.py`: generates the table data on the project page from `summary.json`.

## Limits

- One laptop, single-node databases. A production deployment replicates across zones, which adds a
  consensus round trip to every commit; none of these numbers include it.
- Ingest stops at 5,000,000 offered rows (about 3.5 million unique), all in memory.
- Naive landers stop at 256 for Postgres and YugabyteDB: one connection per lander, and 1,000
  server processes would exhaust this machine's memory.
- The machine was under memory pressure late in the session (macOS reported 18 GB of swap in use);
  FoundationDB's rerun happened after the other databases were stopped.

## Run it

```sh
# Start the four databases with the settings above, then:
cd code
export FDB_LIB=/path/to/dir/containing/libfdb_c.dylib
./run.sh                       # results/<UTC stamp>/results.jsonl and env.txt
python3 summarize.py results/<run>/results.jsonl
```

Requires Rust 1.85 or later (edition 2024). Licence: MIT for the code, CC BY 4.0
for the results and this text (see the repository root).
