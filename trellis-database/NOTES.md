# Notes from the session (17–18 October 2026)

What happened while running these benchmarks, in order, including what went wrong. Times are UTC.

1. **Docker would not start.** Docker Desktop's processes were running, but `docker info`
   failed with "error during connect" on `~/.docker/run/docker.sock`. `docker desktop restart`
   failed with "Failed to stop Docker Desktop … processes still running". We ran the databases
   natively instead (settings in `settings/`).
2. **The disk was nearly full from the start** (about 40 GB free of 926 GB). FoundationDB refused
   to accept writes until its free-space floor was lowered (`settings/foundationdb.sh`).
   YugabyteDB's default web port 7000 was taken by macOS, so it ran on 17000.
3. **First comparison was unfair.** FoundationDB calls `F_FULLFSYNC` on macOS; Postgres and SQLite
   did not. `code/fsync_probe.py` measures the gap (`results/fsync_probe.txt`). We set Postgres
   to `wal_sync_method = fsync_writethrough` and SQLite to `fullfsync = ON` for every run used.
4. **Run discarded: `discarded-first-attempt`.** Its throughput counted connection setup inside
   the measured window, and 256 simultaneous Postgres connects were partly reset (the macOS listen
   backlog is 128). Fixed with connect retries and a start barrier.
5. **Run discarded: `discarded-concurrent-runs`.** We stopped the first attempt with a pattern that
   missed the script (started as `./run.sh`), so it kept running alongside a new run.
6. **Final run** `20261017T120757Z-final` (12:07 UTC 17 Oct) completed Postgres, YugabyteDB and
   SQLite. A restart of our agent session stopped it during FoundationDB.
7. **FoundationDB rerun** `20261017T235654Z-final-fdb` completed all FoundationDB workloads.
8. **Bug in our FoundationDB code.** Coalesced landings read each landed key sequentially; the
   coalesced result plateaued at 4,882 landings a second at 1,000 landers. Fixed to read
   concurrently.
9. **Out of memory and disk.** Before rerunning, at about 00:05 on 18 Oct, `sysctl vm.swapusage`
   reported `total = 19456.00M used = 17959.81M`, and `df -h` showed 4.4 GiB free. We stopped
   Postgres and YugabyteDB, deleted their data, and started a fresh FoundationDB.
10. **FoundationDB hot-ref rerun** `20261018T001641Z-fdb-h1-pipelined` (00:16 on 18 Oct): coalesced
    reached 21,695 landings a second at 1,000 landers. The machine was quieter than in step 7, so
    the change cannot all be credited to the fix.
