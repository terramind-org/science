//! swarm-dbbench: measures candidate metadata databases on swarm's three hottest workloads.
//!
//! - `h1`: landing on one hot ref. `naive` runs one transaction per land, the way a plain
//!   implementation would; `coalesced` sends every land to one owner task that commits whatever is
//!   queued as a single transaction (SPEC: the land coalescer).
//! - `h2`: insert-or-ignore ingest of content-addressed object rows, with a share of duplicates.
//! - `h3`: point reads of object rows by id.
//!
//! Every run prints one JSON line. Workload data comes from a seeded generator, so runs repeat.

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand, ValueEnum};
use hdrhistogram::Histogram;
use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};

#[derive(Parser)]
struct Cli {
    /// Database under test.
    #[arg(long, value_enum)]
    db: Db,
    /// Postgres-style connection string (pg, yb), file path (sqlite) or cluster file (fdb).
    #[arg(long)]
    url: String,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Clone, Copy, ValueEnum, PartialEq)]
enum Db {
    Pg,
    Yb,
    Sqlite,
    Fdb,
}

impl Db {
    fn name(self) -> &'static str {
        match self {
            Db::Pg => "postgres",
            Db::Yb => "yugabytedb",
            Db::Sqlite => "sqlite",
            Db::Fdb => "foundationdb",
        }
    }
}

#[derive(Clone, Copy, ValueEnum, PartialEq)]
enum Mode {
    Naive,
    Coalesced,
}

#[derive(Subcommand)]
enum Cmd {
    /// Create (or reset) the schema.
    Setup,
    /// Hot-ref landing.
    H1 {
        #[arg(long, value_enum)]
        mode: Mode,
        #[arg(long)]
        landers: usize,
        #[arg(long, default_value_t = 10)]
        secs: u64,
        /// Largest number of lands the coalescer commits in one transaction.
        #[arg(long, default_value_t = 1024)]
        max_batch: usize,
    },
    /// Insert-or-ignore ingest.
    H2 {
        #[arg(long)]
        writers: usize,
        #[arg(long)]
        rows: u64,
        #[arg(long, default_value_t = 1000)]
        batch: usize,
        /// Fraction of rows in each batch that repeat an id this writer already sent.
        #[arg(long, default_value_t = 0.3)]
        dup: f64,
    },
    /// Point reads of ids written by a previous `h2` with the same --writers/--rows/--dup.
    H3 {
        #[arg(long)]
        readers: usize,
        #[arg(long, default_value_t = 15)]
        secs: u64,
        #[arg(long)]
        writers: usize,
        #[arg(long)]
        rows: u64,
        #[arg(long, default_value_t = 0.3)]
        dup: f64,
    },
}

// ---------- deterministic data ----------

/// SplitMix64: small, fast, seeded. Good enough for workload data, never used for ids we trust.
#[derive(Clone)]
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed ^ 0x9E37_79B9_7F4A_7C15)
    }
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
    fn chance(&mut self, p: f64) -> bool {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64 <= p
    }
}

/// The j-th new object id written by writer w: 34 bytes shaped like a swarm object id
/// (algorithm byte, kind byte, 32-byte digest).
fn object_id(w: u64, j: u64) -> Vec<u8> {
    let mut r = Rng::new(w.wrapping_mul(1_000_003) ^ j.wrapping_mul(0x1000_0001));
    let mut id = Vec::with_capacity(34);
    id.push(0x01);
    id.push(0x01);
    for _ in 0..4 {
        id.extend_from_slice(&r.next().to_be_bytes());
    }
    id
}

/// Per-process nonce mixed into hot-ref change ids, so repeated runs against one database never
/// reuse an id (each run's lands are new work). Ingest and read data stay fully deterministic.
fn run_nonce() -> u64 {
    use std::sync::OnceLock;
    static N: OnceLock<u64> = OnceLock::new();
    *N.get_or_init(|| std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0))
}

fn change_id(r: &mut Rng) -> Vec<u8> {
    let mut v = r.next().to_be_bytes().to_vec();
    v.extend_from_slice(&r.next().to_be_bytes());
    v
}

/// Generates writer w's batches: new ids are object_id(w, 0..), duplicates repeat an earlier one.
struct BatchGen {
    w: u64,
    next_new: u64,
    rng: Rng,
    dup: f64,
}

impl BatchGen {
    fn new(w: u64, dup: f64) -> Self {
        BatchGen { w, next_new: 0, rng: Rng::new(0xD00D ^ w), dup }
    }
    fn batch(&mut self, n: usize) -> (Vec<Vec<u8>>, Vec<i64>) {
        let mut ids = Vec::with_capacity(n);
        let mut sizes = Vec::with_capacity(n);
        for _ in 0..n {
            let j = if self.next_new > 0 && self.rng.chance(self.dup) {
                self.rng.below(self.next_new)
            } else {
                self.next_new += 1;
                self.next_new - 1
            };
            ids.push(object_id(self.w, j));
            sizes.push((self.rng.below(1 << 20) + 1) as i64);
        }
        (ids, sizes)
    }
}

// ---------- results ----------

fn hist() -> Histogram<u64> {
    Histogram::new_with_bounds(1, 600_000_000, 3).expect("histogram bounds")
}

fn report(db: Db, workload: &str, params: serde_json::Value, ops: u64, secs: f64, h: &Histogram<u64>, extra: serde_json::Value) {
    let us = |q: f64| h.value_at_quantile(q) as f64 / 1000.0;
    let mut out = json!({
        "db": db.name(),
        "workload": workload,
        "params": params,
        "ops": ops,
        "secs": (secs * 1000.0).round() / 1000.0,
        "ops_per_sec": ((ops as f64 / secs) * 10.0).round() / 10.0,
        "latency_ms": {
            "p50": us(0.50), "p90": us(0.90), "p99": us(0.99), "p999": us(0.999),
            "max": h.max() as f64 / 1000.0, "mean": (h.mean() / 10.0).round() / 100.0
        }
    });
    if let (Some(o), Some(e)) = (out.as_object_mut(), extra.as_object()) {
        for (k, v) in e {
            o.insert(k.clone(), v.clone());
        }
    }
    println!("{out}");
}

// ---------- SQL (Postgres and YugabyteDB) ----------

async fn pg_connect(url: &str) -> Result<tokio_postgres::Client> {
    // Hundreds of simultaneous connects overflow the OS listen backlog (128 on macOS) and some get
    // reset; retry those with backoff. Connecting is never inside a measured window.
    let mut wait = 10u64;
    for attempt in 0.. {
        match tokio_postgres::connect(url, tokio_postgres::NoTls).await {
            Ok((client, conn)) => {
                tokio::spawn(async move {
                    let _ = conn.await;
                });
                return Ok(client);
            }
            Err(e) if attempt < 30 => {
                let _ = e;
                tokio::time::sleep(Duration::from_millis(wait)).await;
                wait = (wait * 2).min(1000);
            }
            Err(e) => return Err(anyhow::Error::from(e)).with_context(|| format!("connect {url}")),
        }
    }
    unreachable!()
}

fn pg_retryable(e: &tokio_postgres::Error) -> bool {
    use tokio_postgres::error::SqlState;
    match e.code() {
        Some(c) => *c == SqlState::T_R_SERIALIZATION_FAILURE || *c == SqlState::T_R_DEADLOCK_DETECTED,
        None => false,
    }
}

async fn pg_setup(db: Db, url: &str) -> Result<()> {
    let c = pg_connect(url).await?;
    let objects_pk = if db == Db::Yb { "PRIMARY KEY ((org, id) HASH)" } else { "PRIMARY KEY (org, id)" };
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS refs, reflog, landed, objects;
         CREATE TABLE refs (repo BIGINT, name TEXT, target BYTEA NOT NULL, version BIGINT NOT NULL, PRIMARY KEY (repo, name));
         CREATE TABLE reflog (repo BIGINT, name TEXT, version BIGINT, target BYTEA NOT NULL, PRIMARY KEY (repo, name, version));
         CREATE TABLE landed (repo BIGINT, name TEXT, change_id BYTEA, result BYTEA NOT NULL, PRIMARY KEY (repo, name, change_id));
         CREATE TABLE objects (org BIGINT, id BYTEA, size BIGINT NOT NULL, {objects_pk});
         INSERT INTO refs VALUES (1, 'main', '\\x00', 1);"
    ))
    .await?;
    Ok(())
}

async fn pg_h1_naive(url: String, landers: usize, secs: u64) -> Result<(u64, Histogram<u64>, u64)> {
    // Every lander connects first; the measured window starts when the last one is connected.
    let ready = Arc::new(tokio::sync::Barrier::new(landers));
    let start = Arc::new(std::sync::OnceLock::<Instant>::new());
    let retries = Arc::new(AtomicU64::new(0));
    let mut tasks = Vec::new();
    for l in 0..landers {
        let url = url.clone();
        let retries = retries.clone();
        let ready = ready.clone();
        let start = start.clone();
        tasks.push(tokio::spawn(async move {
            let mut client = pg_connect(&url).await?;
            ready.wait().await;
            let deadline = *start.get_or_init(Instant::now) + Duration::from_secs(secs);
            let mut h = hist();
            let mut rng = Rng::new(0x1A2B ^ l as u64 ^ run_nonce());
            let mut n = 0u64;
            while Instant::now() < deadline {
                let cid = change_id(&mut rng);
                let t0 = Instant::now();
                let mut backoff = 1u64;
                loop {
                    let res: Result<(), tokio_postgres::Error> = async {
                        let tx = client.transaction().await?;
                        let row = tx.query_one("SELECT version FROM refs WHERE repo = 1 AND name = 'main' FOR UPDATE", &[]).await?;
                        let v: i64 = row.get(0);
                        tx.execute("UPDATE refs SET version = $1, target = $2 WHERE repo = 1 AND name = 'main'", &[&(v + 1), &cid]).await?;
                        tx.execute("INSERT INTO reflog (repo, name, version, target) VALUES (1, 'main', $1, $2)", &[&(v + 1), &cid]).await?;
                        tx.execute("INSERT INTO landed (repo, name, change_id, result) VALUES (1, 'main', $1, $2)", &[&cid, &cid]).await?;
                        tx.commit().await
                    }
                    .await;
                    match res {
                        Ok(()) => break,
                        Err(e) if pg_retryable(&e) => {
                            retries.fetch_add(1, Ordering::Relaxed);
                            tokio::time::sleep(Duration::from_millis(rng.below(backoff) + 1)).await;
                            backoff = (backoff * 2).min(64);
                        }
                        Err(e) => return Err(anyhow::Error::from(e)),
                    }
                }
                h.record(t0.elapsed().as_micros() as u64)?;
                n += 1;
            }
            anyhow::Ok((n, h))
        }));
    }
    let mut total = 0;
    let mut h = hist();
    for t in tasks {
        let (n, th) = t.await??;
        total += n;
        h.add(&th)?;
    }
    Ok((total, h, retries.load(Ordering::Relaxed)))
}

type Req = (Vec<u8>, oneshot::Sender<()>);

/// Runs `landers` tasks that each submit a land and wait for it, against a coalescer fed by `rx`.
async fn drive_coalesced(landers: usize, secs: u64, tx: mpsc::Sender<Req>) -> Result<(u64, Histogram<u64>)> {
    let deadline = Instant::now() + Duration::from_secs(secs);
    let mut tasks = Vec::new();
    for l in 0..landers {
        let tx = tx.clone();
        tasks.push(tokio::spawn(async move {
            let mut h = hist();
            let mut rng = Rng::new(0x5EED ^ l as u64 ^ run_nonce().rotate_left(17));
            let mut n = 0u64;
            while Instant::now() < deadline {
                let (done_tx, done_rx) = oneshot::channel();
                let t0 = Instant::now();
                if tx.send((change_id(&mut rng), done_tx)).await.is_err() {
                    break;
                }
                if done_rx.await.is_err() {
                    break;
                }
                h.record(t0.elapsed().as_micros() as u64)?;
                n += 1;
            }
            anyhow::Ok((n, h))
        }));
    }
    drop(tx);
    let mut total = 0;
    let mut h = hist();
    for t in tasks {
        let (n, th) = t.await??;
        total += n;
        h.add(&th)?;
    }
    Ok((total, h))
}

async fn take_batch(rx: &mut mpsc::Receiver<Req>, max: usize) -> Option<Vec<Req>> {
    let first = rx.recv().await?;
    let mut batch = vec![first];
    while batch.len() < max {
        match rx.try_recv() {
            Ok(r) => batch.push(r),
            Err(_) => break,
        }
    }
    Some(batch)
}

async fn pg_coalescer(url: String, mut rx: mpsc::Receiver<Req>, max: usize, stats: Arc<(AtomicU64, AtomicU64)>) -> Result<()> {
    let mut client = pg_connect(&url).await?;
    while let Some(batch) = take_batch(&mut rx, max).await {
        let k = batch.len() as i64;
        let cids: Vec<Vec<u8>> = batch.iter().map(|(c, _)| c.clone()).collect();
        loop {
            let res: Result<(), tokio_postgres::Error> = async {
                let tx = client.transaction().await?;
                let row = tx.query_one("SELECT version FROM refs WHERE repo = 1 AND name = 'main' FOR UPDATE", &[]).await?;
                let v: i64 = row.get(0);
                let versions: Vec<i64> = (1..=k).map(|i| v + i).collect();
                tx.execute("UPDATE refs SET version = $1, target = $2 WHERE repo = 1 AND name = 'main'", &[&(v + k), &cids[cids.len() - 1]]).await?;
                tx.execute("INSERT INTO reflog (repo, name, version, target) SELECT 1, 'main', * FROM unnest($1::BIGINT[], $2::BYTEA[])", &[&versions, &cids]).await?;
                tx.execute("INSERT INTO landed (repo, name, change_id, result) SELECT 1, 'main', c, c FROM unnest($1::BYTEA[]) AS c", &[&cids]).await?;
                tx.commit().await
            }
            .await;
            match res {
                Ok(()) => break,
                Err(e) if pg_retryable(&e) => continue,
                Err(e) => bail!(e),
            }
        }
        stats.0.fetch_add(1, Ordering::Relaxed);
        stats.1.fetch_add(batch.len() as u64, Ordering::Relaxed);
        for (_, done) in batch {
            let _ = done.send(());
        }
    }
    Ok(())
}

async fn pg_h2(url: String, writers: usize, rows: u64, batch: usize, dup: f64) -> Result<(u64, Histogram<u64>, f64)> {
    let per_writer = rows / writers as u64;
    let start = Instant::now();
    let mut tasks = Vec::new();
    for w in 0..writers {
        let url = url.clone();
        tasks.push(tokio::spawn(async move {
            let client = pg_connect(&url).await?;
            let stmt = client
                .prepare("INSERT INTO objects (org, id, size) SELECT 1, * FROM unnest($1::BYTEA[], $2::BIGINT[]) ON CONFLICT DO NOTHING")
                .await?;
            let mut g = BatchGen::new(w as u64, dup);
            let mut h = hist();
            let mut sent = 0u64;
            while sent < per_writer {
                let (ids, sizes) = g.batch(batch);
                let t0 = Instant::now();
                client.execute(&stmt, &[&ids, &sizes]).await?;
                h.record(t0.elapsed().as_micros() as u64)?;
                sent += batch as u64;
            }
            anyhow::Ok((sent, h))
        }));
    }
    let mut total = 0;
    let mut h = hist();
    for t in tasks {
        let (n, th) = t.await??;
        total += n;
        h.add(&th)?;
    }
    Ok((total, h, start.elapsed().as_secs_f64()))
}

fn read_target(rng: &mut Rng, writers: usize, rows: u64, dup: f64) -> Vec<u8> {
    let per_writer_new = ((rows / writers as u64) as f64 * (1.0 - dup) * 0.9) as u64;
    let w = rng.below(writers as u64);
    object_id(w, rng.below(per_writer_new))
}

async fn pg_h3(url: String, readers: usize, secs: u64, writers: usize, rows: u64, dup: f64) -> Result<(u64, Histogram<u64>, u64)> {
    let deadline = Instant::now() + Duration::from_secs(secs);
    let misses = Arc::new(AtomicU64::new(0));
    let mut tasks = Vec::new();
    for r in 0..readers {
        let url = url.clone();
        let misses = misses.clone();
        tasks.push(tokio::spawn(async move {
            let client = pg_connect(&url).await?;
            let stmt = client.prepare("SELECT size FROM objects WHERE org = 1 AND id = $1").await?;
            let mut rng = Rng::new(0xBEEF ^ r as u64);
            let mut h = hist();
            let mut n = 0u64;
            while Instant::now() < deadline {
                let id = read_target(&mut rng, writers, rows, dup);
                let t0 = Instant::now();
                if client.query_opt(&stmt, &[&id]).await?.is_none() {
                    misses.fetch_add(1, Ordering::Relaxed);
                }
                h.record(t0.elapsed().as_micros() as u64)?;
                n += 1;
            }
            anyhow::Ok((n, h))
        }));
    }
    let mut total = 0;
    let mut h = hist();
    for t in tasks {
        let (n, th) = t.await??;
        total += n;
        h.add(&th)?;
    }
    Ok((total, h, misses.load(Ordering::Relaxed)))
}

// ---------- SQLite ----------

fn sqlite_open(path: &str) -> Result<rusqlite::Connection> {
    let c = rusqlite::Connection::open(path)?;
    c.busy_timeout(Duration::from_secs(120))?;
    // fullfsync: on macOS a plain fsync does not flush the drive cache; F_FULLFSYNC does.
    c.execute_batch("PRAGMA journal_mode = WAL; PRAGMA synchronous = FULL; PRAGMA fullfsync = ON; PRAGMA checkpoint_fullfsync = ON;")?;
    Ok(c)
}

fn sqlite_setup(path: &str) -> Result<()> {
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{path}{suffix}"));
    }
    let c = sqlite_open(path)?;
    c.execute_batch(
        "CREATE TABLE refs (repo INTEGER, name TEXT, target BLOB NOT NULL, version INTEGER NOT NULL, PRIMARY KEY (repo, name));
         CREATE TABLE reflog (repo INTEGER, name TEXT, version INTEGER, target BLOB NOT NULL, PRIMARY KEY (repo, name, version));
         CREATE TABLE landed (repo INTEGER, name TEXT, change_id BLOB, result BLOB NOT NULL, PRIMARY KEY (repo, name, change_id));
         CREATE TABLE objects (org INTEGER, id BLOB, size INTEGER NOT NULL, PRIMARY KEY (org, id)) WITHOUT ROWID;
         INSERT INTO refs VALUES (1, 'main', x'00', 1);",
    )?;
    Ok(())
}

fn sqlite_land(c: &mut rusqlite::Connection, cids: &[Vec<u8>]) -> Result<()> {
    let tx = c.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    let v: i64 = tx.query_row("SELECT version FROM refs WHERE repo = 1 AND name = 'main'", [], |r| r.get(0))?;
    let k = cids.len() as i64;
    tx.execute("UPDATE refs SET version = ?1, target = ?2 WHERE repo = 1 AND name = 'main'", rusqlite::params![v + k, cids[cids.len() - 1]])?;
    {
        let mut log = tx.prepare_cached("INSERT INTO reflog (repo, name, version, target) VALUES (1, 'main', ?1, ?2)")?;
        let mut landed = tx.prepare_cached("INSERT INTO landed (repo, name, change_id, result) VALUES (1, 'main', ?1, ?1)")?;
        for (i, cid) in cids.iter().enumerate() {
            log.execute(rusqlite::params![v + 1 + i as i64, cid])?;
            landed.execute(rusqlite::params![cid])?;
        }
    }
    tx.commit()?;
    Ok(())
}

fn sqlite_h1_naive(path: String, landers: usize, secs: u64) -> Result<(u64, Histogram<u64>)> {
    let deadline = Instant::now() + Duration::from_secs(secs);
    let mut threads = Vec::new();
    for l in 0..landers {
        let path = path.clone();
        threads.push(std::thread::spawn(move || -> Result<(u64, Histogram<u64>)> {
            let mut c = sqlite_open(&path)?;
            let mut rng = Rng::new(0x1A2B ^ l as u64 ^ run_nonce());
            let mut h = hist();
            let mut n = 0;
            while Instant::now() < deadline {
                let cid = change_id(&mut rng);
                let t0 = Instant::now();
                sqlite_land(&mut c, std::slice::from_ref(&cid))?;
                h.record(t0.elapsed().as_micros() as u64)?;
                n += 1;
            }
            Ok((n, h))
        }));
    }
    let mut total = 0;
    let mut h = hist();
    for t in threads {
        let (n, th) = t.join().map_err(|_| anyhow::anyhow!("lander panicked"))??;
        total += n;
        h.add(&th)?;
    }
    Ok((total, h))
}

fn sqlite_coalescer(path: String, mut rx: mpsc::Receiver<Req>, max: usize, stats: Arc<(AtomicU64, AtomicU64)>) -> Result<()> {
    let mut c = sqlite_open(&path)?;
    while let Some(first) = rx.blocking_recv() {
        let mut batch = vec![first];
        while batch.len() < max {
            match rx.try_recv() {
                Ok(r) => batch.push(r),
                Err(_) => break,
            }
        }
        let cids: Vec<Vec<u8>> = batch.iter().map(|(c, _)| c.clone()).collect();
        sqlite_land(&mut c, &cids)?;
        stats.0.fetch_add(1, Ordering::Relaxed);
        stats.1.fetch_add(batch.len() as u64, Ordering::Relaxed);
        for (_, done) in batch {
            let _ = done.send(());
        }
    }
    Ok(())
}

fn sqlite_h2(path: String, writers: usize, rows: u64, batch: usize, dup: f64) -> Result<(u64, Histogram<u64>, f64)> {
    let per_writer = rows / writers as u64;
    let start = Instant::now();
    let mut threads = Vec::new();
    for w in 0..writers {
        let path = path.clone();
        threads.push(std::thread::spawn(move || -> Result<(u64, Histogram<u64>)> {
            let mut c = sqlite_open(&path)?;
            let mut g = BatchGen::new(w as u64, dup);
            let mut h = hist();
            let mut sent = 0;
            while sent < per_writer {
                let (ids, sizes) = g.batch(batch);
                let t0 = Instant::now();
                let tx = c.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                {
                    let mut ins = tx.prepare_cached("INSERT OR IGNORE INTO objects (org, id, size) VALUES (1, ?1, ?2)")?;
                    for (id, size) in ids.iter().zip(&sizes) {
                        ins.execute(rusqlite::params![id, size])?;
                    }
                }
                tx.commit()?;
                h.record(t0.elapsed().as_micros() as u64)?;
                sent += batch as u64;
            }
            Ok((sent, h))
        }));
    }
    let mut total = 0;
    let mut h = hist();
    for t in threads {
        let (n, th) = t.join().map_err(|_| anyhow::anyhow!("writer panicked"))??;
        total += n;
        h.add(&th)?;
    }
    Ok((total, h, start.elapsed().as_secs_f64()))
}

fn sqlite_h3(path: String, readers: usize, secs: u64, writers: usize, rows: u64, dup: f64) -> Result<(u64, Histogram<u64>, u64)> {
    let deadline = Instant::now() + Duration::from_secs(secs);
    let misses = Arc::new(AtomicU64::new(0));
    let mut threads = Vec::new();
    for r in 0..readers {
        let path = path.clone();
        let misses = misses.clone();
        threads.push(std::thread::spawn(move || -> Result<(u64, Histogram<u64>)> {
            let c = sqlite_open(&path)?;
            let mut q = c.prepare("SELECT size FROM objects WHERE org = 1 AND id = ?1")?;
            let mut rng = Rng::new(0xBEEF ^ r as u64);
            let mut h = hist();
            let mut n = 0;
            while Instant::now() < deadline {
                let id = read_target(&mut rng, writers, rows, dup);
                let t0 = Instant::now();
                let found: Option<i64> = q.query_row(rusqlite::params![id], |row| row.get(0)).ok();
                if found.is_none() {
                    misses.fetch_add(1, Ordering::Relaxed);
                }
                h.record(t0.elapsed().as_micros() as u64)?;
                n += 1;
            }
            Ok((n, h))
        }));
    }
    let mut total = 0;
    let mut h = hist();
    for t in threads {
        let (n, th) = t.join().map_err(|_| anyhow::anyhow!("reader panicked"))??;
        total += n;
        h.add(&th)?;
    }
    Ok((total, h, misses.load(Ordering::Relaxed)))
}

// ---------- FoundationDB ----------

const REF_KEY: &[u8] = b"r\x00main";

fn reflog_key(v: i64) -> Vec<u8> {
    let mut k = b"l\x00main\x00".to_vec();
    k.extend_from_slice(&v.to_be_bytes());
    k
}

fn landed_key(cid: &[u8]) -> Vec<u8> {
    let mut k = b"d\x00main\x00".to_vec();
    k.extend_from_slice(cid);
    k
}

fn object_key(id: &[u8]) -> Vec<u8> {
    let mut k = b"o".to_vec();
    k.extend_from_slice(&1u64.to_be_bytes());
    k.extend_from_slice(id);
    k
}

fn fdb_version(v: Option<foundationdb::future::FdbSlice>) -> i64 {
    v.map(|s| i64::from_be_bytes(s.as_ref().try_into().unwrap_or([0; 8]))).unwrap_or(1)
}

async fn fdb_setup(db: &foundationdb::Database) -> Result<()> {
    db.run(|trx, _| async move {
        trx.clear_range(b"", b"\xff");
        trx.set(REF_KEY, &1i64.to_be_bytes());
        Ok(())
    })
    .await?;
    Ok(())
}

/// One land transaction for a batch of change ids: read the ref and each landed key (the
/// uniqueness check), then write version + k, k ref-log entries and k landed entries.
async fn fdb_land(db: &foundationdb::Database, cids: Arc<Vec<Vec<u8>>>, attempts: Arc<AtomicU64>) -> Result<()> {
    db.run(|trx, _| {
        let cids = cids.clone();
        let attempts = attempts.clone();
        async move {
            attempts.fetch_add(1, Ordering::Relaxed);
            // Issue the ref read and every landed-key read at once; FoundationDB pipelines them.
            let keys: Vec<Vec<u8>> = cids.iter().map(|c| landed_key(c)).collect();
            let (cur, landed) = futures::future::try_join(
                trx.get(REF_KEY, false),
                futures::future::try_join_all(keys.iter().map(|k| trx.get(k, false))),
            )
            .await?;
            let v = fdb_version(cur);
            if landed.iter().any(|x| x.is_some()) {
                return Ok(());
            }
            let k = cids.len() as i64;
            trx.set(REF_KEY, &(v + k).to_be_bytes());
            for (i, cid) in cids.iter().enumerate() {
                trx.set(&reflog_key(v + 1 + i as i64), cid);
                trx.set(&landed_key(cid), cid);
            }
            Ok(())
        }
    })
    .await?;
    Ok(())
}

async fn fdb_h1_naive(db: Arc<foundationdb::Database>, landers: usize, secs: u64) -> Result<(u64, Histogram<u64>, u64)> {
    let deadline = Instant::now() + Duration::from_secs(secs);
    let attempts = Arc::new(AtomicU64::new(0));
    let mut tasks = Vec::new();
    for l in 0..landers {
        let db = db.clone();
        let attempts = attempts.clone();
        tasks.push(tokio::spawn(async move {
            let mut rng = Rng::new(0x1A2B ^ l as u64 ^ run_nonce());
            let mut h = hist();
            let mut n = 0u64;
            while Instant::now() < deadline {
                let cids = Arc::new(vec![change_id(&mut rng)]);
                let t0 = Instant::now();
                fdb_land(&db, cids, attempts.clone()).await?;
                h.record(t0.elapsed().as_micros() as u64)?;
                n += 1;
            }
            anyhow::Ok((n, h))
        }));
    }
    let mut total = 0;
    let mut h = hist();
    for t in tasks {
        let (n, th) = t.await??;
        total += n;
        h.add(&th)?;
    }
    let retries = attempts.load(Ordering::Relaxed).saturating_sub(total);
    Ok((total, h, retries))
}

async fn fdb_coalescer(db: Arc<foundationdb::Database>, mut rx: mpsc::Receiver<Req>, max: usize, stats: Arc<(AtomicU64, AtomicU64)>) -> Result<()> {
    let attempts = Arc::new(AtomicU64::new(0));
    while let Some(batch) = take_batch(&mut rx, max).await {
        let cids = Arc::new(batch.iter().map(|(c, _)| c.clone()).collect::<Vec<_>>());
        fdb_land(&db, cids, attempts.clone()).await?;
        stats.0.fetch_add(1, Ordering::Relaxed);
        stats.1.fetch_add(batch.len() as u64, Ordering::Relaxed);
        for (_, done) in batch {
            let _ = done.send(());
        }
    }
    Ok(())
}

async fn fdb_h2(db: Arc<foundationdb::Database>, writers: usize, rows: u64, batch: usize, dup: f64) -> Result<(u64, Histogram<u64>, f64)> {
    let per_writer = rows / writers as u64;
    let start = Instant::now();
    let mut tasks = Vec::new();
    for w in 0..writers {
        let db = db.clone();
        tasks.push(tokio::spawn(async move {
            let mut g = BatchGen::new(w as u64, dup);
            let mut h = hist();
            let mut sent = 0u64;
            while sent < per_writer {
                let (ids, sizes) = g.batch(batch);
                let rows = Arc::new(ids.into_iter().zip(sizes).collect::<Vec<_>>());
                let t0 = Instant::now();
                // Content-addressed rows are idempotent: a blind write of the same key and value is
                // FoundationDB's insert-or-ignore, and it never conflicts.
                db.run(|trx, _| {
                    let rows = rows.clone();
                    async move {
                        for (id, size) in rows.iter() {
                            trx.set(&object_key(id), &size.to_be_bytes());
                        }
                        Ok(())
                    }
                })
                .await?;
                h.record(t0.elapsed().as_micros() as u64)?;
                sent += batch as u64;
            }
            anyhow::Ok((sent, h))
        }));
    }
    let mut total = 0;
    let mut h = hist();
    for t in tasks {
        let (n, th) = t.await??;
        total += n;
        h.add(&th)?;
    }
    Ok((total, h, start.elapsed().as_secs_f64()))
}

async fn fdb_h3(db: Arc<foundationdb::Database>, readers: usize, secs: u64, writers: usize, rows: u64, dup: f64) -> Result<(u64, Histogram<u64>, u64)> {
    let deadline = Instant::now() + Duration::from_secs(secs);
    let misses = Arc::new(AtomicU64::new(0));
    let mut tasks = Vec::new();
    for r in 0..readers {
        let db = db.clone();
        let misses = misses.clone();
        tasks.push(tokio::spawn(async move {
            let mut rng = Rng::new(0xBEEF ^ r as u64);
            let mut h = hist();
            let mut n = 0u64;
            while Instant::now() < deadline {
                let key = object_key(&read_target(&mut rng, writers, rows, dup));
                let t0 = Instant::now();
                let trx = db.create_trx()?;
                // Snapshot read: immutable rows need no conflict tracking.
                if trx.get(&key, true).await?.is_none() {
                    misses.fetch_add(1, Ordering::Relaxed);
                }
                h.record(t0.elapsed().as_micros() as u64)?;
                n += 1;
            }
            anyhow::Ok((n, h))
        }));
    }
    let mut total = 0;
    let mut h = hist();
    for t in tasks {
        let (n, th) = t.await??;
        total += n;
        h.add(&th)?;
    }
    Ok((total, h, misses.load(Ordering::Relaxed)))
}

// ---------- main ----------

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let db = cli.db;
    // FoundationDB's network thread must start once per process and stop at exit.
    let _fdb_net = if db == Db::Fdb { Some(unsafe { foundationdb::boot() }) } else { None };
    let fdb = if db == Db::Fdb { Some(Arc::new(foundationdb::Database::new(Some(&cli.url))?)) } else { None };
    let url = cli.url.clone();

    match cli.cmd {
        Cmd::Setup => match db {
            Db::Pg | Db::Yb => pg_setup(db, &url).await?,
            Db::Sqlite => sqlite_setup(&url)?,
            Db::Fdb => fdb_setup(fdb.as_ref().unwrap()).await?,
        },
        Cmd::H1 { mode, landers, secs, max_batch } => {
            let params = json!({"mode": if mode == Mode::Naive {"naive"} else {"coalesced"}, "landers": landers, "max_batch": max_batch});
            // Throughput is over the fixed measured window (connection setup is outside it).
            match mode {
                Mode::Naive => {
                    let (n, h, retries) = match db {
                        Db::Pg | Db::Yb => pg_h1_naive(url, landers, secs).await?,
                        Db::Sqlite => {
                            let (n, h) = tokio::task::spawn_blocking(move || sqlite_h1_naive(url, landers, secs)).await??;
                            (n, h, 0)
                        }
                        Db::Fdb => fdb_h1_naive(fdb.unwrap(), landers, secs).await?,
                    };
                    report(db, "h1_hot_ref", params, n, secs as f64, &h, json!({"retries": retries}));
                }
                Mode::Coalesced => {
                    let (tx, rx) = mpsc::channel::<Req>(landers.max(1) * 2);
                    let stats = Arc::new((AtomicU64::new(0), AtomicU64::new(0)));
                    let st = stats.clone();
                    let coalescer = match db {
                        Db::Pg | Db::Yb => tokio::spawn(pg_coalescer(url, rx, max_batch, st)),
                        Db::Sqlite => tokio::task::spawn_blocking(move || sqlite_coalescer(url, rx, max_batch, st)),
                        Db::Fdb => tokio::spawn(fdb_coalescer(fdb.unwrap(), rx, max_batch, st)),
                    };
                    let (n, h) = drive_coalesced(landers, secs, tx).await?;
                    coalescer.await??;
                    let txns = stats.0.load(Ordering::Relaxed);
                    let lands = stats.1.load(Ordering::Relaxed);
                    let avg = if txns > 0 { lands as f64 / txns as f64 } else { 0.0 };
                    report(db, "h1_hot_ref", params, n, secs as f64, &h,
                        json!({"transactions": txns, "avg_batch": (avg * 10.0).round() / 10.0}));
                }
            }
        }
        Cmd::H2 { writers, rows, batch, dup } => {
            let params = json!({"writers": writers, "rows": rows, "batch": batch, "dup": dup});
            let (n, h, secs) = match db {
                Db::Pg | Db::Yb => pg_h2(url, writers, rows, batch, dup).await?,
                Db::Sqlite => tokio::task::spawn_blocking(move || sqlite_h2(url, writers, rows, batch, dup)).await??,
                Db::Fdb => fdb_h2(fdb.unwrap(), writers, rows, batch, dup).await?,
            };
            // ops = rows offered (duplicates included); per-batch latency.
            report(db, "h2_ingest", params, n, secs, &h, json!({"latency_unit": "per batch"}));
        }
        Cmd::H3 { readers, secs, writers, rows, dup } => {
            let params = json!({"readers": readers, "writers": writers, "rows": rows});
            let t0 = Instant::now();
            let (n, h, misses) = match db {
                Db::Pg | Db::Yb => pg_h3(url, readers, secs, writers, rows, dup).await?,
                Db::Sqlite => tokio::task::spawn_blocking(move || sqlite_h3(url, readers, secs, writers, rows, dup)).await??,
                Db::Fdb => fdb_h3(fdb.unwrap(), readers, secs, writers, rows, dup).await?,
            };
            report(db, "h3_point_read", params, n, t0.elapsed().as_secs_f64(), &h, json!({"misses": misses}));
        }
    }
    Ok(())
}
