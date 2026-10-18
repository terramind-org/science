#!/usr/bin/env bash
# Runs the full benchmark matrix and writes one JSON line per run to results/<stamp>/results.jsonl.
# Requires the four databases from README.md to be running. Usage: ./run.sh [label]
set -euo pipefail
cd "$(dirname "$0")"
: "${FDB_LIB:?set FDB_LIB to the directory holding libfdb_c.dylib}"
export DYLD_LIBRARY_PATH="$FDB_LIB"
RUSTFLAGS="-L native=$FDB_LIB" cargo build -q --release
BIN=target/release/swarm-dbbench
OUT="results/$(date -u +%Y%m%dT%H%M%SZ)${1:+-$1}"; mkdir -p "$OUT"
url_for() {  # bash 3.2 (macOS) has no associative arrays
  case "$1" in
    pg) echo "${PG_URL:-host=127.0.0.1 port=55432 user=bench dbname=bench}" ;;
    yb) echo "${YB_URL:-host=127.0.0.1 port=5433 user=yugabyte password=yugabyte dbname=yugabyte}" ;;
    sqlite) echo "${SQLITE_PATH:-$HOME/.terramind/scratch/dbbench/sqlite/bench.db}" ;;
    fdb) echo "${FDB_CLUSTER:-$HOME/.terramind/scratch/dbbench/fdb/fdb.cluster}" ;;
  esac
}
DBS="${DBS:-pg yb sqlite fdb}"
LANDERS="${LANDERS:-1 16 64 256 1000}"
H1_SECS="${H1_SECS:-10}"; ROWS="${ROWS:-5000000}"; WRITERS="${WRITERS:-8}"; READERS="${READERS:-32}"
run() { echo "+ $*" >&2; timeout 900 "$BIN" "$@" | tee -a "$OUT/results.jsonl" || echo "{\"error\":\"$*\"}" >> "$OUT/results.jsonl"; }
{
  echo "date_utc: $(date -u +%FT%TZ)"; sysctl -n machdep.cpu.brand_string; echo "cores: $(sysctl -n hw.ncpu) memory_bytes: $(sysctl -n hw.memsize)"
  sw_vers | tr '\n' ' '; echo; echo "matrix: DBS=$DBS LANDERS=$LANDERS H1_SECS=$H1_SECS ROWS=$ROWS WRITERS=$WRITERS READERS=$READERS"
} > "$OUT/env.txt"
for d in $DBS; do
  u="$(url_for "$d")"
  "$BIN" --db "$d" --url "$u" setup
  "$BIN" --db "$d" --url "$u" h1 --mode naive --landers 8 --secs 3 >/dev/null   # warm-up, discarded
  for mode in naive coalesced; do
    for n in $LANDERS; do
      # Naive mode opens one connection per lander; 1,000 server processes would exhaust this
      # machine's memory for Postgres and YugabyteDB, so naive stops at NAIVE_SQL_MAX there.
      if [ "$mode" = naive ] && { [ "$d" = pg ] || [ "$d" = yb ]; } && [ "$n" -gt "${NAIVE_SQL_MAX:-256}" ]; then continue; fi
      run --db "$d" --url "$u" h1 --mode "$mode" --landers "$n" --secs "$H1_SECS"
    done
  done
  run --db "$d" --url "$u" h2 --writers "$WRITERS" --rows "$ROWS"
  run --db "$d" --url "$u" h3 --readers "$READERS" --secs 15 --writers "$WRITERS" --rows "$ROWS"
done
echo "results: $OUT" >&2
