#!/usr/bin/env python3
"""Turn a results.jsonl from run.sh into Markdown tables (stdout) and a compact JSON summary.

Usage: python3 summarize.py results/<run>/results.jsonl [summary.json]
"""
import json
import sys
from collections import defaultdict

ORDER = ["postgres", "yugabytedb", "sqlite", "foundationdb"]


def load(path):
    rows = []
    for line in open(path):
        line = line.strip()
        if not line:
            continue
        r = json.loads(line)
        if "error" not in r:
            rows.append(r)
    return rows


def fmt(x):
    if x >= 1000:
        return f"{x:,.0f}"
    if x >= 100:
        return f"{x:.0f}"
    if x >= 10:
        return f"{x:.1f}"
    return f"{x:.2f}"


def main():
    rows = load(sys.argv[1])
    h1 = defaultdict(dict)  # (db, mode) -> landers -> row
    for r in rows:
        if r["workload"] == "h1_hot_ref":
            h1[(r["db"], r["params"]["mode"])][r["params"]["landers"]] = r
    landers = sorted({n for d in h1.values() for n in d})

    print("### Hot ref: lands per second (p99 latency in ms)\n")
    print("| Database | Mode | " + " | ".join(f"{n} landers" for n in landers) + " |")
    print("|---|---|" + "---|" * len(landers))
    for db in ORDER:
        for mode in ("naive", "coalesced"):
            d = h1.get((db, mode))
            if not d:
                continue
            cells = []
            for n in landers:
                r = d.get(n)
                cells.append(f"{fmt(r['ops_per_sec'])} ({fmt(r['latency_ms']['p99'])})" if r else "not run")
            print(f"| {db} | {mode} | " + " | ".join(cells) + " |")

    print("\n### Ingest and point reads\n")
    print("| Database | Ingest rows/s (offered, 30% duplicates) | Ingest batch p99 ms | Point reads/s | Read p50 ms | Read p99 ms | Read misses |")
    print("|---|---|---|---|---|---|---|")
    by = {(r["db"], r["workload"]): r for r in rows if r["workload"] != "h1_hot_ref"}
    for db in ORDER:
        i, rd = by.get((db, "h2_ingest")), by.get((db, "h3_point_read"))
        if not i and not rd:
            continue
        print(
            f"| {db} | {fmt(i['ops_per_sec']) if i else '-'} | {fmt(i['latency_ms']['p99']) if i else '-'} | "
            f"{fmt(rd['ops_per_sec']) if rd else '-'} | {rd['latency_ms']['p50'] if rd else '-'} | "
            f"{rd['latency_ms']['p99'] if rd else '-'} | {rd.get('misses', '-') if rd else '-'} |"
        )

    if len(sys.argv) > 2:
        summary = {
            "h1": {f"{db}/{mode}": {str(n): {"ops_per_sec": r["ops_per_sec"], "p50": r["latency_ms"]["p50"], "p99": r["latency_ms"]["p99"],
                                              "retries": r.get("retries"), "avg_batch": r.get("avg_batch")}
                                     for n, r in sorted(d.items())} for (db, mode), d in h1.items()},
            "h2": {db: {"rows_per_sec": r["ops_per_sec"], "batch_p99_ms": r["latency_ms"]["p99"]} for (db, w), r in by.items() if w == "h2_ingest"},
            "h3": {db: {"reads_per_sec": r["ops_per_sec"], "p50_ms": r["latency_ms"]["p50"], "p99_ms": r["latency_ms"]["p99"], "misses": r.get("misses")}
                   for (db, w), r in by.items() if w == "h3_point_read"},
        }
        json.dump(summary, open(sys.argv[2], "w"), indent=1, sort_keys=True)


if __name__ == "__main__":
    main()
