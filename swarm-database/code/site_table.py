#!/usr/bin/env python3
"""Writes the TypeScript table data used on https://terramind.org/agent-scale-database/ from
results/final/summary.json. Usage: python3 site_table.py ../results/final/summary.json > results.ts"""
import json, sys

s = json.load(open(sys.argv[1]))
names = {"postgres": "Postgres", "yugabytedb": "YugabyteDB", "sqlite": "SQLite", "foundationdb": "FoundationDB"}
order = ["postgres", "yugabytedb", "sqlite", "foundationdb"]
levels = ["1", "16", "64", "256", "1000"]
r = lambda x: round(x) if x >= 100 else round(x, 1)
out = ["// Generated from github.com/terramind-org/science/blob/main/swarm-database/results/final/summary.json",
       "// by swarm-database/code/site_table.py. Do not edit by hand.", "",
       "export const LEVELS = [1, 16, 64, 256, 1000] as const;", "",
       "export type Cell = { perSec: number; p99Ms: number } | null;", "",
       "export const HOT_REF: { db: string; mode: 'one transaction per landing' | 'batched by one owner'; cells: Cell[] }[] = ["]
for db in order:
    for mode, label in (("naive", "one transaction per landing"), ("coalesced", "batched by one owner")):
        d = s["h1"].get(f"{db}/{mode}", {})
        cells = ["null" if l not in d else f"{{ perSec: {r(d[l]['ops_per_sec'])}, p99Ms: {r(d[l]['p99'])} }}" for l in levels]
        out.append(f"  {{ db: '{names[db]}', mode: '{label}', cells: [{', '.join(cells)}] }},")
out += ["];", "", "export const INGEST_READS = ["]
for db in order:
    h2, h3 = s["h2"][db], s["h3"][db]
    out.append(f"  {{ db: '{names[db]}', ingestPerSec: {round(h2['rows_per_sec'])}, readsPerSec: {round(h3['reads_per_sec'])}, "
               f"readP50Ms: {h3['p50_ms']}, readP99Ms: {h3['p99_ms']} }},")
out += ["];", ""]
print("\n".join(out))
