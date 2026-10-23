# FoundationDB 7.3.77, binaries unpacked from FoundationDB-7.3.77_arm64.pkg with `pkgutil --expand-full`.
# The disk was over 95% full, below FoundationDB's default 5% free-space floor, hence the two knobs.
echo "bench:bench@127.0.0.1:4500" > fdb.cluster
# Hot-ref runs (20261018T001641Z-fdb-h1-pipelined):
fdbserver -p 127.0.0.1:4500 -C fdb.cluster -d data -L logs --memory 3GiB --cache-memory 1GiB \
  --knob_min_available_space_ratio=0.005 --knob_min_available_space=500000000 &
# Ingest and read runs (20261017T235654Z-final-fdb) used: --memory 6GiB \
#   --knob_min_available_space_ratio=0.01 --knob_min_available_space=1000000000
fdbcli -C fdb.cluster --exec "configure new single ssd-redwood-1"
