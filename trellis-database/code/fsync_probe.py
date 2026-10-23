#!/usr/bin/env python3
"""Time fsync() against fcntl(F_FULLFSYNC) on macOS: 300 x 4 KiB writes each, median and p99 in ms."""
import fcntl, os, statistics, time

fd = os.open("fsync_probe.bin", os.O_RDWR | os.O_CREAT, 0o644)

def run(kind, n=300):
    times = []
    for i in range(n):
        os.pwrite(fd, os.urandom(4096), (i % 64) * 4096)
        t = time.perf_counter()
        if kind == "fsync":
            os.fsync(fd)
        else:
            fcntl.fcntl(fd, fcntl.F_FULLFSYNC)
        times.append((time.perf_counter() - t) * 1000)
    times.sort()
    return statistics.median(times), times[int(n * 0.99) - 1]

for kind in ("fsync", "F_FULLFSYNC"):
    median, p99 = run(kind)
    print(f"{kind}: median {median:.3f} ms, p99 {p99:.3f} ms")
os.close(fd)
os.remove("fsync_probe.bin")
