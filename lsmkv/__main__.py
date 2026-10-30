"""python -m lsmkv <dir> [put k v | get k | del k | scan [start] [end] | stats | compact | bench N]"""
import os
import random
import sys
import time

from . import DB


def bench(path, n):
    rng = random.Random(1)
    keys = [b"user:%08d" % rng.randrange(n * 4) for _ in range(n)]
    with DB(path, memtable_limit=256 * 1024) as db:
        t0 = time.perf_counter()
        for i, k in enumerate(keys):
            db.put(k, os.urandom(64))
        t_write = time.perf_counter() - t0
        db.flush()
        print(f"writes   {n:>8,}  {n / t_write:>10,.0f} ops/s")

        sample = rng.sample(keys, min(n, 5000))
        t0 = time.perf_counter()
        hits = sum(db.get(k) is not None for k in sample)
        t_hit = time.perf_counter() - t0
        print(f"hit gets {len(sample):>8,}  {len(sample) / t_hit:>10,.0f} ops/s  ({hits} found)")

        misses = [b"nobody:%08d" % i for i in range(5000)]
        t0 = time.perf_counter()
        for k in misses:
            db.get(k)
        t_miss = time.perf_counter() - t0
        print(f"miss gets {len(misses):>7,}  {len(misses) / t_miss:>10,.0f} ops/s  (bloom filters doing the work)")

        t0 = time.perf_counter()
        rows = sum(1 for _ in db.scan(b"user:00010000", b"user:00020000"))
        print(f"range scan          {rows:>6,} rows in {(time.perf_counter() - t0) * 1000:.1f} ms")
        print(db.stats())


def main(argv):
    if len(argv) < 2:
        print(__doc__)
        return 1
    path, cmd, *rest = argv[0], argv[1], *argv[2:]
    if cmd == "bench":
        bench(path, int(rest[0]) if rest else 100_000)
        return 0
    with DB(path) as db:
        if cmd == "put":
            db.put(rest[0], rest[1])
        elif cmd == "get":
            v = db.get(rest[0])
            print("(not found)" if v is None else v.decode(errors="replace"))
        elif cmd == "del":
            db.delete(rest[0])
        elif cmd == "scan":
            for k, v in db.scan(*(rest[:2])):
                print(f"{k.decode(errors='replace')}\t{v.decode(errors='replace')}")
        elif cmd == "stats":
            print(db.stats())
        elif cmd == "compact":
            db.compact()
            print(db.stats())
        else:
            print(f"unknown command {cmd!r}")
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
