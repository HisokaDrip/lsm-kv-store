# lsm-kv-store

A small log-structured merge-tree key-value store in pure Python, no dependencies. I wanted to actually understand how stuff like LevelDB / RocksDB / Cassandra handle writes, so I built the core pieces myself:

- **write-ahead log** with a CRC32 per record, so a crash mid-write just loses the torn tail instead of corrupting the db
- **memtable** that gets flushed to disk once it passes a size limit
- **SSTables** (sorted, immutable files) with a sparse index and a bloom filter each, so a lookup is a bisect in memory + one small read, and a missing key usually never touches the disk at all
- **compaction** that merges all tables into one and drops deleted keys (tombstones) for good
- **range scans** done as a k-way heap merge across the memtable and every table, newest value wins
- a **manifest** that's swapped atomically, plus cleanup of half-written tables left by a crash

## Running it

```
python -m lsmkv ./data put fruit:apple red
python -m lsmkv ./data put fruit:kiwi green
python -m lsmkv ./data del fruit:kiwi
python -m lsmkv ./data scan fruit: fruit:~
python -m lsmkv ./data stats
python -m lsmkv /tmp/bench bench 100000
```

Or from code:

```python
from lsmkv import DB

with DB("./data") as db:
    db.put("user:1", "mazy")
    db.get("user:1")            # b'mazy'
    list(db.scan("user:", "user:~"))
```

## Benchmark

On my box (single thread, 64-byte values, fsync off):

```
writes    100,000      48,637 ops/s
hit gets    5,000      78,884 ops/s  (5000 found)
miss gets   5,000     263,553 ops/s  (bloom filters doing the work)
range scan           2,215 rows in 2.1 ms
```

Misses being ~3x faster than hits is the bloom filters paying off, those lookups never open a file.

## Tests

```
pip install pytest
python -m pytest
```

The main one is a randomized test that throws 5000 puts/deletes at the db (with tiny memtables so it flushes and compacts constantly, and reopens it every 1000 ops) and checks every key and range scan against a plain dict. There are also tests for torn WAL writes and bit-flip corruption.

## What I'd do next

- leveled compaction instead of "merge everything", so big dbs don't rewrite the whole thing
- block compression and caching open file handles
- a background compaction thread instead of compacting inline on flush
