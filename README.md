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

## How it works

**Writes.** `put` and `delete` first append a record to `wal.log`
(`crc32 | op | klen | vlen | key | value`), then update the memtable, which is just a
Python dict. A delete stores `None` as a tombstone, because the key may still exist in an
older table on disk. `fsync` on every write is off by default (`sync=False`), so a crash can
lose whatever the OS hadn't written to disk yet.

**Flush.** Once the memtable passes `memtable_limit` bytes (1 MiB by default), it's sorted
and written out as a new SSTable. The order matters for crash safety: write the table to a
`.tmp` file, fsync it, rename it into place, atomically swap the `MANIFEST` (the list of live
tables, newest first), and only then truncate the WAL. A crash at any step leaves either the
old state plus the WAL, or the new state (maybe with the old WAL replayed on top, which is harmless). On startup, `.sst` files the manifest doesn't list
get deleted, and the WAL is replayed into the memtable, stopping at the first record whose
CRC doesn't match (that's the torn tail).

**SSTable layout.** `[records][sparse index][bloom filter][footer]`. Records are sorted by
key. Every 16th key goes into the sparse index with its file offset. The bloom filter uses
blake2b double hashing, sized for a 1% false positive rate. The footer points at the index
and the bloom filter.

**Reads.** `get` checks the memtable, then each table from newest to oldest. For each table,
the bloom filter is checked first (if it says no, the file isn't touched), then a bisect on
the in-memory sparse index finds the block of up to 16 records to read and scan. The first
hit wins, and a tombstone counts as "not found".

**Scans and compaction.** A range scan runs `heapq.merge` over the sorted memtable slice
and every table's iterator, tagged by age, so for duplicate keys the newest version comes
first and the older ones are skipped. Compaction uses the same merge across all tables,
writes one new table, and drops tombstones, which is safe because nothing older survives.
It runs inline when there are `compact_at` tables (4 by default) and always merges
everything, so on a large db each compaction rewrites all of it.

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
