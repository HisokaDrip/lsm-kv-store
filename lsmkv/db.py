import heapq
import json
import os
import threading

from .sstable import SSTable, write_sstable
from .wal import DEL, PUT, WAL


def _merge(sources):
    """K-way merge of sorted (key, value) iterators. sources[0] is the newest;
    when several have the same key the newest one wins."""
    tagged = (((k, age, v) for k, v in src) for age, src in enumerate(sources))
    last = object()
    for k, _age, v in heapq.merge(*tagged, key=lambda t: (t[0], t[1])):
        if k == last:
            continue
        last = k
        yield k, v


class DB:
    def __init__(self, path, memtable_limit=1 << 20, compact_at=4, sync=False):
        self.path = path
        self.memtable_limit = memtable_limit
        self.compact_at = compact_at
        self.sync = sync
        self._lock = threading.RLock()
        os.makedirs(path, exist_ok=True)

        self._manifest = os.path.join(path, "MANIFEST")
        self.tables = []  # newest first
        self._next_id = 1
        if os.path.exists(self._manifest):
            with open(self._manifest) as f:
                m = json.load(f)
            self._next_id = m["next_id"]
            self.tables = [SSTable(os.path.join(path, name)) for name in m["tables"]]
        self._cleanup_orphans()

        self.mem = {}
        self.mem_bytes = 0
        wal_path = os.path.join(path, "wal.log")
        for op, k, v in WAL.replay(wal_path):
            self._apply(k, v if op == PUT else None)
        self.recovered = len(self.mem)
        self.wal = WAL(wal_path, sync=sync)

    # --- public api -----------------------------------------------------

    def put(self, key, value):
        key, value = _b(key), _b(value)
        with self._lock:
            self.wal.append(PUT, key, value)
            self._apply(key, value)
            self._maybe_flush()

    def delete(self, key):
        key = _b(key)
        with self._lock:
            self.wal.append(DEL, key)
            self._apply(key, None)
            self._maybe_flush()

    def get(self, key, default=None):
        key = _b(key)
        with self._lock:
            if key in self.mem:
                v = self.mem[key]
                return default if v is None else v
            for t in self.tables:
                found, v = t.get(key)
                if found:
                    return default if v is None else v
        return default

    def scan(self, start=None, end=None):
        start = _b(start) if start is not None else None
        end = _b(end) if end is not None else None
        with self._lock:
            mem = sorted(
                (k, v) for k, v in self.mem.items()
                if (start is None or k >= start) and (end is None or k < end)
            )
            sources = [iter(mem)] + [t.scan(start, end) for t in self.tables]
            # materialise under the lock; a compaction mid-scan would delete files
            out = [(k, v) for k, v in _merge(sources) if v is not None]
        return iter(out)

    def flush(self):
        with self._lock:
            if not self.mem:
                return
            name = f"{self._next_id:06d}.sst"
            self._next_id += 1
            table = write_sstable(os.path.join(self.path, name), sorted(self.mem.items()))
            self.tables.insert(0, table)
            self._write_manifest()
            # only safe to drop the wal once the manifest points at the new table
            self.wal.close()
            open(self.wal.path, "wb").close()
            self.wal = WAL(self.wal.path, sync=self.sync)
            self.mem.clear()
            self.mem_bytes = 0
            if len(self.tables) >= self.compact_at:
                self.compact()

    def compact(self):
        """Merge every table into one. Since nothing older survives, tombstones
        can be dropped for good here."""
        with self._lock:
            if len(self.tables) < 2:
                return
            old = self.tables
            name = f"{self._next_id:06d}.sst"
            self._next_id += 1
            merged = ((k, v) for k, v in _merge([t.scan() for t in old]) if v is not None)
            table = write_sstable(os.path.join(self.path, name), merged)
            self.tables = [table]
            self._write_manifest()
            for t in old:
                os.remove(t.path)

    def close(self):
        with self._lock:
            self.flush()
            self.wal.close()

    def stats(self):
        with self._lock:
            return {
                "memtable_keys": len(self.mem),
                "memtable_bytes": self.mem_bytes,
                "sstables": len(self.tables),
                "sstable_keys": sum(t.count for t in self.tables),
                "disk_bytes": sum(t.size for t in self.tables),
            }

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # --- internals ------------------------------------------------------

    def _apply(self, key, value):
        old = self.mem.get(key)
        if key in self.mem:
            self.mem_bytes -= len(key) + (len(old) if old else 0)
        self.mem[key] = value
        self.mem_bytes += len(key) + (len(value) if value else 0)

    def _maybe_flush(self):
        if self.mem_bytes >= self.memtable_limit:
            self.flush()

    def _write_manifest(self):
        tmp = self._manifest + ".tmp"
        with open(tmp, "w") as f:
            json.dump({"next_id": self._next_id,
                       "tables": [os.path.basename(t.path) for t in self.tables]}, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self._manifest)

    def _cleanup_orphans(self):
        # leftovers from a crash between writing a table and updating the manifest
        live = {os.path.basename(t.path) for t in self.tables}
        for name in os.listdir(self.path):
            if name.endswith(".sst.tmp") or (name.endswith(".sst") and name not in live):
                os.remove(os.path.join(self.path, name))


def _b(x):
    if isinstance(x, bytes):
        return x
    if isinstance(x, str):
        return x.encode()
    raise TypeError(f"keys and values must be bytes or str, got {type(x).__name__}")
