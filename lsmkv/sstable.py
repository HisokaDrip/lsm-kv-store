"""Immutable sorted string tables.

Layout on disk:

    [records...][sparse index][bloom][footer]

records are klen:u32 vlen:u32 key value, sorted by key. vlen == TOMBSTONE marks
a delete. The sparse index keeps every INDEX_EVERY-th key with its offset, so a
lookup is a bisect in memory plus one short sequential read.
"""
import bisect
import os
import struct

from .bloom import BloomFilter

TOMBSTONE = 0xFFFFFFFF
INDEX_EVERY = 16
MAGIC = b"LSMKV01\x00"
_REC = struct.Struct("<II")
_FOOTER = struct.Struct("<QQQ8s")


def write_sstable(path, items):
    """items: iterable of (key, value_or_None) already sorted and unique."""
    items = list(items)
    bloom = BloomFilter(len(items))
    index = []
    tmp = path + ".tmp"
    with open(tmp, "wb") as f:
        for i, (k, v) in enumerate(items):
            if i % INDEX_EVERY == 0:
                index.append((k, f.tell()))
            bloom.add(k)
            if v is None:
                f.write(_REC.pack(len(k), TOMBSTONE) + k)
            else:
                f.write(_REC.pack(len(k), len(v)) + k + v)

        index_off = f.tell()
        for k, off in index:
            f.write(struct.pack("<IQ", len(k), off) + k)
        bloom_off = f.tell()
        f.write(bloom.to_bytes())
        f.write(_FOOTER.pack(index_off, bloom_off, len(items), MAGIC))
        f.flush()
        os.fsync(f.fileno())
    # rename is atomic, so readers never see a half-written table
    os.replace(tmp, path)
    return SSTable(path)


class SSTable:
    def __init__(self, path):
        self.path = path
        with open(path, "rb") as f:
            f.seek(-_FOOTER.size, os.SEEK_END)
            footer_pos = f.tell()
            index_off, bloom_off, self.count, magic = _FOOTER.unpack(f.read(_FOOTER.size))
            if magic != MAGIC:
                raise ValueError(f"{path}: not an sstable (bad magic)")
            f.seek(index_off)
            raw_index = f.read(bloom_off - index_off)
            self.bloom = BloomFilter.from_bytes(f.read(footer_pos - bloom_off))
        self.data_end = index_off
        self.index_keys, self.index_offs = [], []
        pos = 0
        while pos < len(raw_index):
            klen, off = struct.unpack_from("<IQ", raw_index, pos)
            pos += 12
            self.index_keys.append(raw_index[pos:pos + klen])
            self.index_offs.append(off)
            pos += klen
        self.size = os.path.getsize(path)

    @property
    def min_key(self):
        return self.index_keys[0] if self.index_keys else None

    def _read_block(self, i):
        start = self.index_offs[i]
        end = self.index_offs[i + 1] if i + 1 < len(self.index_offs) else self.data_end
        with open(self.path, "rb") as f:
            f.seek(start)
            return f.read(end - start)

    @staticmethod
    def _decode(buf):
        pos = 0
        while pos < len(buf):
            klen, vlen = _REC.unpack_from(buf, pos)
            pos += _REC.size
            k = buf[pos:pos + klen]
            pos += klen
            if vlen == TOMBSTONE:
                yield k, None
            else:
                yield k, buf[pos:pos + vlen]
                pos += vlen

    def get(self, key):
        """Returns (found, value). value None + found True means tombstone."""
        if not self.index_keys or key not in self.bloom:
            return False, None
        i = bisect.bisect_right(self.index_keys, key) - 1
        if i < 0:
            return False, None
        for k, v in self._decode(self._read_block(i)):
            if k == key:
                return True, v
            if k > key:
                break
        return False, None

    def scan(self, start=None, end=None):
        """Yield (key, value) with start <= key < end, tombstones included."""
        if not self.index_keys:
            return
        i = 0 if start is None else max(0, bisect.bisect_right(self.index_keys, start) - 1)
        for b in range(i, len(self.index_offs)):
            if end is not None and self.index_keys[b] >= end:
                return
            for k, v in self._decode(self._read_block(b)):
                if start is not None and k < start:
                    continue
                if end is not None and k >= end:
                    return
                yield k, v
