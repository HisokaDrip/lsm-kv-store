import os
import random

from lsmkv.bloom import BloomFilter
from lsmkv.wal import DEL, PUT, WAL


def test_bloom_no_false_negatives_and_low_fp():
    bf = BloomFilter(5000, fp_rate=0.01)
    keys = [f"key-{i}".encode() for i in range(5000)]
    for k in keys:
        bf.add(k)
    assert all(k in bf for k in keys)
    fp = sum(f"other-{i}".encode() in bf for i in range(20000)) / 20000
    assert fp < 0.03


def test_bloom_roundtrip():
    bf = BloomFilter(100)
    bf.add(b"x")
    bf2 = BloomFilter.from_bytes(bf.to_bytes())
    assert b"x" in bf2 and bf2.nbits == bf.nbits and bf2.k == bf.k


def test_wal_replay_and_torn_tail(tmp_path):
    p = str(tmp_path / "wal.log")
    w = WAL(p)
    w.append(PUT, b"a", b"1")
    w.append(DEL, b"a")
    w.append(PUT, b"b", b"2" * 100)
    w.close()
    # simulate a crash halfway through the last record
    size = os.path.getsize(p)
    with open(p, "r+b") as f:
        f.truncate(size - 30)
    assert list(WAL.replay(p)) == [(PUT, b"a", b"1"), (DEL, b"a", b"")]


def test_wal_stops_at_corruption(tmp_path):
    p = str(tmp_path / "wal.log")
    w = WAL(p)
    for i in range(10):
        w.append(PUT, b"k%d" % i, os.urandom(8))
    w.close()
    data = bytearray(open(p, "rb").read())
    data[len(data) // 2] ^= 0xFF
    open(p, "wb").write(data)
    n = len(list(WAL.replay(p)))
    assert 0 < n < 10
