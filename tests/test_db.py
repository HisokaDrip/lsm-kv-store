import random

import pytest

from lsmkv import DB
from lsmkv.sstable import SSTable, write_sstable


def test_sstable_get_and_scan(tmp_path):
    items = [(b"k%05d" % i, b"v%d" % i) for i in range(1000)]
    items[10] = (items[10][0], None)
    t = write_sstable(str(tmp_path / "t.sst"), items)
    t = SSTable(t.path)  # reopen from disk
    assert t.get(b"k00500") == (True, b"v500")
    assert t.get(b"k00010") == (True, None)
    assert t.get(b"nope") == (False, None)
    got = list(t.scan(b"k00100", b"k00105"))
    assert [k for k, _ in got] == [b"k%05d" % i for i in range(100, 105)]


def test_basic_ops(tmp_path):
    with DB(str(tmp_path)) as db:
        db.put("a", "1")
        db.put("b", "2")
        db.put("a", "3")
        db.delete("b")
        assert db.get("a") == b"3"
        assert db.get("b") is None
        assert db.get("b", b"gone") == b"gone"


def test_recovers_from_wal_without_close(tmp_path):
    db = DB(str(tmp_path))
    db.put("x", "y")
    db.delete("x")
    db.put("z", "1")
    # no close(): pretend the process died
    db2 = DB(str(tmp_path))
    assert db2.get("x") is None and db2.get("z") == b"1"
    assert db2.recovered == 2


def test_against_dict_model_with_flushes_and_compaction(tmp_path):
    rng = random.Random(7)
    model = {}
    db = DB(str(tmp_path), memtable_limit=2048, compact_at=3)
    for step in range(5000):
        k = b"key%03d" % rng.randrange(400)
        if rng.random() < 0.25:
            db.delete(k)
            model.pop(k, None)
        else:
            v = b"%d-%d" % (step, rng.randrange(10**6))
            db.put(k, v)
            model[k] = v
        if step % 1000 == 999:
            db.close()
            db = DB(str(tmp_path), memtable_limit=2048, compact_at=3)
    assert db.stats()["sstables"] >= 1
    for i in range(400):
        k = b"key%03d" % i
        assert db.get(k) == model.get(k), k
    assert list(db.scan()) == sorted(model.items())
    lo, hi = b"key100", b"key200"
    assert list(db.scan(lo, hi)) == sorted((k, v) for k, v in model.items() if lo <= k < hi)
    db.close()


def test_compaction_drops_tombstones_and_old_files(tmp_path):
    db = DB(str(tmp_path), memtable_limit=10**9, compact_at=100)
    for i in range(50):
        db.put(b"k%d" % i, b"v")
    db.flush()
    for i in range(50):
        db.delete(b"k%d" % i)
    db.flush()
    assert db.stats()["sstables"] == 2
    db.compact()
    assert db.stats()["sstables"] == 1
    assert db.stats()["sstable_keys"] == 0
    assert len(list(tmp_path.glob("*.sst"))) == 1
    db.close()


def test_orphan_tables_are_cleaned(tmp_path):
    with DB(str(tmp_path)) as db:
        db.put("a", "b")
    (tmp_path / "999999.sst").write_bytes(b"junk")
    (tmp_path / "000123.sst.tmp").write_bytes(b"junk")
    db = DB(str(tmp_path))
    assert db.get("a") == b"b"
    assert not (tmp_path / "999999.sst").exists()


def test_rejects_non_bytes(tmp_path):
    with DB(str(tmp_path)) as db, pytest.raises(TypeError):
        db.put(1, "x")
