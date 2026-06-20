"""Core key-value / iterator contract tests for litewave.

Heavyweight storage-engine features (merge operators, column families, snapshots,
custom comparators / prefix extractors, statistics properties, reverse iteration
and low-level tuning options) are intentionally NOT part of this engine and are
not tested here.
"""

import os
import gc
import shutil
import unittest
import tempfile

import aim.litewave as litewave


def int_to_bytes(ob):
    return str(ob).encode('ascii')


class TestHelper(unittest.TestCase):
    def setUp(self):
        self.db_loc = tempfile.mkdtemp()
        self.addCleanup(self._close_db)

    def _close_db(self):
        del self.db
        gc.collect()
        if os.path.exists(self.db_loc):
            shutil.rmtree(self.db_loc)


class TestDB(TestHelper):
    def setUp(self):
        TestHelper.setUp(self)
        opts = litewave.Options(create_if_missing=True)
        self.db = litewave.DB(os.path.join(self.db_loc, "test"), opts)

    def test_unicode_path(self):
        name = os.path.join(self.db_loc, b'M\xc3\xbcnchen'.decode('utf8'))
        litewave.DB(name, litewave.Options(create_if_missing=True))
        self.assertTrue(os.path.isdir(name))

    def test_get_none(self):
        self.assertIsNone(self.db.get(b'xxx'))

    def test_put_get(self):
        self.db.put(b"a", b"b")
        self.assertEqual(b"b", self.db.get(b"a"))

    def test_get_by_keyword(self):
        # aim calls db.get(key=...)
        self.db.put(b"a", b"b")
        self.assertEqual(b"b", self.db.get(key=b"a"))

    def test_multi_get(self):
        self.db.put(b"a", b"1")
        self.db.put(b"b", b"2")
        self.db.put(b"c", b"3")
        ret = self.db.multi_get([b'a', b'b', b'c'])
        self.assertEqual({b'a': b'1', b'b': b'2', b'c': b'3'}, ret)

    def test_delete(self):
        self.db.put(b"a", b"b")
        self.assertEqual(b"b", self.db.get(b"a"))
        self.db.delete(b"a")
        self.assertIsNone(self.db.get(b"a"))

    def test_delete_range(self):
        for k in (b'a1', b'a2', b'a3', b'b1'):
            self.db.put(k, k)
        batch = litewave.WriteBatch()
        batch.delete_range(b'a', b'b')
        self.db.write(batch)
        self.assertIsNone(self.db.get(b'a1'))
        self.assertIsNone(self.db.get(b'a3'))
        self.assertEqual(b'b1', self.db.get(b'b1'))

    def test_write_batch(self):
        batch = litewave.WriteBatch()
        batch.put(b"key", b"v1")
        batch.delete(b"key")
        batch.put(b"key", b"v2")
        self.db.write(batch)
        self.assertEqual(b"v2", self.db.get(b"key"))

    def test_key_may_exists(self):
        self.db.put(b"a", b'1')
        self.assertEqual((False, None), self.db.key_may_exist(b"x"))
        self.assertEqual((False, None), self.db.key_may_exist(b'x', True))
        self.assertEqual((True, None), self.db.key_may_exist(b'a'))
        self.assertEqual((True, b'1'), self.db.key_may_exist(b'a', True))

    def test_seek_for_prev(self):
        for k in (b'a1', b'a3', b'b1', b'b2', b'c2', b'c4'):
            self.db.put(k, k + b'_value')

        it = self.db.iterkeys()
        it.seek(b'a1')
        self.assertEqual(it.get(), b'a1')
        it.seek(b'a3')
        self.assertEqual(it.get(), b'a3')
        it.seek_for_prev(b'c4')
        self.assertEqual(it.get(), b'c4')
        it.seek_for_prev(b'c3')
        self.assertEqual(it.get(), b'c2')

        it = self.db.itervalues()
        it.seek_for_prev(b'c3')
        self.assertEqual(it.get(), b'c2_value')

        it = self.db.iteritems()
        it.seek(b'a3')
        self.assertEqual(it.get(), (b'a3', b'a3_value'))
        it.seek_for_prev(b'c3')
        self.assertEqual(it.get(), (b'c2', b'c2_value'))

    def test_iter_keys(self):
        for x in range(300):
            self.db.put(int_to_bytes(x), int_to_bytes(x))
        it = self.db.iterkeys()
        self.assertEqual([], list(it))

        it.seek_to_last()
        self.assertEqual([b'99'], list(it))

        ref = sorted([int_to_bytes(x) for x in range(300)])
        it.seek_to_first()
        self.assertEqual(ref, list(it))

        it.seek(b'90')
        ref = sorted([int_to_bytes(x) for x in range(300)])
        ref = list(filter(lambda x: x >= b'90', ref))
        self.assertEqual(ref, list(it))

    def test_iter_values(self):
        for x in range(300):
            self.db.put(int_to_bytes(x), int_to_bytes(x * 1000))
        it = self.db.itervalues()
        it.seek_to_first()
        ref = sorted([int_to_bytes(x) for x in range(300)])
        ref = [int_to_bytes(int(x) * 1000) for x in ref]
        self.assertEqual(ref, list(it))

    def test_iter_items(self):
        for x in range(300):
            self.db.put(int_to_bytes(x), int_to_bytes(x * 1000))
        it = self.db.iteritems()
        it.seek_to_first()
        ref = sorted([int_to_bytes(x) for x in range(300)])
        ref = [(x, int_to_bytes(int(x) * 1000)) for x in ref]
        self.assertEqual(ref, list(it))

    def test_compact_range_is_noop_local(self):
        for x in range(50):
            self.db.put(int_to_bytes(x), int_to_bytes(x))
        # Should not raise in local mode.
        self.db.compact_range()
        self.db.flush()
        self.db.flush_wal()
        self.assertEqual(b'10', self.db.get(b'10'))


if __name__ == "__main__":
    unittest.main()
