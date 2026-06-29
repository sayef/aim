"""S3-backed engine tests using moto to mock S3.

Covers: flush -> segment + manifest, reopen-and-merge, delete / delete_range
tombstone propagation, two concurrent writers converging via the manifest, and
compaction reducing the live segment count.
"""

import os
import tempfile
import unittest

os.environ.setdefault('AWS_ACCESS_KEY_ID', 'testing')
os.environ.setdefault('AWS_SECRET_ACCESS_KEY', 'testing')
os.environ.setdefault('AWS_SECURITY_TOKEN', 'testing')
os.environ.setdefault('AWS_SESSION_TOKEN', 'testing')
os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

import boto3
import pytest

moto = pytest.importorskip('moto')
from moto import mock_aws

import aim.litewave as litewave
from aim.litewave import S3Config

BUCKET = 'litewave-test'


def _cfg(tmp_root):
    """An S3Config pointing at the mock bucket, rooted under tmp_root."""
    return S3Config(
        bucket=BUCKET,
        prefix='litewave/',
        local_root=tmp_root,
        flush_interval=0.2,
        compact_threshold=3,
    )


@mock_aws
class TestS3Store(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        boto3.client('s3').create_bucket(Bucket=BUCKET)
        self.config = _cfg(self.tmp)

    def _open(self, path, read_only=False, config=None):
        return litewave.DB(
            path,
            litewave.Options(create_if_missing=True),
            read_only=read_only,
            config=config if config is not None else self.config,
        )

    def _list_segments(self, db_path):
        root = self.config.s3_root_for(db_path)
        s3 = boto3.client('s3')
        resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=f'{root}/segments/')
        return [o['Key'] for o in resp.get('Contents', [])]

    def test_flush_creates_segment_and_manifest(self):
        path = os.path.join(self.tmp, '.aim', 'meta', 'chunks', 'run1')
        db = self._open(path)
        db.put(b'k1', b'v1')
        db.put(b'k2', b'v2')
        db.flush()

        s3 = boto3.client('s3')
        root = self.config.s3_root_for(path)
        manifest = s3.get_object(Bucket=BUCKET, Key=f'{root}/manifest.json')['Body'].read()
        self.assertIn(b'segments', manifest)
        self.assertEqual(len(self._list_segments(path)), 1)
        self.assertTrue(root.startswith('litewave/'))
        self.assertIn('meta/chunks/run1', root)
        db.close()

    def test_reopen_merges_from_s3(self):
        path = os.path.join(self.tmp, '.aim', 'meta', 'chunks', 'run2')
        db = self._open(path)
        db.put(b'a', b'1')
        db.put(b'b', b'2')
        db.close()

        import shutil
        shutil.rmtree(path)
        db2 = self._open(path, read_only=True)
        self.assertEqual(db2.get(b'a'), b'1')
        self.assertEqual(db2.get(b'b'), b'2')
        db2.close()

    def test_delete_propagates(self):
        path = os.path.join(self.tmp, '.aim', 'meta', 'chunks', 'run3')
        db = self._open(path)
        db.put(b'x', b'1')
        db.put(b'y', b'2')
        db.delete(b'x')
        db.close()

        import shutil
        shutil.rmtree(path)
        db2 = self._open(path, read_only=True)
        self.assertIsNone(db2.get(b'x'))
        self.assertEqual(db2.get(b'y'), b'2')
        db2.close()

    def test_delete_range_propagates(self):
        path = os.path.join(self.tmp, '.aim', 'meta', 'chunks', 'run4')
        db = self._open(path)
        for k in (b'a1', b'a2', b'a3', b'b1'):
            db.put(k, k)
        batch = litewave.WriteBatch()
        batch.delete_range(b'a', b'b')
        db.write(batch)
        db.close()

        import shutil
        shutil.rmtree(path)
        db2 = self._open(path, read_only=True)
        self.assertIsNone(db2.get(b'a1'))
        self.assertIsNone(db2.get(b'a3'))
        self.assertEqual(db2.get(b'b1'), b'b1')
        db2.close()

    def test_two_writers_converge(self):
        # Two independent writers (distinct local caches) pointed at the SAME
        # logical S3 store. Per-DB config: give each a config whose root maps to
        # the same shared location.
        shared_root = 'litewave/shared/run'

        def fixed_root_config():
            cfg = _cfg(self.tmp)
            cfg.s3_root_for = lambda db_path: shared_root
            return cfg

        p1 = os.path.join(self.tmp, 'cacheA')
        p2 = os.path.join(self.tmp, 'cacheB')
        w1 = self._open(p1, config=fixed_root_config())
        w2 = self._open(p2, config=fixed_root_config())
        w1.put(b'from_w1', b'1')
        w1.flush()
        w2.put(b'from_w2', b'2')
        w2.flush()
        s3 = boto3.client('s3')
        segs = s3.list_objects_v2(Bucket=BUCKET, Prefix=f'{shared_root}/segments/')
        self.assertEqual(len(segs.get('Contents', [])), 2)
        w1.close()
        w2.close()

        reader = self._open(
            os.path.join(self.tmp, 'cacheR'), read_only=True, config=fixed_root_config()
        )
        self.assertEqual(reader.get(b'from_w1'), b'1')
        self.assertEqual(reader.get(b'from_w2'), b'2')
        reader.close()

    def test_compaction_reduces_segment_count(self):
        path = os.path.join(self.tmp, '.aim', 'meta', 'chunks', 'run5')
        db = self._open(path)
        for i in range(6):
            db.put(f'k{i}'.encode(), f'v{i}'.encode())
            db.flush()
        self.assertGreater(len(self._list_segments(path)), 3)

        db.compact_range()
        self.assertLessEqual(len(self._list_segments(path)), 1)

        import shutil
        db.close()
        shutil.rmtree(path)
        db2 = self._open(path, read_only=True)
        for i in range(6):
            self.assertEqual(db2.get(f'k{i}'.encode()), f'v{i}'.encode())
        db2.close()


if __name__ == '__main__':
    unittest.main()