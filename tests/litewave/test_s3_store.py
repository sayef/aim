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
import aim.litewave._config as _config

BUCKET = 'litewave-test'


def _configure(tmp_root):
    """Point the global config at a mock S3 bucket rooted under tmp_root."""
    _config.config.bucket = BUCKET
    _config.config.prefix = 'litewave/'
    _config.config.local_root = tmp_root
    _config.config.flush_interval = 0.2
    _config.config.compact_threshold = 3
    _config.config.region = None
    _config.config.endpoint_url = None


def _open(path, read_only=False):
    return litewave.DB(path, litewave.Options(create_if_missing=True), read_only=read_only)


@mock_aws
class TestS3Store(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        boto3.client('s3').create_bucket(Bucket=BUCKET)
        self._saved = (_config.config.bucket, _config.config.prefix,
                       _config.config.local_root, _config.config.compact_threshold)
        _configure(self.tmp)

    def tearDown(self):
        (_config.config.bucket, _config.config.prefix,
         _config.config.local_root, _config.config.compact_threshold) = self._saved

    def _list_segments(self, db_path):
        root = _config.config.s3_root_for(db_path)
        s3 = boto3.client('s3')
        resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=f'{root}/segments/')
        return [o['Key'] for o in resp.get('Contents', [])]

    def test_flush_creates_segment_and_manifest(self):
        path = os.path.join(self.tmp, '.aim', 'meta', 'chunks', 'run1')
        db = _open(path)
        db.put(b'k1', b'v1')
        db.put(b'k2', b'v2')
        db.flush()

        s3 = boto3.client('s3')
        root = _config.config.s3_root_for(path)
        manifest = s3.get_object(Bucket=BUCKET, Key=f'{root}/manifest.json')['Body'].read()
        self.assertIn(b'segments', manifest)
        self.assertEqual(len(self._list_segments(path)), 1)
        self.assertTrue(root.startswith('litewave/'))
        self.assertIn('meta/chunks/run1', root)
        db.close()

    def test_reopen_merges_from_s3(self):
        path = os.path.join(self.tmp, '.aim', 'meta', 'chunks', 'run2')
        db = _open(path)
        db.put(b'a', b'1')
        db.put(b'b', b'2')
        db.close()

        import shutil
        shutil.rmtree(path)
        db2 = _open(path, read_only=True)
        self.assertEqual(db2.get(b'a'), b'1')
        self.assertEqual(db2.get(b'b'), b'2')
        db2.close()

    def test_delete_propagates(self):
        path = os.path.join(self.tmp, '.aim', 'meta', 'chunks', 'run3')
        db = _open(path)
        db.put(b'x', b'1')
        db.put(b'y', b'2')
        db.delete(b'x')
        db.close()

        import shutil
        shutil.rmtree(path)
        db2 = _open(path, read_only=True)
        self.assertIsNone(db2.get(b'x'))
        self.assertEqual(db2.get(b'y'), b'2')
        db2.close()

    def test_delete_range_propagates(self):
        path = os.path.join(self.tmp, '.aim', 'meta', 'chunks', 'run4')
        db = _open(path)
        for k in (b'a1', b'a2', b'a3', b'b1'):
            db.put(k, k)
        batch = litewave.WriteBatch()
        batch.delete_range(b'a', b'b')
        db.write(batch)
        db.close()

        import shutil
        shutil.rmtree(path)
        db2 = _open(path, read_only=True)
        self.assertIsNone(db2.get(b'a1'))
        self.assertIsNone(db2.get(b'a3'))
        self.assertEqual(db2.get(b'b1'), b'b1')
        db2.close()

    def test_two_writers_converge(self):
        shared_root = 'litewave/shared/run'
        orig = _config.config.s3_root_for
        _config.config.s3_root_for = lambda db_path: shared_root
        try:
            p1 = os.path.join(self.tmp, 'cacheA')
            p2 = os.path.join(self.tmp, 'cacheB')
            w1 = _open(p1)
            w2 = _open(p2)
            w1.put(b'from_w1', b'1')
            w1.flush()
            w2.put(b'from_w2', b'2')
            w2.flush()
            s3 = boto3.client('s3')
            segs = s3.list_objects_v2(Bucket=BUCKET, Prefix=f'{shared_root}/segments/')
            self.assertEqual(len(segs.get('Contents', [])), 2)
            w1.close()
            w2.close()

            reader = _open(os.path.join(self.tmp, 'cacheR'), read_only=True)
            self.assertEqual(reader.get(b'from_w1'), b'1')
            self.assertEqual(reader.get(b'from_w2'), b'2')
            reader.close()
        finally:
            _config.config.s3_root_for = orig

    def test_compaction_reduces_segment_count(self):
        path = os.path.join(self.tmp, '.aim', 'meta', 'chunks', 'run5')
        db = _open(path)
        for i in range(6):
            db.put(f'k{i}'.encode(), f'v{i}'.encode())
            db.flush()
        self.assertGreater(len(self._list_segments(path)), 3)

        db.compact_range()
        self.assertLessEqual(len(self._list_segments(path)), 1)

        import shutil
        db.close()
        shutil.rmtree(path)
        db2 = _open(path, read_only=True)
        for i in range(6):
            self.assertEqual(db2.get(f'k{i}'.encode()), f'v{i}'.encode())
        db2.close()


if __name__ == '__main__':
    unittest.main()
