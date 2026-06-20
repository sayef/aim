"""
S3 sync backend for litewave: immutable per-writer segments + a CAS-guarded manifest.

Design (multi-writer-safe without locking on the hot path):

* Each flush serialises the not-yet-flushed local writes into a NEW immutable
  segment object ``<root>/segments/<writer_id>-<n>.seg`` -- it is never
  overwritten, so concurrent writers never collide on data objects.
* The only shared mutable object is the tiny ``<root>/manifest.json`` which
  lists the live segments. It is updated with S3 conditional writes
  (``If-Match`` / ``If-None-Match``) so concurrent updates serialise via
  compare-and-swap; the loser re-reads and retries.
* On open we download the segments not yet merged into the local view and apply
  them in manifest-sequence order (newest wins).
* Compaction merges many segments into one and CAS-replaces the manifest.

Segments use a compact length-prefixed binary format (no temp files).
"""

import json
import logging
import struct
import threading
import time
from typing import List, Optional, Tuple

from . import errors
from ._config import S3Config
from ._local import LocalStore

logger = logging.getLogger(__name__)

_SEG_MAGIC = b'ARSEG\x01'


def serialize_segment(
    rows: List[Tuple[bytes, Optional[bytes], int]],
    ranges: List[Tuple[bytes, bytes]],
) -> bytes:
    """Pack (key, value, deleted) rows and range tombstones into one blob."""
    out = bytearray(_SEG_MAGIC)
    out += struct.pack('<I', len(rows))
    for key, value, deleted in rows:
        value = value or b''
        out += struct.pack('<BI', 1 if deleted else 0, len(key))
        out += key
        out += struct.pack('<I', len(value))
        out += value
    out += struct.pack('<I', len(ranges))
    for begin_key, end_key in ranges:
        out += struct.pack('<I', len(begin_key))
        out += begin_key
        out += struct.pack('<I', len(end_key))
        out += end_key
    return bytes(out)


def deserialize_segment(
    blob: bytes,
) -> Tuple[List[Tuple[bytes, Optional[bytes], int]], List[Tuple[bytes, bytes]]]:
    try:
        if blob[: len(_SEG_MAGIC)] != _SEG_MAGIC:
            raise ValueError('bad segment magic')
        pos = len(_SEG_MAGIC)

        def take(n: int) -> bytes:
            nonlocal pos
            chunk = blob[pos:pos + n]
            if len(chunk) != n:
                raise ValueError('truncated segment')
            pos += n
            return chunk

        (n_rows,) = struct.unpack('<I', take(4))
        rows: List[Tuple[bytes, Optional[bytes], int]] = []
        for _ in range(n_rows):
            deleted, klen = struct.unpack('<BI', take(5))
            key = take(klen)
            (vlen,) = struct.unpack('<I', take(4))
            value = take(vlen)
            rows.append((key, None if deleted else value, deleted))

        (n_ranges,) = struct.unpack('<I', take(4))
        ranges: List[Tuple[bytes, bytes]] = []
        for _ in range(n_ranges):
            (blen,) = struct.unpack('<I', take(4))
            begin_key = take(blen)
            (elen,) = struct.unpack('<I', take(4))
            end_key = take(elen)
            ranges.append((begin_key, end_key))

        return rows, ranges
    except (ValueError, struct.error) as exc:
        raise errors.Corruption(f'corrupt segment: {exc}') from exc


class S3SyncBackend:
    """Drives synchronisation of one LocalStore with its S3 tree."""

    def __init__(self, store: LocalStore, db_path: str, config: S3Config, read_only: bool = False):
        self._store = store
        self._config = config
        self._read_only = read_only

        try:
            import boto3  # local import: only needed when S3 is enabled
        except ImportError as exc:  # pragma: no cover
            raise errors.StoreIOError(
                'LITEWAVE_S3_BUCKET is set but boto3 is not installed'
            ) from exc

        self._s3 = boto3.client('s3', **config.boto_client_kwargs())
        self._bucket = config.bucket

        root = config.s3_root_for(db_path)
        self._manifest_key = f'{root}/manifest.json'
        self._segments_prefix = f'{root}/segments/'

        self._flush_lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

        # Initial merge from S3 into the local view.
        self.pull()

        if not self._read_only:
            self._thread = threading.Thread(
                target=self._loop, name='litewave-s3-flush', daemon=True
            )
            self._thread.start()

    # ----- manifest helpers ---------------------------------------------

    def _get_manifest(self) -> Tuple[Optional[dict], Optional[str]]:
        """Return (manifest dict or None, etag or None)."""
        try:
            resp = self._s3.get_object(Bucket=self._bucket, Key=self._manifest_key)
            body = resp['Body'].read()
            return json.loads(body.decode('utf-8')), resp.get('ETag')
        except self._s3.exceptions.NoSuchKey:
            return None, None
        except self._s3.exceptions.ClientError as exc:
            code = exc.response.get('Error', {}).get('Code')
            if code in ('NoSuchKey', '404', 'NoSuchBucket'):
                return None, None
            raise errors.StoreIOError(f'failed to read manifest: {exc}') from exc

    def _put_manifest_cas(self, manifest: dict, etag: Optional[str]) -> bool:
        """Conditionally write the manifest. Returns False on CAS conflict."""
        body = json.dumps(manifest).encode('utf-8')
        kwargs = dict(
            Bucket=self._bucket,
            Key=self._manifest_key,
            Body=body,
            ContentType='application/json',
        )
        if etag is None:
            kwargs['IfNoneMatch'] = '*'
        else:
            kwargs['IfMatch'] = etag
        try:
            self._s3.put_object(**kwargs)
            return True
        except self._s3.exceptions.ClientError as exc:
            code = exc.response.get('Error', {}).get('Code')
            status = exc.response.get('ResponseMetadata', {}).get('HTTPStatusCode')
            if code in ('PreconditionFailed', 'ConditionalRequestConflict') or status == 412:
                return False
            raise errors.StoreIOError(f'failed to write manifest: {exc}') from exc

    # ----- read path -----------------------------------------------------

    def pull(self):
        """Download and merge any segments not yet present in the local view."""
        manifest, _ = self._get_manifest()
        if not manifest:
            return
        segments = sorted(manifest.get('segments', []), key=lambda s: s['seq'])
        for seg in segments:
            name = seg['name']
            if self._store.is_merged(name):
                continue
            blob = self._download_segment(name)
            if blob is None:
                continue
            rows, ranges = deserialize_segment(blob)
            self._store.apply_segment(rows, ranges, seg['seq'])
            self._store.record_merged(name)

    def _download_segment(self, name: str) -> Optional[bytes]:
        key = f'{self._segments_prefix}{name}'
        try:
            resp = self._s3.get_object(Bucket=self._bucket, Key=key)
            return resp['Body'].read()
        except self._s3.exceptions.NoSuchKey:
            # Likely compacted away by another writer; a fresh pull will reconcile.
            logger.debug('segment %s missing (compacted?)', name)
            return None
        except self._s3.exceptions.ClientError as exc:
            raise errors.StoreIOError(f'failed to download segment {name}: {exc}') from exc

    # ----- write path ----------------------------------------------------

    def _next_segment_name(self) -> str:
        counter = int(self._store._meta_get('seg_counter', '0'))
        counter += 1
        self._store._meta_set('seg_counter', str(counter))
        return f'{self._store.writer_id}-{counter}.seg'

    def flush(self):
        """Serialise dirty local writes into a new segment and register it."""
        if self._read_only:
            return
        with self._flush_lock:
            rows, ranges = self._store.collect_dirty()
            if not rows and not ranges:
                return

            try:
                name = self._next_segment_name()
                blob = serialize_segment(rows, ranges)
                self._upload_segment(name, blob)
                manifest_seq = self._append_to_manifest(name)
            except Exception:
                # Re-arm the same writes for the next attempt.
                self._store.abort_flush()
                raise

            # Our own data is already in the local view; mark it flushed/merged.
            self._store.clear_dirty(manifest_seq)
            self._store.record_merged(name)

    def _upload_segment(self, name: str, blob: bytes):
        key = f'{self._segments_prefix}{name}'
        try:
            self._s3.put_object(
                Bucket=self._bucket, Key=key, Body=blob,
                ContentType='application/octet-stream',
            )
        except self._s3.exceptions.ClientError as exc:
            raise errors.StoreIOError(f'failed to upload segment {name}: {exc}') from exc

    def _append_to_manifest(self, name: str) -> int:
        """CAS-append a segment entry; returns the assigned manifest seq."""
        for _ in range(50):
            manifest, etag = self._get_manifest()
            if manifest is None:
                manifest = {'version': 1, 'next_seq': 0, 'segments': []}
            seq = manifest['next_seq']
            # Merge any newly-discovered segments from other writers first.
            self._merge_unseen(manifest)
            manifest['segments'].append(
                {'name': name, 'seq': seq, 'writer': self._store.writer_id}
            )
            manifest['next_seq'] = seq + 1
            if self._put_manifest_cas(manifest, etag):
                return seq
            time.sleep(0.02)
        raise errors.StoreIOError('manifest CAS failed after repeated retries')

    def _merge_unseen(self, manifest: dict):
        for seg in sorted(manifest.get('segments', []), key=lambda s: s['seq']):
            if self._store.is_merged(seg['name']):
                continue
            blob = self._download_segment(seg['name'])
            if blob is None:
                continue
            rows, ranges = deserialize_segment(blob)
            self._store.apply_segment(rows, ranges, seg['seq'])
            self._store.record_merged(seg['name'])

    # ----- compaction ----------------------------------------------------

    def compact(self):
        """Merge all live segments into one and CAS-replace the manifest."""
        if self._read_only:
            return
        with self._flush_lock:
            manifest, etag = self._get_manifest()
            if not manifest or len(manifest.get('segments', [])) <= self._config.compact_threshold:
                return

            segments = sorted(manifest['segments'], key=lambda s: s['seq'])
            net: dict = {}
            for seg in segments:
                blob = self._download_segment(seg['name'])
                if blob is None:
                    continue
                rows, ranges = deserialize_segment(blob)
                for begin_key, end_key in ranges:
                    for k in list(net.keys()):
                        if begin_key <= k < end_key:
                            net[k] = (None, 1)
                for key, value, deleted in rows:
                    net[key] = (value, deleted)

            # Drop tombstoned keys: a fresh base needs no tombstones.
            live_rows = [(k, v, 0) for k, (v, d) in net.items() if not d]

            new_name = self._next_segment_name()
            self._upload_segment(new_name, serialize_segment(live_rows, []))

            new_seq = manifest['next_seq']
            new_manifest = {
                'version': 1,
                'next_seq': new_seq + 1,
                'segments': [{'name': new_name, 'seq': new_seq, 'writer': self._store.writer_id}],
            }
            if not self._put_manifest_cas(new_manifest, etag):
                # Lost the race; drop our orphan and let the next cycle retry.
                self._delete_segment(new_name)
                return

            self._store.record_merged(new_name)
            for seg in segments:
                self._delete_segment(seg['name'])

    def _delete_segment(self, name: str):
        try:
            self._s3.delete_object(Bucket=self._bucket, Key=f'{self._segments_prefix}{name}')
        except Exception as exc:  # best-effort cleanup
            logger.debug('failed to delete old segment %s: %s', name, exc)

    # ----- background loop / lifecycle -----------------------------------

    def _loop(self):
        interval = max(0.5, self._config.flush_interval)
        while not self._stop.wait(interval):
            try:
                self.flush()
                self.compact()
            except Exception as exc:  # never kill the daemon thread
                logger.warning('litewave background sync error: %s', exc)

    def close(self):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=10)
            self._thread = None
        if not self._read_only:
            # Best-effort: close() is also reachable via __del__, so never raise.
            try:
                self.flush()
            except Exception as exc:
                logger.warning('final flush on close failed: %s', exc)
            try:
                self.compact()
            except Exception as exc:
                logger.debug('compaction on close failed: %s', exc)
