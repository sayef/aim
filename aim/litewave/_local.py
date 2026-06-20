"""
Local SQLite engine backing litewave.

This module is the workhorse: it stores the materialised, merged key-value view
plus any not-yet-flushed (``dirty``) local writes in a single SQLite file, and
provides ordered iterators with this contract:

* ``get()``  -> current ``(key, value)``; raises ``ValueError`` when invalid.
* ``next()`` -> current ``(key, value)`` then advances; returns ``None`` at end.
* ``__next__`` -> ``next()`` raising ``StopIteration`` at end.
* ``seek`` / ``seek_for_prev`` / ``seek_to_first`` / ``seek_to_last`` position
  the cursor in lexicographic (memcmp) key order.

Keys are stored as ``BLOB`` so SQLite orders them by ``memcmp`` -- i.e. plain
byte-wise key ordering.
"""

import os
import sqlite3
import threading
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Sentinel sequence number for locally-written, not-yet-flushed rows. It is
# larger than any manifest sequence so a concurrent writer's merged segment
# never clobbers a pending local write (last-flush-wins is reconciled later).
LOCAL_SEQ = 1 << 62


class LocalStore:
    """A single SQLite-backed key-value store with tombstones and dirty tracking."""

    def __init__(self, db_path: str, read_only: bool = False, create_if_missing: bool = True):
        self.db_path = db_path
        self.read_only = read_only
        self._lock = threading.RLock()

        Path(db_path).mkdir(parents=True, exist_ok=True)
        db_file = os.path.join(db_path, 'data.sqlite')

        if not os.path.exists(db_file) and not create_if_missing and not read_only:
            raise FileNotFoundError(f'Database {db_file} does not exist (create_if_missing=False)')

        # NOTE: we deliberately do NOT open with mode=ro even for read-only
        # databases. A read-only SQLite connection cannot read an un-checkpointed
        # WAL, so a reader opened while a writer is still streaming into the WAL
        # (e.g. a live aim query during training) would see stale/empty data. The
        # local cache is process-private, so a writable connection is safe; the
        # ``read_only`` flag still governs whether we push to S3.
        uri = f'file:{db_file}?uri=true'

        # Autocommit (isolation_level=None) so readers always observe the latest
        # committed state; explicit BEGIN/COMMIT is used for atomic batches.
        self._conn = sqlite3.connect(uri, uri=True, check_same_thread=False, isolation_level=None)
        self._conn.execute('PRAGMA journal_mode = WAL')
        self._conn.execute('PRAGMA synchronous = NORMAL')
        self._create_schema()

    # ----- schema / meta -------------------------------------------------

    def _create_schema(self):
        with self._lock:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS kv (
                    key     BLOB PRIMARY KEY,
                    value   BLOB,
                    deleted INTEGER NOT NULL DEFAULT 0,
                    dirty   INTEGER NOT NULL DEFAULT 0,
                    seq     INTEGER NOT NULL DEFAULT 0
                );
                CREATE INDEX IF NOT EXISTS idx_kv_dirty ON kv(dirty) WHERE dirty = 1;

                CREATE TABLE IF NOT EXISTS pending_range_tombstones (
                    begin_key BLOB NOT NULL,
                    end_key   BLOB NOT NULL,
                    inflight  INTEGER NOT NULL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS merged_segments (
                    name TEXT PRIMARY KEY
                );

                CREATE TABLE IF NOT EXISTS store_meta (
                    k TEXT PRIMARY KEY,
                    v TEXT
                );
                """
            )

    def _meta_get(self, key: str, default: Optional[str] = None) -> Optional[str]:
        row = self._conn.execute('SELECT v FROM store_meta WHERE k = ?', (key,)).fetchone()
        return row[0] if row else default

    def _meta_set(self, key: str, value: str):
        self._conn.execute(
            'INSERT INTO store_meta (k, v) VALUES (?, ?) '
            'ON CONFLICT(k) DO UPDATE SET v = excluded.v',
            (key, value),
        )

    @property
    def writer_id(self) -> str:
        """Stable per-DB-directory writer identity, persisted on first write."""
        wid = self._meta_get('writer_id')
        if wid is None:
            import uuid
            wid = uuid.uuid4().hex
            self._meta_set('writer_id', wid)
        return wid

    # ----- point operations ---------------------------------------------

    def put(self, key: bytes, value: bytes):
        with self._lock:
            self._conn.execute(
                'INSERT INTO kv (key, value, deleted, dirty, seq) VALUES (?, ?, 0, 1, ?) '
                'ON CONFLICT(key) DO UPDATE SET value = excluded.value, deleted = 0, '
                'dirty = 1, seq = ?',
                (key, value, LOCAL_SEQ, LOCAL_SEQ),
            )

    def get(self, key: bytes) -> Optional[bytes]:
        with self._lock:
            row = self._conn.execute(
                'SELECT value FROM kv WHERE key = ? AND deleted = 0', (key,)
            ).fetchone()
            return row[0] if row else None

    def delete(self, key: bytes):
        with self._lock:
            # Record a tombstone so the deletion propagates through a segment.
            self._conn.execute(
                'INSERT INTO kv (key, value, deleted, dirty, seq) VALUES (?, NULL, 1, 1, ?) '
                'ON CONFLICT(key) DO UPDATE SET value = NULL, deleted = 1, dirty = 1, seq = ?',
                (key, LOCAL_SEQ, LOCAL_SEQ),
            )

    def delete_range(self, begin_key: bytes, end_key: bytes):
        with self._lock:
            # Mask existing in-range rows locally...
            self._conn.execute(
                'UPDATE kv SET deleted = 1, value = NULL, dirty = 1, seq = ? '
                'WHERE key >= ? AND key < ?',
                (LOCAL_SEQ, begin_key, end_key),
            )
            # ...and remember the range so the next segment carries it (this
            # masks other writers' overlapping keys on merge).
            self._conn.execute(
                'INSERT INTO pending_range_tombstones (begin_key, end_key) VALUES (?, ?)',
                (begin_key, end_key),
            )

    def write_batch(self, operations: List[tuple]):
        """Atomically apply a list of (op_type, key, value, cf) operations."""
        with self._lock:
            try:
                self._conn.execute('BEGIN')
                for op in operations:
                    op_type, key, value = op[0], op[1], op[2]
                    if op_type in ('PUT', 'MERGE'):
                        self._conn.execute(
                            'INSERT INTO kv (key, value, deleted, dirty, seq) VALUES (?, ?, 0, 1, ?) '
                            'ON CONFLICT(key) DO UPDATE SET value = excluded.value, deleted = 0, '
                            'dirty = 1, seq = ?',
                            (key, value, LOCAL_SEQ, LOCAL_SEQ),
                        )
                    elif op_type == 'DELETE':
                        self._conn.execute(
                            'INSERT INTO kv (key, value, deleted, dirty, seq) VALUES (?, NULL, 1, 1, ?) '
                            'ON CONFLICT(key) DO UPDATE SET value = NULL, deleted = 1, dirty = 1, seq = ?',
                            (key, LOCAL_SEQ, LOCAL_SEQ),
                        )
                    elif op_type == 'DELETE_RANGE':
                        begin_key, end_key = key, value
                        self._conn.execute(
                            'UPDATE kv SET deleted = 1, value = NULL, dirty = 1, seq = ? '
                            'WHERE key >= ? AND key < ?',
                            (LOCAL_SEQ, begin_key, end_key),
                        )
                        self._conn.execute(
                            'INSERT INTO pending_range_tombstones (begin_key, end_key) VALUES (?, ?)',
                            (begin_key, end_key),
                        )
                self._conn.execute('COMMIT')
            except Exception:
                self._conn.execute('ROLLBACK')
                raise

    def multi_get(self, keys: List[bytes]) -> Dict[bytes, Optional[bytes]]:
        with self._lock:
            result: Dict[bytes, Optional[bytes]] = {}
            for key in keys:
                row = self._conn.execute(
                    'SELECT value FROM kv WHERE key = ? AND deleted = 0', (key,)
                ).fetchone()
                result[key] = row[0] if row else None
            return result

    def key_may_exist(self, key: bytes, fetch: bool = False) -> Tuple[bool, Optional[bytes]]:
        with self._lock:
            row = self._conn.execute(
                'SELECT value FROM kv WHERE key = ? AND deleted = 0', (key,)
            ).fetchone()
            if row:
                return (True, row[0] if fetch else None)
            return (False, None)

    # ----- sync helpers (used by the S3 backend) -------------------------

    def collect_dirty(self) -> Tuple[List[Tuple[bytes, Optional[bytes], int]], List[Tuple[bytes, bytes]]]:
        """Snapshot pending writes and mark them in-flight.

        Pending rows (``dirty = 1``) become in-flight (``dirty = 2``) and pending
        range tombstones are flagged ``inflight = 1``. New writes that arrive
        while the segment is being uploaded stay ``dirty = 1`` and are therefore
        not lost. Pair with :meth:`clear_dirty` (success) or :meth:`abort_flush`
        (failure).
        """
        with self._lock:
            self._conn.execute('BEGIN')
            try:
                rows = self._conn.execute(
                    'SELECT key, value, deleted FROM kv WHERE dirty = 1 ORDER BY key'
                ).fetchall()
                ranges = self._conn.execute(
                    'SELECT begin_key, end_key FROM pending_range_tombstones WHERE inflight = 0'
                ).fetchall()
                self._conn.execute('UPDATE kv SET dirty = 2 WHERE dirty = 1')
                self._conn.execute('UPDATE pending_range_tombstones SET inflight = 1 WHERE inflight = 0')
                self._conn.execute('COMMIT')
            except Exception:
                self._conn.execute('ROLLBACK')
                raise
            return ([(r[0], r[1], r[2]) for r in rows], [(r[0], r[1]) for r in ranges])

    def clear_dirty(self, manifest_seq: int):
        """Finalise an in-flight flush at the given manifest seq."""
        with self._lock:
            self._conn.execute(
                'UPDATE kv SET dirty = 0, seq = ? WHERE dirty = 2', (manifest_seq,)
            )
            self._conn.execute('DELETE FROM pending_range_tombstones WHERE inflight = 1')

    def abort_flush(self):
        """Revert in-flight markers so the next flush retries the same writes."""
        with self._lock:
            self._conn.execute('UPDATE kv SET dirty = 1 WHERE dirty = 2')
            self._conn.execute('UPDATE pending_range_tombstones SET inflight = 0 WHERE inflight = 1')

    def apply_segment(
        self,
        rows: List[Tuple[bytes, Optional[bytes], int]],
        ranges: List[Tuple[bytes, bytes]],
        manifest_seq: int,
    ):
        """Merge a downloaded segment into the local view (newest-seq wins).

        Local dirty rows are never clobbered -- they are pending writes that will
        be flushed and assigned a later manifest seq.
        """
        with self._lock:
            try:
                self._conn.execute('BEGIN')
                for begin_key, end_key in ranges:
                    self._conn.execute(
                        'UPDATE kv SET deleted = 1, value = NULL, seq = ? '
                        'WHERE key >= ? AND key < ? AND seq < ? AND dirty = 0',
                        (manifest_seq, begin_key, end_key, manifest_seq),
                    )
                for key, value, deleted in rows:
                    self._conn.execute(
                        'INSERT INTO kv (key, value, deleted, dirty, seq) VALUES (?, ?, ?, 0, ?) '
                        'ON CONFLICT(key) DO UPDATE SET value = excluded.value, '
                        'deleted = excluded.deleted, seq = excluded.seq '
                        'WHERE excluded.seq >= kv.seq AND kv.dirty = 0',
                        (key, value, deleted, manifest_seq),
                    )
                self._conn.execute('COMMIT')
            except Exception:
                self._conn.execute('ROLLBACK')
                raise

    def is_merged(self, name: str) -> bool:
        with self._lock:
            return self._conn.execute(
                'SELECT 1 FROM merged_segments WHERE name = ?', (name,)
            ).fetchone() is not None

    def record_merged(self, name: str):
        with self._lock:
            self._conn.execute(
                'INSERT OR IGNORE INTO merged_segments (name) VALUES (?)', (name,)
            )

    def has_dirty(self) -> bool:
        with self._lock:
            return self._conn.execute(
                'SELECT 1 FROM kv WHERE dirty = 1 LIMIT 1'
            ).fetchone() is not None

    # ----- iteration -----------------------------------------------------

    def iterator(self, kind: str) -> 'LocalIterator':
        return LocalIterator(self, kind)

    def _cursor(self):
        return self._conn.cursor()

    def checkpoint(self):
        """Fold the WAL into the main db file so read-only openers see all data."""
        with self._lock:
            if self._conn is not None:
                try:
                    self._conn.execute('PRAGMA wal_checkpoint(TRUNCATE)')
                except Exception:
                    pass

    def close(self):
        with self._lock:
            if self._conn is not None:
                self.checkpoint()
                try:
                    self._conn.close()
                finally:
                    self._conn = None


class LocalIterator:
    """Ordered iterator over the (non-deleted) keyspace, in byte-wise key order.

    ``kind`` selects the projection: ``'items'`` -> (key, value),
    ``'keys'`` -> key, ``'values'`` -> value.
    """

    def __init__(self, store: LocalStore, kind: str):
        self._store = store
        self._kind = kind
        self._cur = None            # SQLite cursor streaming rows AFTER _current
        self._current: Optional[Tuple[bytes, bytes]] = None  # always full (key, value)

    # -- positioning --

    def seek_to_first(self):
        with self._store._lock:
            self._cur = self._store._cursor()
            self._cur.execute('SELECT key, value FROM kv WHERE deleted = 0 ORDER BY key')
            self._advance()

    def seek(self, key: bytes):
        with self._store._lock:
            self._cur = self._store._cursor()
            self._cur.execute(
                'SELECT key, value FROM kv WHERE deleted = 0 AND key >= ? ORDER BY key', (key,)
            )
            self._advance()

    def seek_for_prev(self, key: bytes):
        with self._store._lock:
            row = self._store._conn.execute(
                'SELECT key, value FROM kv WHERE deleted = 0 AND key <= ? ORDER BY key DESC LIMIT 1',
                (key,),
            ).fetchone()
            self._position_after(row)

    def seek_to_last(self):
        with self._store._lock:
            row = self._store._conn.execute(
                'SELECT key, value FROM kv WHERE deleted = 0 ORDER BY key DESC LIMIT 1'
            ).fetchone()
            self._position_after(row)

    def _position_after(self, row):
        """Set current to ``row`` and open a forward cursor strictly after it."""
        if row is None:
            self._current = None
            self._cur = None
            return
        self._current = (row[0], row[1])
        self._cur = self._store._cursor()
        self._cur.execute(
            'SELECT key, value FROM kv WHERE deleted = 0 AND key > ? ORDER BY key', (row[0],)
        )

    def _advance(self):
        row = self._cur.fetchone() if self._cur is not None else None
        self._current = (row[0], row[1]) if row is not None else None

    # -- access --

    def _project(self, item: Tuple[bytes, bytes]):
        if self._kind == 'keys':
            return item[0]
        if self._kind == 'values':
            return item[1]
        return item

    def get(self):
        if self._current is None:
            raise ValueError()
        return self._project(self._current)

    def next(self):
        """Return current item then advance; ``None`` when exhausted."""
        if self._current is None:
            return None
        with self._store._lock:
            ret = self._project(self._current)
            self._advance()
            return ret

    def skip(self):
        if self._current is None:
            raise ValueError()
        with self._store._lock:
            self._advance()

    def __iter__(self):
        return self

    def __next__(self):
        item = self.next()
        if item is None:
            raise StopIteration
        return item
