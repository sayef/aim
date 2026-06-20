"""
litewave public API: ``DB``, ``Options``, ``WriteBatch`` and the ``Iterator``
family.

Backed by a local SQLite cache (:mod:`litewave._local`) that, when
``LITEWAVE_S3_BUCKET`` is configured, periodically flushes to S3 as immutable
segments (:mod:`litewave._s3`).

Pure Python -- no native build, no Cython, no third-party storage engine.
"""

from typing import Dict, List, Optional, Tuple

from ._config import config
from ._local import LocalStore

__all__ = [
    'Options',
    'WriteBatch',
    'DB',
    'Iterator',
    'BaseIterator',
    'KeysIterator',
    'ValuesIterator',
    'ItemsIterator',
]


class Iterator:
    """Base iterator type.

    The default methods define the sentinel-``None`` ``next()`` protocol;
    subclasses (and the concrete :class:`litewave._local.LocalIterator` returned
    by ``DB``) override them. Consumers may subclass this as their iterator base.
    """

    def __iter__(self):
        return self

    def __next__(self):
        item = self.next()
        if item is None:
            raise StopIteration
        return item

    def next(self):
        return None

    def get(self):
        raise ValueError()

    def skip(self):
        pass


# Named iterator types. Concrete iteration uses LocalIterator (returned by
# DB.iter*); these exist for `from aim.litewave import *` and for type references.
class BaseIterator(Iterator):
    pass


class KeysIterator(BaseIterator):
    pass


class ValuesIterator(BaseIterator):
    pass


class ItemsIterator(BaseIterator):
    pass


class Options:
    """Permissive options bag. Engine-tuning kwargs are accepted and ignored."""

    def __init__(self, create_if_missing: bool = True, **kwargs):
        self.create_if_missing = create_if_missing
        # ``aim`` checks/sets ``in_use``; keep it for compatibility.
        self.in_use = False
        for key, value in kwargs.items():
            setattr(self, key, value)


class WriteBatch:
    """Accumulates write operations for an atomic :meth:`DB.write`."""

    def __init__(self, data: Optional[bytes] = None):
        self.operations: List[tuple] = []

    def put(self, key: bytes, value: bytes, column_family=None):
        self.operations.append(('PUT', key, value, column_family))

    def merge(self, key: bytes, value: bytes, column_family=None):
        # Merge operators are unsupported; treat as last-write-wins.
        self.operations.append(('MERGE', key, value, column_family))

    def delete(self, key: bytes, column_family=None):
        self.operations.append(('DELETE', key, None, column_family))

    def delete_range(self, begin_key: bytes, end_key: bytes, column_family=None):
        self.operations.append(('DELETE_RANGE', begin_key, end_key, column_family))

    def clear(self):
        self.operations.clear()

    def count(self) -> int:
        return len(self.operations)

    def __iter__(self):
        return iter(self.operations)


class DB:
    """Ordered key-value database with a local SQLite cache and optional S3 sync."""

    def __init__(
        self,
        db_path: str,
        opts: Optional[Options] = None,
        column_families: Optional[Dict] = None,
        read_only: bool = False,
        **kwargs,
    ):
        self.db_path = db_path
        self.opts = opts if opts is not None else Options()
        self.read_only = read_only

        # The local SQLite cache is always writable: read-only opens still need
        # to materialise S3 segments into it. ``read_only`` only governs whether
        # we push to S3.
        self._store = LocalStore(
            db_path,
            read_only=False,
            create_if_missing=getattr(self.opts, 'create_if_missing', True),
        )

        self._s3 = None
        if config.enabled:
            from ._s3 import S3SyncBackend
            self._s3 = S3SyncBackend(self._store, db_path, config, read_only=read_only)

    # ----- point operations ---------------------------------------------

    def put(self, key: bytes, value: bytes, sync: bool = False, disable_wal: bool = False,
            column_family=None):
        self._store.put(key, value)

    def get(self, key: bytes, column_family=None) -> Optional[bytes]:
        return self._store.get(key)

    def delete(self, key: bytes, sync: bool = False, disable_wal: bool = False, column_family=None):
        self._store.delete(key)

    def delete_range(self, begin_key: bytes, end_key: bytes, sync: bool = False,
                     disable_wal: bool = False, column_family=None):
        self._store.delete_range(begin_key, end_key)

    def merge(self, key: bytes, value: bytes, sync: bool = False, disable_wal: bool = False,
              column_family=None):
        self._store.put(key, value)

    def write(self, batch: WriteBatch, sync: bool = False, disable_wal: bool = False):
        self._store.write_batch(batch.operations)

    def multi_get(self, keys: List[bytes], column_family=None) -> Dict[bytes, Optional[bytes]]:
        return self._store.multi_get(keys)

    def key_may_exist(self, key: bytes, fetch: bool = False, column_family=None
                      ) -> Tuple[bool, Optional[bytes]]:
        return self._store.key_may_exist(key, fetch=fetch)

    # ----- iteration -----------------------------------------------------

    def iterkeys(self, column_family=None):
        return self._store.iterator('keys')

    def itervalues(self, column_family=None):
        return self._store.iterator('values')

    def iteritems(self, column_family=None):
        return self._store.iterator('items')

    # ----- durability / maintenance -------------------------------------

    def flush(self):
        # Fold the WAL into the main db file so read-only openers (e.g. aim
        # queries) observe everything written so far, then push to S3.
        self._store.checkpoint()
        if self._s3 is not None:
            self._s3.flush()

    def flush_wal(self, sync: bool = False):
        self._store.checkpoint()
        if self._s3 is not None:
            self._s3.flush()

    def compact_range(self, begin=None, end=None, column_family=None, **kwargs):
        if self._s3 is not None:
            self._s3.compact()

    def close(self):
        if self._s3 is not None:
            self._s3.close()
            self._s3 = None
        if self._store is not None:
            self._store.close()
            self._store = None

    # ----- misc compatibility shims -------------------------------------

    def get_column_family(self, name: bytes):
        return name.decode('utf-8') if isinstance(name, (bytes, bytearray)) else name

    @property
    def column_families(self):
        return ['default']

    def get_property(self, prop: bytes, column_family=None):
        return None

    def get_live_files_metadata(self):
        return []

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass
