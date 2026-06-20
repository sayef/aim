"""
Configuration and backend selection for the litewave engine.

All configuration is read from environment variables. The S3 backend is enabled
automatically when ``LITEWAVE_S3_BUCKET`` is set; otherwise litewave behaves as a
purely local SQLite key-value store (no network, no background sync).
"""

import hashlib
import os
from pathlib import Path
from typing import Optional


def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    value = os.environ.get(name)
    if value is None or value == '':
        return default
    return value


def _env_int(name: str, default: int) -> int:
    raw = _env(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _env_float(name: str, default: float) -> float:
    raw = _env(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


class S3Config:
    """Resolved S3 configuration, derived once from the environment."""

    def __init__(self):
        self.bucket = _env('LITEWAVE_S3_BUCKET')
        self.prefix = _env('LITEWAVE_S3_PREFIX', 'litewave/')
        self.region = _env('LITEWAVE_S3_REGION')
        self.endpoint_url = _env('LITEWAVE_S3_ENDPOINT')
        self.local_root = _env('LITEWAVE_LOCAL_ROOT')
        self.flush_interval = _env_float('LITEWAVE_FLUSH_INTERVAL_SEC', 5.0)
        self.compact_threshold = _env_int('LITEWAVE_SEGMENT_COMPACT_THRESHOLD', 16)

        # Normalise prefix to a single trailing slash, no leading slash.
        prefix = (self.prefix or '').strip('/')
        self.prefix = (prefix + '/') if prefix else ''

    @property
    def enabled(self) -> bool:
        return bool(self.bucket)

    def boto_client_kwargs(self) -> dict:
        kwargs = {}
        if self.region:
            kwargs['region_name'] = self.region
        if self.endpoint_url:
            kwargs['endpoint_url'] = self.endpoint_url
        return kwargs

    def s3_root_for(self, db_path: str) -> str:
        """Map a local DB path to its S3 key prefix (the per-DB tree root).

        The tree mirrors the local directory structure relative to
        ``LITEWAVE_LOCAL_ROOT``. When the root is not configured we auto-detect
        the nearest ``.aim`` ancestor and root at it, so the ``.aim`` segment is
        stripped from the S3 keys (e.g. local ``<repo>/.aim/meta/chunks/<id>``
        maps to ``<prefix>meta/chunks/<id>``). If neither applies, we fall back
        to a stable hash of the absolute path so distinct DBs never collide.
        """
        abs_path = Path(db_path).expanduser().resolve()

        root = self._resolve_local_root(abs_path)
        rel: Optional[str] = None
        if root is not None:
            try:
                rel = abs_path.relative_to(root).as_posix()
            except ValueError:
                rel = None

        if not rel:
            rel = hashlib.sha256(str(abs_path).encode('utf-8')).hexdigest()

        return f'{self.prefix}{rel}'

    def _resolve_local_root(self, abs_path: Path) -> Optional[Path]:
        if self.local_root:
            return Path(self.local_root).expanduser().resolve()
        # Auto-detect: root at the nearest '.aim' ancestor itself, so the
        # '.aim' segment is kept locally but stripped from the S3 keys.
        for parent in abs_path.parents:
            if parent.name == '.aim':
                return parent
        return None


# A single resolved configuration per process. litewave is reconfigured by
# re-importing in a fresh process, matching the lifecycle of CLI/run processes.
config = S3Config()
