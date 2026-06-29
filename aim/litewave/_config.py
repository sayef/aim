"""
Configuration for the litewave engine.

:class:`S3Config` is a plain, self-contained value object. Clients construct it
themselves and hand it to :class:`litewave.DB` (or whatever wraps it). There is
no process-wide singleton and nothing to ``configure()`` globally:

    from aim.litewave import DB, S3Config

    db = DB(path, config=S3Config(bucket="my-bucket", prefix="aim/"))

When a ``DB`` is opened without a ``config`` it falls back to
:meth:`S3Config.from_env`, which reads the ``LITEWAVE_*`` environment variables;
with no bucket from either source litewave is a purely local SQLite key-value
store (no network, no background sync).
"""

import hashlib
import os
from dataclasses import dataclass
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


@dataclass
class S3Config:
    """litewave configuration value object.

    Construct it directly to drive S3 sync from code, or call
    :meth:`from_env` to build one from the ``LITEWAVE_*`` environment variables.
    With no ``bucket`` set, litewave runs purely locally.
    """

    bucket: Optional[str] = None
    prefix: str = 'litewave/'
    region: Optional[str] = None
    endpoint_url: Optional[str] = None
    local_root: Optional[str] = None
    flush_interval: float = 5.0
    compact_threshold: int = 16

    def __post_init__(self):
        # Normalise prefix to a single trailing slash, no leading slash.
        prefix = (self.prefix or '').strip('/')
        self.prefix = (prefix + '/') if prefix else ''

    @classmethod
    def from_env(cls) -> 'S3Config':
        """Build a config from the ``LITEWAVE_*`` environment variables."""
        return cls(
            bucket=_env('LITEWAVE_S3_BUCKET'),
            prefix=_env('LITEWAVE_S3_PREFIX', 'litewave/'),
            region=_env('LITEWAVE_S3_REGION'),
            endpoint_url=_env('LITEWAVE_S3_ENDPOINT'),
            local_root=_env('LITEWAVE_LOCAL_ROOT'),
            flush_interval=_env_float('LITEWAVE_FLUSH_INTERVAL_SEC', 5.0),
            compact_threshold=_env_int('LITEWAVE_SEGMENT_COMPACT_THRESHOLD', 16),
        )

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
        ``local_root``. When the root is not configured we auto-detect the
        nearest ``.aim`` ancestor and root at it, so the ``.aim`` segment is
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


class ActiveConfig:
    """Process-wide default :class:`S3Config`.

    A small, explicit holder for components that do not create the ``DB``
    directly -- notably ``aim``, which opens DBs deep in its storage layer with
    no way to pass a config down. Set it once at application/process startup::

        from aim.litewave import S3Config, active_config
        active_config.set(S3Config(bucket="my-bucket", prefix="aim/"))

    A ``DB`` opened without an explicit ``config=`` argument falls back to this
    holder, then to :meth:`S3Config.from_env`, then to a local-only store.
    """

    def __init__(self):
        self._config: Optional[S3Config] = None

    def set(self, config: Optional[S3Config]) -> None:
        self._config = config

    def get(self) -> Optional[S3Config]:
        return self._config

    def clear(self) -> None:
        self._config = None


# The single shared holder. Mutated via ``active_config.set(...)``.
active_config = ActiveConfig()


def resolve_config(config: Optional[S3Config] = None) -> S3Config:
    """Resolve the config a ``DB`` should use, honouring the precedence:

    explicit argument -> shared :data:`active_config` -> environment -> local-only.
    """
    if config is not None:
        return config
    shared = active_config.get()
    if shared is not None:
        return shared
    return S3Config.from_env()