"""Path -> S3-key mapping (litewave._config.S3Config.s3_root_for)."""

from aim.litewave._config import S3Config


def _cfg(prefix='aim/', local_root=None):
    cfg = S3Config()
    cfg.prefix = prefix
    cfg.local_root = local_root
    return cfg


def test_auto_detect_strips_dot_aim(tmp_path):
    # Local layout keeps `.aim`; the S3 key must not include it.
    cfg = _cfg()
    p = tmp_path / '.aim' / 'meta' / 'chunks' / 'abc123'
    assert cfg.s3_root_for(str(p)) == 'aim/meta/chunks/abc123'


def test_explicit_local_root(tmp_path):
    cfg = _cfg(local_root=str(tmp_path / '.aim'))
    p = tmp_path / '.aim' / 'seqs' / 'chunks' / 'xyz'
    assert cfg.s3_root_for(str(p)) == 'aim/seqs/chunks/xyz'


def test_fallback_hash_when_no_root(tmp_path):
    # No `.aim` ancestor and no explicit root -> stable hash, still under prefix.
    cfg = _cfg()
    p = tmp_path / 'some' / 'random' / 'db'
    key = cfg.s3_root_for(str(p))
    assert key.startswith('aim/') and len(key) > len('aim/')
