"""S3Config: env loading, precedence, and path -> S3-key mapping."""

import pytest

from aim.litewave import S3Config, active_config
from aim.litewave._config import resolve_config


def _cfg(prefix='aim/', local_root=None):
    return S3Config(prefix=prefix, local_root=local_root)


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


def test_prefix_is_normalised():
    assert S3Config(prefix='aim').prefix == 'aim/'
    assert S3Config(prefix='/aim/').prefix == 'aim/'
    assert S3Config(prefix='').prefix == ''


def test_default_is_local_only():
    # No bucket -> S3 disabled, regardless of how it was built.
    assert S3Config().enabled is False


def test_explicit_config_enables_s3():
    cfg = S3Config(bucket='my-bucket', prefix='aim/')
    assert cfg.enabled is True
    assert cfg.bucket == 'my-bucket'
    assert cfg.prefix == 'aim/'


def test_from_env_reads_environment(monkeypatch):
    monkeypatch.setenv('LITEWAVE_S3_BUCKET', 'env-bucket')
    monkeypatch.setenv('LITEWAVE_S3_PREFIX', 'envprefix')
    cfg = S3Config.from_env()
    assert cfg.bucket == 'env-bucket'
    assert cfg.prefix == 'envprefix/'


def test_from_env_local_only_when_unset(monkeypatch):
    monkeypatch.delenv('LITEWAVE_S3_BUCKET', raising=False)
    assert S3Config.from_env().enabled is False


def test_boto_client_kwargs():
    assert S3Config(bucket='b').boto_client_kwargs() == {}
    cfg = S3Config(bucket='b', region='eu-central-1', endpoint_url='http://localhost:9000')
    assert cfg.boto_client_kwargs() == {
        'region_name': 'eu-central-1',
        'endpoint_url': 'http://localhost:9000',
    }


@pytest.fixture
def clean_active_config():
    """Reset the shared holder around a test so cases don't leak into each other."""
    saved = active_config.get()
    active_config.clear()
    try:
        yield
    finally:
        active_config.set(saved)


def test_resolve_explicit_arg_wins(clean_active_config, monkeypatch):
    monkeypatch.setenv('LITEWAVE_S3_BUCKET', 'env-bucket')
    active_config.set(S3Config(bucket='shared-bucket'))
    resolved = resolve_config(S3Config(bucket='arg-bucket'))
    assert resolved.bucket == 'arg-bucket'


def test_resolve_active_config_beats_env(clean_active_config, monkeypatch):
    monkeypatch.setenv('LITEWAVE_S3_BUCKET', 'env-bucket')
    active_config.set(S3Config(bucket='shared-bucket'))
    assert resolve_config().bucket == 'shared-bucket'


def test_resolve_falls_back_to_env(clean_active_config, monkeypatch):
    monkeypatch.setenv('LITEWAVE_S3_BUCKET', 'env-bucket')
    assert resolve_config().bucket == 'env-bucket'


def test_resolve_local_only_when_nothing_set(clean_active_config, monkeypatch):
    monkeypatch.delenv('LITEWAVE_S3_BUCKET', raising=False)
    assert resolve_config().enabled is False


def test_active_config_clear(clean_active_config):
    active_config.set(S3Config(bucket='b'))
    assert active_config.get().bucket == 'b'
    active_config.clear()
    assert active_config.get() is None