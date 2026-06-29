import os

import click

from aim.sdk.repo import Repo
from aim.sdk.utils import clean_repo_path


@click.command()
@click.option('--repo', required=False, type=click.Path(exists=True, file_okay=False, dir_okay=True, writable=True))
@click.option('-y', '--yes', is_flag=True, help='Automatically confirm prompt')
@click.option('-s', '--skip-if-exists', is_flag=True, help='Skip initialization if the repo already exists')
@click.option(
    '--sync-from',
    required=False,
    default=None,
    metavar='S3_URI',
    help='S3 URI to restore runs from (e.g. s3://my-bucket/aim/). '
         'Pulls run inventory and data from S3 into the newly created local repo.',
)
def init(repo, yes, skip_if_exists, sync_from):
    """
    Initializes new repository in the --repo directory.
    Initializes new repository in the current working directory if --repo argument is not provided:
     - Creates .aim directory & runs upgrades for structured DB

    Optionally restore runs from S3 with --sync-from s3://bucket/prefix/.
    """
    repo_path = clean_repo_path(repo) or os.getcwd()
    re_init = False
    if Repo.exists(repo_path):
        if yes and skip_if_exists:
            raise click.BadParameter('Conflicting init options.Either specify -y/--yes or -s/--skip-if-exists')
        elif yes:
            re_init = True
        elif skip_if_exists:
            click.echo('Repo exists at {}. Skipped initialization.'.format(repo_path))
            return
        else:
            re_init = click.confirm(
                'Aim repository is already initialized. Do you want to re-initialize to empty Aim repository?'
            )
        if not re_init:
            return
        # Clear old repo
        Repo.rm(repo_path)

    repo_obj = Repo.from_path(repo_path, init=True)
    if re_init:
        click.echo('Re-initialized empty Aim repository at {}'.format(repo_obj.root_path))
    else:
        click.echo('Initialized a new Aim repository at {}'.format(repo_obj.root_path))

    if sync_from:
        _sync_from_s3(repo_obj, sync_from)


def _restore_run_props(repo_obj: 'Repo', run_hash: str):
    """Reconstruct structured DB props from the litewave meta tree.

    aim's Run property setters (name, experiment, description, archived, tags,
    created_at) mirror their values into ``meta_run_tree['__props__', <key>]``
    at write time.  Here we read them back and apply them to the structured DB
    entry so run_metadata.sqlite is fully reconstructed after a sync-from-s3.

    Each setter on ModelMappedRun self-commits via session_commit_or_flush, so
    we call them directly — no surrounding ``with structured_db`` needed (that
    would open a second session and the props object would be on a different one).
    """
    try:
        meta_tree = repo_obj.request_tree('meta', run_hash, read_only=True).subtree('meta')
        run_tree = meta_tree.subtree('chunks').subtree(run_hash)

        try:
            stored = run_tree.subtree('__props__').collect()
        except (KeyError, StopIteration):
            return  # run predates mirroring — nothing to restore

        # find_run returns a ModelMappedRun bound to its own auto-commit session.
        props = repo_obj.structured_db.find_run(run_hash)
        if not props:
            return

        if stored.get('name'):
            props.name = stored['name']
        if stored.get('experiment'):
            props.experiment = stored['experiment']
        if stored.get('description') is not None:
            props.description = stored['description']
        if stored.get('archived') is not None:
            props.archived = stored['archived']
        for tag in (stored.get('tags') or []):
            try:
                props.add_tag(tag)
            except Exception:
                pass
    except Exception:
        pass  # best-effort; missing props don't break the sync


def _sync_from_s3(repo_obj: 'Repo', s3_uri: str):
    """Pull run inventory and data from S3 into the local repo."""
    from aim import litewave
    from aim.sdk.index_manager import RepoIndexManager

    # Parse s3://bucket/prefix
    if not s3_uri.startswith('s3://'):
        raise click.BadParameter(f'--sync-from must be an S3 URI (s3://bucket/prefix), got: {s3_uri}')
    without_scheme = s3_uri[len('s3://'):]
    bucket, _, prefix = without_scheme.partition('/')
    if not bucket:
        raise click.BadParameter(f'Could not parse bucket from {s3_uri!r}')
    # Normalise prefix: no leading slash, trailing slash present
    prefix = prefix.strip('/')
    prefix = (prefix + '/') if prefix else ''

    cfg = litewave.S3Config(bucket=bucket, prefix=prefix)

    click.echo(f'Listing runs in s3://{bucket}/{prefix} ...')
    try:
        s3_hashes = litewave.list_run_hashes(cfg, aim_path=repo_obj.path)
    except Exception as exc:
        raise click.ClickException(f'Failed to list runs from S3: {exc}') from exc

    if not s3_hashes:
        click.echo('No runs found in S3 — nothing to sync.')
        return

    click.echo(f'Found {len(s3_hashes)} run(s). Syncing...')

    # Set the shared config so every DB opened below pulls from S3.
    litewave.active_config.set(cfg)

    chunks_dir = os.path.join(repo_obj.path, 'meta', 'chunks')
    os.makedirs(chunks_dir, exist_ok=True)

    index_manager = RepoIndexManager.get_index_manager(repo_obj)
    failed = []
    for i, run_hash in enumerate(s3_hashes, 1):
        click.echo(f'  [{i}/{len(s3_hashes)}] {run_hash}', nl=False)
        try:
            os.makedirs(os.path.join(chunks_dir, run_hash), exist_ok=True)

            if not repo_obj.structured_db.find_run(run_hash):
                with repo_obj.structured_db:
                    repo_obj.structured_db.create_run(run_hash)

            index_manager.index(run_hash)

            # Restore run name and experiment from the meta tree (mirrored
            # there by ExperimentLogger at write time).
            _restore_run_props(repo_obj, run_hash)

            click.echo(' ✓')
        except Exception as exc:
            click.echo(f' ✗ ({exc})')
            failed.append(run_hash)

    litewave.active_config.clear()

    if failed:
        click.secho(f'Warning: {len(failed)} run(s) failed to index: {failed}', fg='yellow')
    click.echo(f'Synced {len(s3_hashes) - len(failed)}/{len(s3_hashes)} run(s) into {repo_obj.root_path}')