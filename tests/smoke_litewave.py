"""Standalone smoke test: aim on the litewave storage backend.

Runs without the heavy test fixtures so it works in a minimal CI environment:
track a few metrics, close, reindex, reopen, and assert the values round-trip.
"""

import tempfile


def main():
    from aim import Run, Repo

    repo_dir = tempfile.mkdtemp(prefix='aim_litewave_smoke_')

    run = Run(repo=repo_dir, experiment='smoke', system_tracking_interval=None)
    run['hp'] = {'lr': 0.01, 'batch': 32}
    for step in range(5):
        run.track(step * 0.1, name='loss', step=step, context={'subset': 'train'})
    run_hash = run.hash
    run.close()

    repo = Repo(repo_dir)
    repo._recreate_index()

    r = repo.get_run(run_hash)
    assert r is not None, 'run not found after reopen'
    assert r['hp'] == {'lr': 0.01, 'batch': 32}, r['hp']

    loss = [m for m in r.metrics() if m.name == 'loss'][0]
    df = loss.dataframe().sort_values('step')
    pairs = dict(zip(df['step'].tolist(), [round(float(v), 3) for v in df['value'].tolist()]))
    assert pairs == {0: 0.0, 1: 0.1, 2: 0.2, 3: 0.3, 4: 0.4}, pairs

    print('OK: aim + litewave smoke passed ->', pairs)


if __name__ == '__main__':
    main()
