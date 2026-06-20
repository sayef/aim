"""Generate a few demo runs so there's something to look at in the aim UI.

Run it with the litewave S3 env vars set (see the printed hint) and it will write
to the local .aim repo AND flush to S3. Safe to run repeatedly.
"""
import math
import os
import random

from aim import Run, Repo

REPO = os.environ.get('AIM_REPO', os.path.dirname(os.path.abspath(__file__)))

EXPERIMENTS = ['baseline', 'tuned']
OPTIMIZERS = ['adam', 'sgd']


def main():
    backend = 'S3 (' + os.environ['LITEWAVE_S3_BUCKET'] + ')' if os.environ.get('LITEWAVE_S3_BUCKET') else 'local-only'
    print(f'repo={REPO}  backend={backend}')
    for i in range(4):
        run = Run(repo=REPO, experiment=EXPERIMENTS[i % 2])
        lr = 10 ** -random.randint(2, 4)
        run['hparams'] = {'learning_rate': lr, 'batch_size': 2 ** (4 + i), 'optimizer': OPTIMIZERS[i % 2]}
        run['model'] = f'demo-net-{i}'
        for step in range(80):
            run.track(2.5 * math.exp(-step / 25) + random.uniform(0, 0.05),
                      name='loss', step=step, context={'subset': 'train'})
            run.track(0.4 + 0.55 * (1 - math.exp(-step / 20)) + random.uniform(-0.02, 0.02),
                      name='accuracy', step=step, context={'subset': 'val'})
        print(f'  tracked run {run.hash}  (exp={EXPERIMENTS[i % 2]}, lr={lr})')
        run.close()

    # Make the runs queryable immediately (the `aim up` server also indexes,
    # but this guarantees they show up right away).
    Repo(REPO)._recreate_index()
    print('done — refresh the aim UI')


if __name__ == '__main__':
    main()
