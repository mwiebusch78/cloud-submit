import os
import datetime as dt
import subprocess
import sys

from .utils import clear_path, ensure_path, CloudSubmitError
from .config import Config
from .build import build


def _prepare_artifact_path(path):
    path = os.path.abspath(path)
    if not path.startswith('/root/'):
        raise CloudSubmitError('Project folder must be under /root.')

    try:
        os.makedirs(path, exist_ok=True)
    except Exception as e:
        raise CloudSubmitError(
            f'Error creating artifact directory {path}: {str(e)}')

    subdir=path[len('/root/'):]
    mount_arg = (
        'type=volume,'
        'src=devenv-home,'
        'dst=/root/artifacts,'
        f'volume-subpath={subdir}'
    )
    return mount_arg


def _docker_run(image, artifact_path, env):
    mount_arg = _prepare_artifact_path(artifact_path)
    command = [
        'docker',
        'run',
        '--rm',
        '--mount',
        mount_arg,
        image,
    ]
    try:
        result = subprocess.run(
            command,
            stdout=sys.stdout,
            stderr=sys.stderr,
            env=env,
        )
    except KeyboardInterrupt:
        raise CloudSubmitError('Aborted on user request.')

    if result.returncode != 0:
        raise CloudSubmitError(
            f'Container exited with status code {result.returncode}.')


def run_local(service, build_id=None, run_id=None, product_root=None):
    # Read config
    config = Config(product_root=product_root)

    # Generate build and run ID if necessary.
    if not run_id:
        run_id = dt.datetime.utcnow().strftime('%Y%m%d-%H%M%S')
    if not build_id:
        build_id = run_id

    # Build image.
    image = build(service, build_id=build_id, product_root=product_root)

    # Run job.
    print(f'Running service {service} locally.')
    _docker_run(
        image,
        config.get_artifact_path(service, run_id),
        config.get_env(),
    )
