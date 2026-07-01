import os
import sys
import shutil
import subprocess

# We use relative imports here to avoid circular imports because this module is
# used by some of the core cloud-submit modules. Normally you can just do
#
# from cloud_submit import ...
from ...environment_handler import EnvironmentHandler
from ...utils import (
    ensure_path,
    CloudSubmitError,
    build_docker_mount_option,
    run_command,
)


def build_artifacts_mount_option(path, scope):
    return build_docker_mount_option(path, f'/root/artifacts/{scope}')


class LocalEnv(EnvironmentHandler):
    def install_execution_handler(self, path):
        sourcedir = os.path.dirname(__file__)
        shutil.copyfile(
            os.path.join(sourcedir, 'execution_handler.py'),
            os.path.join(path, 'execution_handler.py'),
        )

    def pull_image(self, ref):
        result = run_command(
            [
                self._docker_command,
                'images',
                ref,
                '-q',
            ],
            stdout=subprocess.PIPE,
            text=True,
        )
        if not result.stdout.strip():
            raise CloudSubmitError(
                f'Could not find image {ref}. You may have to build it again.'
            )

    def list_remote_image_tags(self, repo_name):
        return []

    def remove_remote_image_refs(self, refs):
        pass

    def get_remote_artifact_path(self, artifact, run_id=None):
        return ''

    def list_remote_artifacts(self, artifacts, run_ids=None):
        return [[] for a in artifacts]

    def remove_remote_artifacts(self, artifacts, run_ids):
        pass

    def run_pipeline(self, pipeline, image_refs, timestamp, run_id):
        artifacts_project_path = os.path.join('artifacts', 'shared')
        artifacts_user_path = os.path.join(
            'artifacts', 'users', self._user, 'shared')
        artifacts_run_path = os.path.join(
            'artifacts', 'users', self._user, 'runs', run_id)
        ensure_path(artifacts_project_path)
        ensure_path(artifacts_user_path)
        ensure_path(artifacts_run_path)
        for step in pipeline.steps:
            if step.name not in image_refs:
                continue
            ref = image_refs[step.name]

            worker_indices = [-1]
            if step.num_workers is not None:
                worker_indices = range(step.num_workers)

            for worker_index in worker_indices:
                run_command([
                    self._docker_command,
                    'run',
                    '--rm',
                    '--mount',
                    build_artifacts_mount_option(
                        artifacts_project_path, 'project'),
                    '--mount',
                    build_artifacts_mount_option(artifacts_user_path, 'user'),
                    '--mount',
                    build_artifacts_mount_option(artifacts_run_path, 'run'),
                    '--env', f'CSUB_TIMESTAMP={timestamp.isoformat()}',
                    '--env', f'CSUB_RUN_ID={run_id}',
                    '--env', f'CSUB_WORKER_INDEX={worker_index}',
                    ref,
                    pipeline.name,
                    step.name,
                ])
