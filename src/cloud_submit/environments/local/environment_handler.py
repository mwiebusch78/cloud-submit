import os
import sys
import shutil
import subprocess

from ..handler import EnvironmentHandler
from ...utils import ensure_path, CloudSubmitError


class LocalEnv(EnvironmentHandler):
    def install_execution_handler(self, path):
        sourcedir = os.path.dirname(__file__)
        shutil.copyfile(
            os.path.join(sourcedir, 'execution_handler.py'),
            os.path.join(path, 'execution_handler.py'),
        )

    def _get_artifacts_mount(self, path):
        volume = os.environ.get('CSUB_DOD_VOLUME', None)
        if not volume:
            return f'type=bind,src={path},dst=/root/artifacts'
        dod_mount = os.environ.get('CSUB_DOD_MOUNT_POINT', None)
        if not dod_mount:
            raise CloudSubmitError(
                'CSUB_DOD_MOUNT_POINT variable must be set to use cloud-submit '
                'in a docker-outside-docker setup. If do not want to use '
                'cloud-submit in docker-outside-docker mode make sure that the '
                'variable CSUB_DOD_VOLUME is *not* set.'
            )
        path = os.path.abspath(path)
        dod_mount = os.path.abspath(dod_mount)
        if os.path.commonpath([path, dod_mount]) != dod_mount:
            raise CloudSubmitError(
                f'Artifact directory {path} is not a subpath of {dod_mount}. '
                'Change the CSUB_DOD_MOUNT_POINT variable or move your '
                'project directory.'
            )
        subpath = os.path.relpath(path, start=dod_mount)
        return (
            'type=volume,'
            f'src={volume},'
            'dst=/root/artifacts,'
            f'volume-subpath={subpath}'
        )

    def pull_image(self, ref):
        command = [
            'docker',
            'images',
            ref,
            '-q',
        ]
        try:
            result = subprocess.run(
                command,
                stdout=subprocess.PIPE,
            )
        except KeyboardInterrupt:
            raise CloudSubmitError('Aborted on user request.')
        if result.returncode != 0:
            raise CloudSubmitError(
                'docker images command exited with status code '
                f'{result.returncode}.'
            )
        if not result.stdout.strip():
            raise CloudSubmitError(
                f'Could not find image {ref}. You may have to build it again.'
            )

    def submit(self, pipeline, image_refs, run_id):
        artifacts_path = os.path.join('artifacts', pipeline.name, run_id)
        ensure_path(artifacts_path)
        for step in pipeline.steps:
            if step.name not in image_refs:
                continue
            ref = image_refs[step.name]

            command = [
                'docker',
                'run',
                '--rm',
                '--mount', self._get_artifacts_mount(artifacts_path),
                ref,
                pipeline.name,
                step.name,
            ]
            try:
                result = subprocess.run(
                    command,
                    stdout=sys.stdout,
                    stderr=sys.stderr,
                )
            except KeyboardInterrupt:
                raise CloudSubmitError('Aborted on user request.')

            if result.returncode != 0:
                raise CloudSubmitError(
                    f'Container exited with status code {result.returncode}.')
