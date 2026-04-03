import sys
import datetime as dt
import uuid
import subprocess

from ..utils import CloudSubmitError
from ..images import BaseImage, ExecutionImage


class EnvironmentHandler:
    def __init__(
            self,
            name,
            project,
            user,
            docker_registry=None,
            docker_namespace='csub',
    ):
        self.name = name
        self._project = project
        self._user = user
        self._docker_registry = docker_registry
        self._docker_namespace = docker_namespace

    def _generate_id(self, id=None):
        if id is not None:
            return id
        now = dt.datetime.utcnow()
        uid = str(uuid.uuid4())
        return now.strftime('%Y%m%d-%H%M%S-') + uid[:4]

    def generate_build_id(self, build_id=None):
        return self._generate_id(build_id)

    def generate_run_id(self, run_id=None):
        return self._generate_id(run_id)

    def install_execution_handler(self, path):
        raise NotImplementedError

    def get_image_repo_name(self, image):
        prefix = []
        if self._docker_registry is not None:
            prefix.append(self._docker_registry)
        else:
            prefix.append('localhost')
        if self._docker_namespace is not None:
            prefix.append(self._docker_namespace)
        prefix = '/'.join(prefix)

        if isinstance(image, BaseImage):
            return (
                f'{prefix}'
                f'/{self._project}'
                f'/{self._user}'
                f'/{image.name}'
            )
        elif isinstance(image, ExecutionImage):
            return (
                f'{prefix}'
                f'/{self._project}'
                f'/{self._user}'
                f'/{image.name}'
                f'.{self.name}'
            )

    def build_image(self, path, image, build_id):
        repo = self.get_image_repo_name(image)
        ref = ':'.join([repo, build_id])
        command = [
            'docker',
            'build',
            '-t', ref,
            path,
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
                f'Build exited with status code {result.returncode}.')
        return ref

    def submit(self, pipeline, image_refs, run_id):
        raise NotImplementedError
