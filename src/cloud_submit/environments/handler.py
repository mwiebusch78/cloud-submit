import sys
import datetime as dt
import uuid
import subprocess

from ..utils import CloudSubmitError
from ..images import BaseImage, ExecutionImage
from ..execution.config import to_utc


class EnvironmentHandler:
    def __init__(
            self,
            name,
            project,
            user,
            docker_command='docker',
            docker_registry=None,
            docker_namespace='csub',
    ):
        self.name = name
        self._project = project
        self._user = user
        self._docker_command = str(docker_command)
        self._docker_registry = docker_registry
        self._docker_namespace = docker_namespace

    def _generate_id(self, timestamp, id):
        if id is not None:
            return id
        uid = str(uuid.uuid4())
        timestamp=to_utc(timestamp)
        return timestamp.strftime('%Y%m%d-%H%M%S-') + uid[:4]

    def generate_build_id(self, timestamp, build_id=None):
        return self._generate_id(timestamp, build_id)

    def generate_run_id(self, timestamp, run_id=None):
        return self._generate_id(timestamp, run_id)

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

    def pull_image(self, ref):
        command = [
            self._docker_command,
            'pull',
            ref,
        ]
        try:
            result = subprocess.run(command)
        except KeyboardInterrupt:
            raise CloudSubmitError('Aborted on user request.')
        if result.returncode != 0:
            raise CloudSubmitError(
                'docker pull command exited with status code '
                f'{result.returncode}.'
            )

    def list_local_image_tags(self, repo_name):
        command = [
            self._docker_command,
            'image',
            'list',
            '--format', '{{.Tag}}',
            repo_name,
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
                'docker image list exited with status code '
                f'{result.returncode}.'
            )
        result = result.stdout.decode('utf-8').strip()
        if not result:
            return []
        return result.split('\n')

    def list_remote_image_tags(self, repo_name):
        raise NotImplementedError

    def remove_local_image_refs(self, refs):
        command = [
            self._docker_command,
            'image',
            'remove',
            *refs,
        ]
        try:
            result = subprocess.run(command)
        except KeyboardInterrupt:
            raise CloudSubmitError('Aborted on user request.')
        if result.returncode != 0:
            raise CloudSubmitError(
                'docker image remove exited with status code '
                f'{result.returncode}.'
            )

    def remove_remote_image_refs(self, refs):
        raise NotImplementedError

    def build_image(self, path, image, build_id):
        repo = self.get_image_repo_name(image)
        ref = ':'.join([repo, build_id])
        command = [
            self._docker_command,
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

    def submit(self, pipeline, image_refs, timestamp, run_id):
        raise NotImplementedError
