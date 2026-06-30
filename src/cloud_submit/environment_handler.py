import os
import sys
import datetime as dt
import uuid
import subprocess
import glob

from .utils import CloudSubmitError, run_command, ensure_path, clear_path
from .images import BaseImage, ExecutionImage
from .execution.config import to_utc


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

    def _get_image_ref_path(self, image):
        if isinstance(image, BaseImage):
            image_name = image.name
        elif isinstance(image, ExecutionImage):
            image_name = '.'.join([image.name, self.name])
        else:
            raise ValueError(f'Cannot handle image of type {type(image)}')

        return os.path.join('images', self._user, image_name)

    def get_image_ref(self, image):
        ensure_path(os.path.join('images', self._user))
        path = self._get_image_ref_path(image)
        try:
            with open(path, 'r') as stream:
                ref = stream.read().strip()
        except FileNotFoundError:
            return None
        return ref

    def save_image_ref(self, image, ref):
        ensure_path(os.path.join('images', self._user))
        path = self._get_image_ref_path(image)
        with open(path, 'w') as stream:
            stream.write(ref)

    def clear_image_ref(self, image):
        ensure_path(os.path.join('images', self._user))
        path = self._get_image_ref_path(image)
        try:
            os.remove(path)
        except FileNotFoundError:
            pass

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
        run_command([
            self._docker_command,
            'pull',
            ref,
        ])

    def list_local_image_tags(self, repo_name):
        result = run_command(
            [
                self._docker_command,
                'image',
                'list',
                '--format', '{{.Tag}}',
                repo_name,
            ],
            stdout=subprocess.PIPE,
            text=True,
        )
        result = result.stdout.strip()
        if not result:
            return []
        return result.split('\n')

    def list_remote_image_tags(self, repo_name):
        raise NotImplementedError

    def remove_local_image_refs(self, refs):
        run_command([
            self._docker_command,
            'image',
            'remove',
            *refs,
        ])

    def remove_remote_image_refs(self, refs):
        raise NotImplementedError

    def build_image(self, path, image, build_id):
        repo = self.get_image_repo_name(image)
        ref = ':'.join([repo, build_id])
        run_command([
            self._docker_command,
            'build',
            '-t', ref,
            path,
        ])
        return ref

    def get_local_artifact_path(self, artifact, run_id=None):
        if artifact.kind != 'file':
            raise CloudSubmitError(
                f'Cannot get path for local artifact {artifact.name}. '
                "Only artifacts of kind 'file' are supported and this one "
                f'is of kind {repr(artifact.kind)}.'
            )
        if artifact.scope == 'project':
            return os.path.join('artifacts', 'shared', artifact.name)
        elif artifact.scope == 'user':
            return os.path.join(
                'artifacts', 'users', self._user, 'shared', artifact.name)
        elif artifact.scope == 'run':
            if run_id is None:
                raise ValueError(
                    'You must specify `run_id` to get the path '
                    "for an artifact with scope 'run'"
                )
            return os.path.join(
                'artifacts', 'users', self._user, 'runs', run_id, artifact.name)
        else:
            raise CloudSubmitError(
                f'Unknown scope {artifact.scope} for artifact {artifact.name}.')

    def get_remote_artifact_path(self, artifact, run_id=None):
        raise NotImplementedError

    def list_local_artifacts(self, artifacts, run_ids=None):
        if run_ids is not None:
            run_ids = set(run_ids)

        results = []
        for artifact in artifacts:
            if artifact.kind != 'file':
                raise CloudSubmitError(
                    f'Cannot list run IDs for local artifact {artifact.name}. '
                    "Only artifacts of kind 'file' are supported and this one "
                    f'is of kind {repr(artifact.kind)}.'
                )
            if artifact.scope == 'run':
                pattern = os.path.join(
                    'artifacts', 'users', self._user,
                    'runs', '*', artifact.name,
                )
                files = glob.glob(pattern)
                ids = [os.path.basename(os.path.dirname(f)) for f in files]
                if run_ids is not None:
                    ids = [i for i in ids if i in run_ids]
            elif artifact.scope == 'user':
                pattern = os.path.join(
                    'artifacts', 'users', self._user, 'shared', artifact.name)
                files = glob.glob(pattern)
                if files:
                    ids = [None]
                else:
                    ids = []
            elif artifact.scope == 'project':
                pattern = os.path.join(
                    'artifacts', 'shared', artifact.name)
                files = glob.glob(pattern)
                if files:
                    ids = [None]
                else:
                    ids = []
            results.append(ids)
        return results

    def list_remote_artifacts(self, artifacts, run_ids=None):
        raise NotImplementedError

    def push_artifact(self, artifact, run_id):
        raise NotImplementedError

    def pull_artifact(self, artifact, run_id):
        raise NotImplementedError

    def remove_local_artifact(self, artifact, run_id):
        path = self.get_local_artifact_path(artifact, run_id)
        clear_path(path)

    def remove_remote_artifact(self, artifact, run_id=None):
        raise NotImplementedError

    def run_pipeline(self, pipeline, image_refs, timestamp, run_id):
        raise NotImplementedError
