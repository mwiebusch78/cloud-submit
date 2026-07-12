import os
import json
import subprocess

from .base_execution_handler import BaseExecutionHandler
from .utils import CloudSubmitError, read_json, run_command
from .s3_tools import get_remote_artifact_path


class ExecutionHandler(BaseExecutionHandler):
    def __init__(self):
        super().__init__()
        self._run_id = self.get_run_id()
        config = read_json('src/csub/execution_config.json')
        self._project = config['project']
        self._user = config['user']
        self._container_aws_command = config['container_aws_command']
        self._s3_bucket = config['s3_bucket']
        self._s3_prefix = config['s3_prefix']

    def get_remote_artifact_path(self, artifact):
        return get_remote_artifact_path(
            artifact,
            self._project,
            self._user,
            self._run_id,
            self._s3_bucket,
            self._s3_prefix,
        )

    def download_artifact(self, artifact):
        if artifact.kind != 'file':
            raise CloudSubmitError(
                f'Artifact {artifact.name} is of kind {repr(artifact.kind)} '
                "but only artifacts of kind 'file' are supported by this "
                'environment.'
            )
        remote_path = self.get_remote_artifact_path(artifact)
        remote_path = '/'.join(remote_path.split('/')[:-1])
        local_path = self.get_local_artifact_path(artifact)
        local_path = os.path.dirname(local_path)

        print(f'Downloading artifact {artifact.name}.')
        command = [
            self._container_aws_command,
            's3',
            'cp',
            '--recursive',
            '--no-progress',
            remote_path,
            local_path,
            '--exclude', '*',
            '--include', artifact.name,
            '--include', '/'.join([artifact.name, '*'])
        ]
        run_command(command)

    def upload_artifact(self, artifact):
        if artifact.kind != 'file':
            raise CloudSubmitError(
                f'Artifact {artifact.name} is of kind {repr(artifact.kind)} '
                "but only artifacts of kind 'file' are supported by this "
                'environment.'
            )
        remote_path = self.get_remote_artifact_path(artifact)
        remote_path = '/'.join(remote_path.split('/')[:-1])
        local_path = self.get_local_artifact_path(artifact)
        local_path = os.path.dirname(local_path)

        print(f'Uploading artifact {artifact.name}.')
        command = [
            self._container_aws_command,
            's3',
            'cp',
            '--recursive',
            '--no-progress',
            local_path,
            remote_path,
            '--exclude', '*',
            '--include', artifact.name,
            '--include', '/'.join([artifact.name, '*'])
        ]
        run_command(command)


def create_execution_handler():
    return ExecutionHandler()
