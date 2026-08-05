import os
import json
import subprocess

from .base_execution_handler import BaseExecutionHandler
from .utils import CloudSubmitError, run_command
from .s3_tools import get_remote_artifact_path


class ExecutionHandler(BaseExecutionHandler):
    def __init__(self):
        super().__init__()
        self._run_id = self.get_run_id()
        self._project = os.environ['CSUB_PROJECT']
        self._user = os.environ['CSUB_USER']
        self._aws_command = os.environ['CSUB_AWS_COMMAND']
        self._s3_bucket = os.environ['CSUB_S3_BUCKET']
        self._s3_prefix = os.environ['CSUB_S3_PREFIX']

    def get_artifact_location(self, location_type, artifact):
        if location_type == 'uri':
            return get_remote_artifact_path(
                artifact,
                self._project,
                self._user,
                self._run_id,
                self._s3_bucket,
                self._s3_prefix,
            )
        return super().get_artifact_location(location_type, artifact)

    def download_artifact(self, artifact):
        if artifact.kind != 'file':
            raise CloudSubmitError(
                f'Artifact {artifact.name} is of kind {repr(artifact.kind)} '
                "but only artifacts of kind 'file' are supported by this "
                'environment.'
            )
        remote_path = self.get_artifact_location('uri', artifact)
        remote_path = '/'.join(remote_path.split('/')[:-1])
        local_path = self.get_artifact_location('path', artifact)
        local_path = os.path.dirname(local_path)

        print(f'Downloading artifact {artifact.name}.')
        command = [
            self._aws_command,
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
        remote_path = self.get_artifact_location('uri', artifact)
        remote_path = '/'.join(remote_path.split('/')[:-1])
        local_path = self.get_artifact_location('path', artifact)
        local_path = os.path.dirname(local_path)

        print(f'Uploading artifact {artifact.name}.')
        command = [
            self._aws_command,
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
