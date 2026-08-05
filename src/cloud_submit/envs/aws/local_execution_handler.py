import os

from .base_execution_handler import BaseExecutionHandler
from .s3_tools import get_remote_artifact_ref
from .utils import CloudSubmitError, run_command


class ExecutionHandler(BaseExecutionHandler):
    def __init__(self):
        super().__init__()
        self._run_id = self.get_run_id()
        self._project = os.environ['CSUB_PROJECT']
        self._user = os.environ['CSUB_USER']
        self._aws_region = os.environ['CSUB_AWS_REGION']
        self._aws_profile = os.environ['CSUB_AWS_PROFILE']
        self._container_aws_command = os.environ['CSUB_AWS_COMMAND']
        self._s3_bucket = os.environ['CSUB_S3_BUCKET']
        self._s3_prefix = os.environ['CSUB_S3_PREFIX']

    def get_artifact_reference(self, reference_type, artifact):
        if reference_type == 'uri':
            return get_remote_artifact_ref(
                artifact,
                self._project,
                self._user,
                self._run_id,
                self._s3_bucket,
                self._s3_prefix,
            )
        return super().get_artifact_reference(reference_type, artifact)

    def download_artifact(self, artifact):
        if artifact.kind != 'file':
            raise CloudSubmitError(
                f'Artifact {artifact.name} is of kind {repr(artifact.kind)} '
                "but only artifacts of kind 'file' are supported by this "
                'environment.'
            )
        remote_path = self.get_artifact_reference('uri', artifact)
        remote_path = '/'.join(remote_path.split('/')[:-1])
        local_path = self.get_artifact_reference('path', artifact)
        local_path = os.path.dirname(local_path)

        print(f'Downloading artifact {artifact.name}.')
        command = [
            self._container_aws_command,
            's3',
            'cp',
            '--profile', self._aws_profile,
            '--region', self._aws_region,
            '--recursive',
            '--no-progress',
            remote_path,
            local_path,
            '--exclude', '*',
            '--include', artifact.name,
            '--include', '/'.join([artifact.name, '*'])
        ]
        run_command(command)


def create_execution_handler():
    return ExecutionHandler()
