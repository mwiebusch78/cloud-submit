from .base_execution_handler import BaseExecutionHandler
from .utils import read_json
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

def create_execution_handler():
    return ExecutionHandler()
