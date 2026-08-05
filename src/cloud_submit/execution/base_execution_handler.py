import os
import shutil
import datetime as dt


class BaseExecutionHandler:
    def __init__(self):
        pass

    def is_local_reference_type(self, reference_type):
        return reference_type == 'path'

    def get_artifact_reference(self, reference_type, artifact):
        if reference_type != 'path':
            raise RuntimeError(
                f'Cannot handle reference type {repr(reference_type)}.')
        if artifact.kind != 'file':
            raise RuntimeError(
                f'Cannot construct local path for artifact {artifact.name} '
                f'of kind {artifact.kind}'
            )
        if artifact.scope not in ['run', 'user', 'project']:
            raise RuntimeError(
                f'Invalid scope {artifact.scope} '
                f'for artifact {artifact.name} '
            )
        return os.path.join(
            '/mnt/artifacts',
            artifact.scope,
            artifact.name,
        )

    def download_artifact(self, artifact):
        pass

    def upload_artifact(self, artifact):
        pass

    def get_submit_timestamp(self):
        return dt.datetime.fromisoformat(os.environ['CSUB_TIMESTAMP'])

    def get_run_id(self):
        return os.environ['CSUB_RUN_ID']

    def get_worker_index(self):
        return int(os.environ['CSUB_WORKER_INDEX'])

    def get_run_steps(self):
        return os.environ['CSUB_RUN_STEPS'].split(',')
