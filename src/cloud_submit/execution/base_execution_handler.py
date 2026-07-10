import os
import shutil
import datetime as dt


class BaseExecutionHandler:
    def __init__(self):
        pass

    def get_local_artifact_path(self, artifact):
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

    def get_remote_artifact_path(self, artifact):
        raise RuntimeError(
            'Cannot construct remote path for artifact '
            f'{artifact.name}.'
        )

    def clear_artifact(self, artifact):
        if artifact.kind != 'file':
            raise RuntimeError(
                f'Cannot clear artifact {artifact.name} '
                f'of kind {artifact.kind}'
            )
        path = self.get_local_artifact_path(artifact)
        shutil.rmtree(path, ignore_errors=True)
        try:
            os.remove(path)
        except FileNotFoundError:
            pass

    def download_artifact(self, artifact):
        print(f'Skipping download of artifact {artifact.name}.')

    def upload_artifact(self, artifact):
        print(f'Skipping upload of artifact {artifact.name}.')

    def get_submit_timestamp(self):
        return dt.datetime.fromisoformat(os.environ['CSUB_TIMESTAMP'])

    def get_run_id(self):
        return os.environ['CSUB_RUN_ID']

    def get_worker_index(self):
        return int(os.environ['CSUB_WORKER_INDEX'])
