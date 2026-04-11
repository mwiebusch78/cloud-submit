import os
import shutil


class BaseExecutionHandler:
    def __init__(self):
        pass

    def get_artifact_location(self, artifact_location):
        if artifact_location.location_type != 'local':
            raise RuntimeError(
                'Cannot construct location for non-local artifact '
                f'{artifact_location.artifact.name}.'
            )
        if artifact_location.artifact.kind != 'file':
            raise RuntimeError(
                f'Cannot construct location for artifact '
                f'{artifact_location.artifact.name} '
                f'of kind {artifact_location.artifact.kind}'
            )
        if artifact_location.artifact.scope not in ['run', 'user', 'project']:
            raise RuntimeError(
                f'Invalid scope {artifact_location.artifact.scope} '
                f'for artifact {artifact_location.artifact.name} '
            )
        return os.path.join(
            '/root/artifacts',
            artifact_location.artifact.scope,
            artifact_location.artifact.name,
        )

    def clear_artifact(self, artifact):
        if artifact.kind != 'file':
            raise RuntimeError(
                f'Cannot clear artifact {artifact.name} '
                f'of kind {artifact.kind}'
            )
        path = self.get_artifact_location(artifact.local)
        shutil.rmtree(path, ignore_errors=True)
        try:
            os.remove(path)
        except FileNotFoundError:
            pass

    def sync_artifact_location(self, artifact_location):
        print(f'Skipping sync of artifact {artifact_location.artifact.name}.')

    def get_worker_index(self):
        return os.environ['CSUB_WORKER_INDEX']
