import os


class ExecutionHandler:
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
                f'Cannot handle artifact {artifact_location.artifact.name} '
                f'of kind {artifact_location.artifact.kind}'
            )
        if artifact_location.artifact.scope != 'run':
            raise RuntimeError(
                f'Cannot handle artifact {artifact_location.artifact.name} '
                f'with scope {artifact_location.artifact.scope}'
            )
        return os.path.join('/root/artifacts', artifact_location.artifact.name)

    def clear_artifact(self, artifact):
        pass

    def sync_artifact_location(self, artifact_location):
        print(f'Skipping sync of artifact {artifact_location.artifact.name}.')

    def get_worker_index(self):
        return os.environ['CSUB_WORKER_INDEX']


def create_execution_handler():
    return ExecutionHandler()
