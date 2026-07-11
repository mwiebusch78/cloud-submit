import os

try:
    from .utils import CloudSubmitError
except ImportError:
    from cloud_submit import CloudSubmitError


def get_remote_artifact_path(
    artifact,
    project,
    user,
    run_id,
    s3_bucket,
    s3_prefix,
):
    if artifact.kind != 'file':
        raise CloudSubmitError(
            f'Cannot get remote path for artifact {artifact.name}. '
            "Only artifacts of kind 'file' are supported and this one "
            f'is of kind {repr(artifact.kind)}.'
        )
    if artifact.scope == 'project':
        return '/'.join([
            's3:/', s3_bucket, s3_prefix, project,
            'shared', artifact.name
        ])
    elif artifact.scope == 'user':
        return '/'.join([
            's3:/', s3_bucket, s3_prefix, project,
            'users', user, 'shared', artifact.name
        ])
    elif artifact.scope == 'run':
        if run_id is None:
            raise ValueError(
                'You must specify `run_id` to get the path '
                "for an artifact with scope 'run'"
            )
        return '/'.join([
            's3:/', s3_bucket, s3_prefix, project,
            'users', user, 'runs', run_id, artifact.name
        ])
    else:
        raise CloudSubmitError(
            f'Unknown scope {artifact.scope} for artifact {artifact.name}.')

