import os

try:
    from .utils import CloudSubmitError, clear_path, run_command
except ImportError:
    from cloud_submit import CloudSubmitError, clear_path, run_command


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


def remove_remote_artifacts(
    artifacts,
    run_ids,
    project,
    user,
    s3_bucket,
    s3_prefix,
    aws_profile,
    aws_region,
    aws_command,
):
    path = '/'.join(
        ['s3:/', s3_bucket, s3_prefix, project])
    command = [
        aws_command,
        's3',
        'rm',
    ]
    if aws_profile is not None:
        command.extend(['--profile', aws_profile])
    if aws_region is not None:
        command.extend(['--region', aws_region])
    command.extend([
        '--recursive',
        path,
        '--exclude', '*',
    ])
    for artifact, runs in zip(artifacts, run_ids):
        if artifact.kind != 'file':
            continue
        if artifact.scope == 'project':
            for run_id in runs:
                command.extend([
                    '--include',
                    '/'.join(['shared', artifact.name]),
                    '--include',
                    '/'.join(['shared', artifact.name, '*']),
                ])
        elif artifact.scope == 'user':
            for run_id in runs:
                command.extend([
                    '--include',
                    '/'.join([
                        'users', user, 'shared', artifact.name]),
                    '--include',
                    '/'.join([
                        'users', user, 'shared', artifact.name, '*']),
                ])
        elif artifact.scope == 'run':
            for run_id in runs:
                command.extend([
                    '--include',
                    '/'.join([
                        'users', user, 'runs', run_id,
                        artifact.name,
                    ]),
                    '--include',
                    '/'.join([
                        'users', user, 'runs', run_id,
                        artifact.name, '*',
                    ]),
                ])

    run_command(command)
