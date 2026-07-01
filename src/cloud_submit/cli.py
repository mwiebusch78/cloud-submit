import os
import sys
import datetime as dt

import click
import yaml

from .utils import CloudSubmitError
from .controller import Controller


def abort(msg):
    sys.stderr.write(msg)
    sys.stderr.write('\n')
    sys.exit(1)


def init(ctx):
    ctx.ensure_object(dict)
    if ctx.obj['controller'] is not None:
        return
    project_root = os.path.abspath(ctx.obj['project_root'])
    ctx.obj['project_root'] = project_root
    user = ctx.obj['user']

    sys.path.insert(0, os.path.join(project_root, 'src'))
    userconfig_path = os.path.join(project_root, 'userconfig', f'{user}.yaml')
    try:
        stream = open(userconfig_path, 'r')
    except FileNotFoundError:
        abort(
            f'Could not find user config at {userconfig_path}. Are you in the '
            'project root directory? When working outside the project root '
            'directory you must specify it with -p.'
        )
    try:
        userconfig = yaml.safe_load(stream)
    except yaml.YAMLError as e:
        abort(f'Error reading user config at {userconfig_path}: {str(e)}')
    finally:
        stream.close()

    from csub.build_config import build_config
    csubconfig = build_config(project_root, userconfig)
    controller = Controller(csubconfig)
    ctx.obj['config'] = csubconfig
    ctx.obj['controller'] = controller


def tabulate(data, header=None):
    if not data:
        return ''
    if header is not None:
        data = [header] + data
    data = [
        [('' if cell is None else cell) for cell in row]
        for row in data
    ]
    maxlen = None
    for row in data:
        if maxlen is None:
            maxlen = [len(cell) for cell in row]
        elif len(row) == len(maxlen):
            maxlen = [max(len(cell), l) for cell, l in zip(row, maxlen)]
        else:
            raise ValueError('Inconsistent element count in rows.')

    result = []
    for row in data:
        output_row = []
        for cell, l in zip(row, maxlen):
            output_row.append(cell.ljust(l))
        result.append(' '.join(output_row))
    return '\n'.join(result)


def get_steps(pipeline, steps_arg):
    if steps_arg is None:
        return None

    tokens = steps_arg.split(',')
    all_steps = [step.name for step in pipeline.steps]
    indices = {step: i for i, step in enumerate(all_steps)}
    
    selected = set()
    for token in tokens:
        token = token.split(':')
        if len(token) == 1:
            if token[0] not in indices:
                raise CloudSubmitError(f'Invalid step name: {token[0]}')
            selected.add(token[0])
        elif len(token) == 2:
            try:
                begin = indices[token[0]]
            except KeyError:
                raise CloudSubmitError(f'Invalid step name: {token[0]}')
            try:
                end = indices[token[1]]
            except KeyError:
                raise CloudSubmitError(f'Invalid step name: {token[1]}')
            for step in all_steps[beg:end+1]:
                selected.add(step)
        else:
            raise CloudSubmitError(f'Invalid step range: {":".join(token)}')

    return [step for step in all_steps if step in selected]


@click.group()
@click.option(
    '--project-root', '-p',
    type=click.Path(exists=True, file_okay=False),
    default='.',
    help='Path to the project root directory. Defaults to current directory.',
)
@click.option(
    '--user', '-u',
    type=str,
    default='default',
    help='Path to the project root directory. Defaults to current directory.',
)
@click.pass_context
def main(ctx, project_root, user):
    ctx.ensure_object(dict)
    ctx.obj['project_root'] = project_root
    ctx.obj['user'] = user
    ctx.obj['controller'] = None
    ctx.obj['config'] = None


# pipelines subcommand


@main.group('pipelines', help='View and execute pipelines.')
def pipelines():
    pass


@pipelines.command(
    name='list',
    help="""List available pipelines and their steps.

If one or more PIPELINES arguments are given those pipelines will be 
listed (if they exist). Otherwise all available pipelines are listed.
""",
)
@click.option(
    '--steps', '-s',
    is_flag=True,
    help='Also list the steps of each pipeline.'
)
@click.argument(
    'pipelines',
    type=str,
    nargs=-1,
)
@click.pass_context
def list_pipelines(ctx, steps, pipelines):
    init(ctx)
    config = ctx.obj['config']
    if pipelines:
        pipelines = set(pipelines)
    else:
        pipelines = None
    for pipeline in config.pipelines.values():
        if pipelines is None or pipeline.name in pipelines:
            print(pipeline.name)
            if steps:
                for step in pipeline.steps:
                    print('    ' + step.name)


@pipelines.command(
    name='run',
    help="""Run a pipeline (or a part of it).

Run the pipeline PIPELINE. Specify STEPS to select a subset of steps to run.
STEPS should be a comma-separated list of step names or step ranges of the form
start:end where start and end are step names. If steps is not specified all
steps are executed. By default, all *execution* images needed for the run are
re-built (but you can use --no-rebuild to prevent that).
"""
)
@click.option(
    '--env', '-e',
    type=str,
    default=None,
    help='The name of the environment to use for the run.',
)
@click.option(
    '--build-env',
    type=str,
    default=None,
    help='The name of the environment to use for building images.',
)
@click.option(
    '--run-id', '-r',
    type=str,
    default=None,
    help=(
        'The ID for the run. This will be used, for example, to uniquely name '
        'artifacts. Defaults to the current timestamp in YYYYMMDD-HHMMSS-XXXX '
        'format, where XXXX is a four-character UUID.'
    ),
)
@click.option(
    '--timestamp', '-t',
    type=str,
    default=None,
    help=(
        'The timestamp used as the submit time of the pipeline. '
        'If not specified the default submit timestamp of the pipeline is '
        'used. If that is not specified either the current time is used. '
        'You can use this option to simulate runs submitted at a different '
        'time. The value must be in ISO format (YYYY-MM-DDTHH:MM:SS). '
        'You can also use "-t now" to use the current time irrespective of '
        'the pipeline default.'
    ),
)
@click.argument(
    'pipeline',
    type=str,
    required=True,
)
@click.argument(
    'steps',
    type=str,
    default=None,
)
@click.pass_context
def run(ctx, env, build_env, run_id, timestamp, pipeline, steps):
    init(ctx)
    config = ctx.obj['config']
    controller = ctx.obj['controller']

    try:
        pipeline = config.pipelines[pipeline]
    except KeyError:
        abort(f'Pipeline not found: {pipeline}')

    if timestamp is not None and timestamp != 'now':
        timestamp = dt.datetime.fromisoformat(timestamp)

    try:
        steps = get_steps(pipeline, steps)
        controller.run(
            pipeline.name,
            steps=steps,
            run_id=run_id,
            timestamp=timestamp,
            env=env,
            build_env=build_env,
        )
    except CloudSubmitError as e:
        abort(str(e))


# images subgroup


@main.group('images', help='Manage docker images.')
def images():
    pass


@images.command(
    name='build',
    help='Build one or more docker images.',
)
@click.option(
    '--env', '-e',
    type=str,
    default=None,
    help='The name of the build environment to use.',
)
@click.option(
    '--build-id', '-b',
    type=str,
    default=None,
    help=(
        'The ID for the build. This will be used as the image tag. '
        'Defaults to the current timestamp in YYYYMMDD-HHMMSS-XXXX format, '
        'where XXXX is a four-character UUID.'
    ),
)
@click.option(
    '--all', '-a', 'build_all',
    is_flag=True,
    help='Build all images.',
)
@click.argument('images', type=str, nargs=-1)
@click.pass_context
def build(ctx, env, build_id, build_all, images):
    init(ctx)
    config = ctx.obj['config']
    controller = ctx.obj['controller']

    for image in images:
        if image not in config.images:
            abort(f'No definition found for image {image}.')
    if build_all:
        images = list(config.images.keys())
    try:
        controller.build(images, build_id=build_id, env=env)
    except CloudSubmitError as e:
        abort(str(e))


@images.command(
    name='list',
    help='List the images associated with the project.',
)
@click.option(
    '--env', '-e',
    type=str,
    default=None,
    help='The name of the environment to use for listing images.',
)
@click.option(
    '--images', '-i',
    type=str,
    default=None,
    help=(
        'Comma-separated list of names for the images to show. '
        'If absent, all declared images are shown.'
    ),
)
@click.option(
    '--local', '-l',
    is_flag=True,
    help='Show local image refs.',
)
@click.option(
    '--remote', '-r',
    is_flag=True,
    help='Show image refs in remote registry.',
)
@click.option(
    '--build-ids', '-b',
    type=str,
    default=None,
    help=(
        'Comma-separated list of build IDs. Only images '
        'with the given build IDs are shown.'
    ),
)
@click.pass_context
def list_images(ctx, env, images, local, remote, build_ids):
    if local and remote:
        abort('The flags --local and --remote are mutually exclusive.')
    init(ctx)
    config = ctx.obj['config']
    controller = ctx.obj['controller']

    if images is not None:
        images = list(set(images.split(',')))
    if build_ids is not None:
        build_ids = list(set(build_ids.split(',')))

    if local or remote:
        try:
            results = controller.list_image_refs(
                images=images,
                ids=build_ids,
                env=env,
                remote=remote,
            )
        except CloudSubmitError as e:
            abort(str(e))
        for ref in results:
            print(ref)
    else:
        data = controller.list_images(images=images)
        data = [
            (name, tpe, ('' if ref is None else ref.split('@')[0]))
            for name, tpe, ref in data
        ]
        table = tabulate(data, header=['NAME', 'TYPE', 'REF'])
        if table:
            print(table)


@images.command(
    name='remove',
    help='Remove images associated with the project.',
)
@click.option(
    '--env', '-e',
    type=str,
    default=None,
    help='The name of the environment to use for listing images.',
)
@click.option(
    '--images', '-i',
    type=str,
    default=None,
    help=(
        'Comma-separated list of names for the images to remove. '
        'If absent, all declared images will be removed.'
    ),
)
@click.option(
    '--local', '-l',
    is_flag=True,
    help='Remove images only locally.',
)
@click.option(
    '--remote', '-r',
    is_flag=True,
    help='Remove images only from remote registry.',
)
@click.option(
    '--build-ids', '-b',
    type=str,
    default=None,
    help=(
        'Comma-separated list of build IDs. Only images '
        'with the given build IDs are removed.'
    ),
)
@click.pass_context
def remove_images(ctx, env, images, local, remote, build_ids):
    if local and remote:
        abort('The flags --local and --remote are mutually exclusive.')
    init(ctx)
    config = ctx.obj['config']
    controller = ctx.obj['controller']

    remove_local = True
    remove_remote = True
    if local:
        remove_remote = False
    if remote:
        remove_local = False

    if images is not None:
        images = list(set(images.split(',')))
    if build_ids is not None:
        build_ids = list(set(build_ids.split(',')))

    if remove_local:
        try:
            refs = controller.list_image_refs(
                images=images,
                ids=build_ids,
                env=env,
                remote=False,
            )
            controller.remove_image_refs(refs, remote=False, env=env)
        except CloudSubmitError as e:
            abort(str(e))
    if remove_remote:
        try:
            refs = controller.list_image_refs(
                images=images,
                ids=build_ids,
                env=env,
                remote=True,
            )
            controller.remove_image_refs(refs, remote=True, env=env)
        except CloudSubmitError as e:
            abort(str(e))


@images.command(
    name='set',
    help='Set the image reference for image IMAGE to REF.'
)
@click.option(
    '--env', '-e',
    type=str,
    default=None,
    help='The name of the environment to use.',
)
@click.argument('image')
@click.argument('ref')
@click.pass_context
def set_image(ctx, env, image, ref):
    init(ctx)
    controller = ctx.obj['controller']
    controller.set_image(image, ref, env=env)


@images.command(
    name='unset',
    help='Clear the image references for IMAGES.'
)
@click.option(
    '--env', '-e',
    type=str,
    default=None,
    help='The name of the environment to use.',
)
@click.option(
    '--all', '-a', 'unset_all',
    is_flag=True,
    help='Clear all image references.',
)
@click.argument('images', type=str, nargs=-1)
@click.pass_context
def unset_image(ctx, env, unset_all, images):
    init(ctx)
    config = ctx.obj['config']
    controller = ctx.obj['controller']
    if unset_all:
        images = list(config.images.keys())
    for image in images:
        controller.unset_image(image, env=env)


# artifacts subgroup


@main.group('artifacts', help='Manage artifacts.')
def artifacts():
    pass


@artifacts.command(
    name='list',
    help="""List local or remote artifacts.

If neither --local nor --remote are specified this just lists the declared
artifacts for the project. Otherwise it lists the artifacts that are stored
locally or remotely (from different runs).
""",
)
@click.option(
    '--env', '-e',
    type=str,
    default=None,
    help='The name of the environment to use for listing artifacts.',
)
@click.option(
    '--artifacts', '-a',
    type=str,
    default=None,
    help=(
        'Comma-separated list of names for the artifacts to show. '
        'If absent, all declared artifacts are shown.'
    ),
)
@click.option(
    '--local',
    is_flag=True,
    help='Show local artifact paths.',
)
@click.option(
    '--remote',
    is_flag=True,
    help='Show remote artifact paths.',
)
@click.option(
    '--run-ids', '-r',
    type=str,
    default=None,
    help=(
        'Comma-separated list of run IDs. Only artifacts associated with '
        'the given runs are shown.'
    ),
)
@click.pass_context
def list_artifacts(ctx, env, artifacts, local, remote, run_ids):
    if local and remote:
        abort('The flags --local and --remote are mutually exclusive.')
    init(ctx)
    config = ctx.obj['config']
    controller = ctx.obj['controller']

    if artifacts is not None:
        artifacts = sorted(set(artifacts.split(',')))
    if run_ids is not None:
        run_ids = sorted(set(run_ids.split(',')))

    if local or remote:
        try:
            env_handler = config.get_run_env(env)
            artifact_names, run_id_lists = controller.list_artifacts(
                artifact_names=artifacts,
                run_ids=run_ids,
                env=env,
                remote=remote,
            )
            data = []
            for name, runs in zip(artifact_names, run_id_lists):
                if name not in config.artifacts:
                    continue
                artifact = config.artifacts[name]
                for run_id in runs:
                    if local:
                        path = env_handler.get_local_artifact_path(
                            artifact, run_id)
                    else:
                        path = env_handler.get_remote_artifact_path(
                            artifact, run_id)
                    data.append((
                        name,
                        artifact.kind,
                        artifact.scope,
                        run_id,
                        path,
                    ))
        except CloudSubmitError as e:
            abort(str(e))
        table = tabulate(
            data,
            header=['NAME', 'KIND', 'SCOPE', 'RUN_ID', 'PATH'],
        )
        if table:
            print(table)
    else:
        data = [
            (a.name, a.kind, a.scope)
            for a in config.artifacts.values()
            if artifacts is None or a.name in artifacts
        ]
        table = tabulate(data, header=['NAME', 'KIND', 'SCOPE'])
        if table:
            print(table)


@artifacts.command(
    name='remove',
    help='Remove artifacts.',
)
@click.option(
    '--env', '-e',
    type=str,
    default=None,
    help='The name of the environment to use for removing artifacts.',
)
@click.option(
    '--artifacts', '-a', 'artifact_names',
    type=str,
    default=None,
    help=(
        'Comma-separated list of names for the artifacts to remove. '
        'If absent, all declared artifacts will be removed.'
    ),
)
@click.option(
    '--local',
    is_flag=True,
    help='Remove artifacts only locally.',
)
@click.option(
    '--remote',
    is_flag=True,
    help='Remove artifacts only from remote storage.',
)
@click.option(
    '--run-ids', '-r',
    type=str,
    default=None,
    help=(
        'Comma-separated list of run IDs. Only artifacts '
        'from the given runs are removed.'
    ),
)
@click.pass_context
def remove_artifacts(ctx, env, artifact_names, local, remote, run_ids):
    if local and remote:
        abort('The flags --local and --remote are mutually exclusive.')
    init(ctx)
    config = ctx.obj['config']
    controller = ctx.obj['controller']

    remove_local = True
    remove_remote = True
    if local:
        remove_remote = False
    if remote:
        remove_local = False

    if artifact_names is not None:
        artifact_names = list(set(artifact_names.split(',')))
    if run_ids is not None:
        run_ids = list(set(run_ids.split(',')))

    with config.in_project_root():
        if remove_local:
            try:
                artifact_names, run_id_lists = controller.list_artifacts(
                    artifact_names=artifact_names,
                    run_ids=run_ids,
                    env=env,
                    remote=False,
                )
                controller.remove_artifacts(
                    artifact_names, run_id_lists, remote=False, env=env)
            except CloudSubmitError as e:
                abort(str(e))
        if remove_remote:
            try:
                artifact_names, run_id_lists = controller.list_artifacts(
                    artifact_names=artifact_names,
                    run_ids=run_ids,
                    env=env,
                    remote=True,
                )
                controller.remove_artifacts(
                    artifact_names, run_id_lists, remote=True, env=env)
            except CloudSubmitError as e:
                abort(str(e))
