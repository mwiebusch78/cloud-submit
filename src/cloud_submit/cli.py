import os
import sys
import datetime as dt

import click
import yaml

from .utils import CloudSubmitError
from .controller import Controller
from .images import BaseImage, ExecutionImage


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



@main.command(
    name='submit',
    help="""Run a pipeline (or a part of it).

Run the pipeline PIPELINE. Specify STEPS to select a subset of steps to run.
STEPS should be a comma-separated list of step names or step ranges of the form
start:end where start and end are step names. If steps is not specified all
steps are executed.
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
        'You can use this option to simulate run submitted at a different '
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
def submit(ctx, env, build_env, run_id, timestamp, pipeline, steps):
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
        controller.submit(
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
        data = []
        for i in config.images.values():
            if images is not None and i.name not in images:
                continue
            if isinstance(i, BaseImage):
                tpe = 'base'
            elif isinstance(i, ExecutionImage):
                tpe = 'execution'
            else:
                raise ValueError('Unknown image type.')
            data.append([i.name, tpe])
        table = tabulate(data, header=['NAME', 'TYPE'])
        if table:
            print(table)
