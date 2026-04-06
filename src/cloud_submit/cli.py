import os
import sys

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
    else:
        image_set = set(images)
        images = [
            image for image in config.images.keys()
            if image in image_set
        ]

    for image in images:
        try:
            controller.build(image, build_id=build_id, env=env)
        except CloudSubmitError as e:
            abort(str(e))


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
def submit(ctx, env, build_env, run_id, pipeline, steps):
    init(ctx)
    config = ctx.obj['config']
    controller = ctx.obj['controller']

    try:
        pipeline = config.pipelines[pipeline]
    except KeyError:
        abort(f'Pipeline not found: {pipeline}')

    try:
        steps = get_steps(pipeline, steps)
        controller.submit(
            pipeline.name,
            steps=steps,
            run_id=run_id,
            env=env,
            build_env=build_env,
        )
    except CloudSubmitError as e:
        abort(str(e))
