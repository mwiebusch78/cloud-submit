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
    project_root = os.path.abspath(project_root)
    ctx.obj['project_root'] = project_root

    sys.path.insert(0, os.path.join(project_root, 'src'))
    userconfig_path = os.path.join(project_root, 'userconfig', f'{user}.yaml')
    with open(userconfig_path, 'r') as stream:
        try:
            userconfig = yaml.safe_load(stream)
        except yaml.YAMLError as e:
            abort(f'Error reading user config at {userconfig_path}: {str(e)}')

    from csub.build_config import build_config
    csubconfig = build_config(userconfig)
    controller = Controller(csubconfig)
    ctx.obj['config'] = csubconfig
    ctx.obj['controller'] = controller

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
def build_cmd(ctx, env, build_id, build_all, images):
    ctx.ensure_object(dict)
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

