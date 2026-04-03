import sys
import click
from . import lock
from . import build
from . import run_local
from . import utils


@click.group()
@click.option(
    '--product-root', '-p',
    type=click.Path(exists=True, file_okay=False),
    default=None,
    help='Path to the product root directory. Defaults to current directory.',
)
@click.pass_context
def main(ctx, product_root):
    ctx.ensure_object(dict)
    ctx.obj['product_root'] = product_root


@main.command(
    name='lock',
    help='Lock the python environment of a service.',
)
@click.argument('service', type=str)
@click.pass_context
def lock_cmd(ctx, service):
    try:
        lock.lock(service, product_root=ctx.obj['product_root'])
    except utils.CloudSubmitError as e:
        sys.stderr.write(str(e))
        sys.stderr.write('\n')
        sys.exit(1)


@main.command(
    name='build',
    help='Build the docker image for a service.',
)
@click.argument('service', type=str)
@click.option(
    '--build-id', '-b',
    type=str,
    default=None,
    help=(
        'The ID for the build. This will be used as the image tag. '
        'Defaults to the current timestamp in YYYYMMDD-HHMMSS format.'
    ),
)
@click.pass_context
def build_cmd(ctx, service, build_id):
    try:
        build.build(
            service,
            build_id=build_id,
            product_root=ctx.obj['product_root'],
        )
    except utils.CloudSubmitError as e:
        sys.stderr.write(str(e))
        sys.stderr.write('\n')
        sys.exit(1)


@main.command(
    name='run-local',
    help='Build the docker image and run the service locally.',
)
@click.argument('service', type=str)
@click.option(
    '--build-id', '-b',
    type=str,
    default=None,
    help=(
        'The ID for the build. This will be used as the image tag. '
        'Same as the run ID if not specified.'
    ),
)
@click.option(
    '--run-id', '-r',
    type=str,
    default=None,
    help=(
        'The ID for the run. This will be used in the folder name for the '
        'run artifacts. Defaults to the current timestamp in '
        'YYYYMMDD-HHMMSS format.'
    ),
)
@click.pass_context
def run_local_cmd(ctx, service, build_id, run_id):
    try:
        run_local.run_local(
            service,
            build_id=build_id,
            run_id=run_id,
            product_root=ctx.obj['product_root'],
        )
    except utils.CloudSubmitError as e:
        sys.stderr.write(str(e))
        sys.stderr.write('\n')
        sys.exit(1)
