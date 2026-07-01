import os
import shutil
import shlex
import subprocess
import datetime as dt
import re


class CloudSubmitError(Exception):
    pass


def clear_path(path):
    shutil.rmtree(path, ignore_errors=True)
    try:
        os.remove(path)
    except FileNotFoundError:
        pass


def ensure_path(path, clear=False):
    if clear:
        clear_path(path)
    os.makedirs(path, exist_ok=True)


def build_docker_mount_option(source, dest):
    source = os.path.abspath(source)
    volume = os.environ.get('CSUB_DOD_VOLUME', None)
    if not volume:
        return f'type=bind,src={source},dst={dest}'
    dod_mount = os.environ.get('CSUB_DOD_MOUNT_POINT', None)
    if not dod_mount:
        raise CloudSubmitError(
            'CSUB_DOD_MOUNT_POINT variable must be set to use cloud-submit '
            'in a docker-outside-docker setup. If do not want to use '
            'cloud-submit in docker-outside-docker mode make sure that the '
            'variable CSUB_DOD_VOLUME is *not* set.'
        )
    dod_mount = os.path.abspath(dod_mount)
    if os.path.commonpath([source, dod_mount]) != dod_mount:
        raise CloudSubmitError(
            f'Artifact directory {path} is not a subpath of {dod_mount}. '
            'Change the CSUB_DOD_MOUNT_POINT variable or move your '
            'project directory.'
        )
    subpath = os.path.relpath(source, start=dod_mount)
    return (
        'type=volume,'
        f'src={volume},'
        f'dst={dest},'
        f'volume-subpath={subpath}'
    )


def run_command(command, check=True, **kwargs):
    try:
        result = subprocess.run(command, **kwargs)
    except FileNotFoundError:
        raise CloudSubmitError(
            'Error. Command not found when trying to execute:\n'
            + ' '.join([shlex.quote(part) for part in command])
        )
    except KeyboardInterrupt:
        raise CloudSubmitError('Aborted on user request.')
    if check and result.returncode != 0:
        msg = (
            f'Command exited with status code {result.returncode}:\n'
            + ' '.join([shlex.quote(part) for part in command])
        )
        raise CloudSubmitError(msg)
    return result


_IMAGE_REF_REGEX = re.compile(
    '^([a-zA-Z0-9-.:]+)/([a-z0-9-_./]+):([a-zA-Z0-9-_.]+)(@([a-zA-Z0-9:]+))?$'
)


def parse_image_ref(ref):
    match = _IMAGE_REF_REGEX.match(ref)
    if match is None:
        raise CloudSubmitError(f'Could not parse image ref {ref}')
    return (
        match.group(1),
        match.group(2),
        match.group(3),
        match.group(5),
    )
