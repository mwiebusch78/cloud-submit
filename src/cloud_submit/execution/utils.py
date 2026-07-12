import os
import sys
import shutil
import shlex
import json
import subprocess
import datetime as dt
import re


_timedelta_regex = re.compile(r'^(-?[\d]+)d([\d]+)s([\d]+)u$')


class DecodingError(Exception):
    pass


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


def _encode(obj):
    if isinstance(obj, dt.datetime):
        return {'$datetime': obj.isoformat()}
    elif isinstance(obj, dt.date):
        return {'$date': obj.isoformat()}
    elif isinstance(obj, dt.timedelta):
        return {'$timedelta': f'{obj.days}d{obj.seconds}s{obj.microseconds}u'}
    else:
        raise TypeError(f'Object of type {type(obj)} is not JSON serializable')


def _decode(obj):
    if len(obj) == 1:
        key, value = next(iter(obj.items()))
        if key == '$datetime':
            return dt.datetime.fromisoformat(value)
        elif key == '$date':
            return dt.date.fromisoformat(value)
        elif key == '$timedelta':
            match = _timedelta_regex.match(value)
            if not match:
                raise DecodingError(
                    f'Expecting timedelta expression but got {repr(value)}')
            return dt.timedelta(
                days=int(match.group(1)),
                seconds=int(match.group(2)),
                microseconds=int(match.group(3)),
            )
    return obj


def read_json(path):
    with open(path, 'r') as stream:
        obj = json.load(stream, object_hook=_decode)
    return obj


def write_json(obj, path):
    with open(path, 'w') as stream:
        json.dump(obj, stream, default=_encode, indent=4)


def run_command(command, check=True, hide_stderr=False, **kwargs):
    if check and hide_stderr:
        kwargs['stderr'] = subprocess.PIPE
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
        if hide_stderr:
            sys.stderr.write(result.stderr)
        msg = (
            f'Command exited with status code {result.returncode}:\n'
            + ' '.join([shlex.quote(part) for part in command])
        )
        raise CloudSubmitError(msg)
    return result
