import os
import datetime as dt

from .utils import read_json


class ConfigError(Exception):
    pass


def to_utc(timestamp):
    if timestamp is None:
        return None
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=dt.UTC)
    return timestamp.astimezone(dt.UTC)


class Artifact:
    def __init__(
        self,
        name,
        kind='file',
        scope='run',
    ):
        if scope not in ['run', 'user', 'project']:
            raise ValueError(
                f'Invalid value {repr(scope)} for scope. '
                "Must be 'run', 'user' or 'project'."
            )
        self.name = name
        self.kind = kind
        self.scope = scope

    def __eq__(self, other):
        return (
            isinstance(other, Artifact)
            and other.name == self.name
            and other.kind == self.kind
            and other.scope == self.scope
        )

    def copy(self):
        return Artifact(self.name, kind=self.kind, scope=self.scope)

    def to_dict(self):
        return {
            'name': self.name,
            'kind': self.kind,
            'scope': self.scope,
        }

    @staticmethod
    def from_dict(obj):
        return Artifact(
            obj['name'],
            kind=obj['kind'],
            scope=obj['scope'],
        )


class ArtifactLocation:
    def __init__(
        self,
        artifact_name,
        is_local,
        sync=True,
    ):
        self.artifact_name = str(artifact_name)
        self.is_local = bool(is_local)
        self.sync = bool(sync)

    def to_dict(self):
        return {
            'artifact_name': self.artifact_name,
            'is_local': self.is_local,
            'sync': self.sync
        }

    @staticmethod
    def from_dict(obj):
        return ArtifactLocation(
            obj['artifact_name'],
            obj['is_local'],
            sync=obj['sync'],
        )


def local(name, sync=True):
    return ArtifactLocation(name, is_local=True, sync=sync)


def remote(name, sync=True):
    return ArtifactLocation(name, is_local=False, sync=sync)


class Spec:
    def __init__(self, **kwargs):
        self._spec = kwargs

    def get(self, attr, default=None):
        return self._spec.get(attr, default)

    def __eq__(self, other):
        if other is None:
            return False
        return self._spec == other._spec


class Step:
    def __init__(
        self,
        name,
        function,
        image=None,
        spec=None,
        params=None,
        inputs=None,
        outputs=None,
        temporaries=None,
        num_workers=None,
        pass_num_workers_as='num_workers',
        pass_worker_index_as='worker_index',
        pass_submit_timestamp_as=None,
        pass_run_id_as=None,
    ):
        self.name = name
        self.function = function
        self.params = params
        if self.params is None:
            self.params = {}
        self.inputs = inputs
        if self.inputs is None:
            self.inputs = {}
        self.outputs = outputs
        if self.outputs is None:
            self.outputs = {}
        self.temporaries = temporaries
        if self.temporaries is None:
            self.temporaries = {}
        self.image = image
        self.spec = spec
        if self.spec is None:
            self.spec = Spec()
        self.num_workers = None
        if num_workers is not None:
            self.num_workers = int(num_workers)
        self.pass_num_workers_as = pass_num_workers_as
        self.pass_worker_index_as = pass_worker_index_as
        self.pass_submit_timestamp_as = pass_submit_timestamp_as
        self.pass_run_id_as = pass_run_id_as

    def to_dict(self):
        return {
            'name': self.name,
            'function': self.function,
            'params': self.params,
            'inputs': {k: v.to_dict() for k, v in self.inputs.items()},
            'outputs': {k: v.to_dict() for k, v in self.outputs.items()},
            'temporaries':
                {k: v.to_dict() for k, v in self.temporaries.items()},
            'num_workers': self.num_workers,
            'pass_num_workers_as': self.pass_num_workers_as,
            'pass_worker_index_as': self.pass_worker_index_as,
            'pass_submit_timestamp_as': self.pass_submit_timestamp_as,
            'pass_run_id_as': self.pass_run_id_as,
        }

    @staticmethod
    def from_dict(obj):
        return Step(
            obj['name'],
            obj['function'],
            params=obj['params'],
            inputs={
                k: ArtifactLocation.from_dict(v)
                for k, v in obj['inputs'].items()
            },
            outputs={
                k: ArtifactLocation.from_dict(v)
                for k, v in obj['outputs'].items()
            },
            temporaries={
                k: ArtifactLocation.from_dict(v)
                for k, v in obj['temporaries'].items()
            },
            num_workers=obj['num_workers'],
            pass_num_workers_as=obj['pass_num_workers_as'],
            pass_worker_index_as=obj['pass_worker_index_as'],
            pass_submit_timestamp_as=obj['pass_submit_timestamp_as'],
            pass_run_id_as=obj['pass_run_id_as'],
        )


class Pipeline:
    def __init__(
        self,
        name,
        steps,
        default_submit_timestamp=None,
    ):
        self.name = name
        self.steps = list(steps)
        self.default_submit_timestamp = to_utc(default_submit_timestamp)

        step_names = set()
        for step in self.steps:
            if step.name in step_names:
                raise ConfigError(
                    f'Duplicate step name {step.name} '
                    f'in pipeline {self.name}.'
                )
            step_names.add(step.name)

    def to_dict(self):
        return {
            'name': self.name,
            'steps': [s.to_dict() for s in self.steps],
            'default_submit_timestamp': self.default_submit_timestamp
        }

    @staticmethod
    def from_dict(obj):
        return Pipeline(
            name=obj['name'],
            steps=[Step.from_dict(s) for s in obj['steps']],
            default_submit_timestamp=obj['default_submit_timestamp']
        )


def read_pipelines():
    path = os.path.join(os.path.dirname(__file__), 'pipelines.json')
    obj = read_json(path)
    pipelines = {}
    for name, steps in obj.items():
        pipelines[name] = [Step.from_dict(s) for s in steps]
    return pipelines


def read_artifacts():
    path = os.path.join(os.path.dirname(__file__), 'artifacts.json')
    obj = read_json(path)
    artifacts = {}
    for artifact in obj['artifacts']:
        artifact = Artifact.from_dict(artifact)
        artifacts[artifact.name] = artifact
    return artifacts

