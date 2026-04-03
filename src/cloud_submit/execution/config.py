import os
from .json_io import read_json


class ConfigError(Exception):
    pass


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

    def _location(self, location_type):
        return ArtifactLocation(self, location_type)

    @property
    def local(self):
        return self._location('local')

    @property
    def remote(self):
        return self._location('remote')

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
        artifact,
        location_type,
        sync=True,
    ):
        self.artifact = artifact
        if location_type not in ['local', 'remote']:
            raise ValueError(f'Unknown location type {repr(location_type)}')
        self.location_type = location_type
        self.sync = bool(sync)

    @property
    def nosync(self):
        return ArtifactLocation(self.artifact, self.location_type, sync=False)

    def to_dict(self):
        return {
            'artifact': self.artifact.to_dict(),
            'location_type': self.location_type,
            'sync': self.sync
        }

    @staticmethod
    def from_dict(obj):
        return ArtifactLocation(
            Artifact.from_dict(obj['artifact']),
            obj['location_type'],
            sync=obj['sync'],
        )


class Spec:
    def __init__(self, **kwargs):
        self._spec = kwargs

    def __getattr__(self, attr):
        try:
            return self._spec[attr]
        except KeyError:
            raise AttributeError(f'Attribute {attr} not found.')


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
        self.num_workers = None
        if num_workers is not None:
            self.num_workers = int(num_workers)
        self.pass_num_workers_as = pass_num_workers_as
        self.pass_worker_index_as = pass_worker_index_as

    def to_dict(self):
        return {
            'name': self.name,
            'function': self.function,
            'params': self.params,
            'inputs': {k: v.to_dict() for k, v in self.inputs.items()},
            'outputs': {k: v.to_dict() for k, v in self.outputs.items()},
            'temporaries':
                {k: v.to_dict() for k, v in self.temporaries.items()},
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
        )


class Pipeline:
    def __init__(
        self,
        name,
        steps,
    ):
        self.name = name
        self.steps = list(steps)

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
        }

    @staticmethod
    def from_dict(obj):
        return Pipeline(
            name=obj['name'],
            steps=[Step.from_dict(s) for s in obj['steps']],
        )


def read_pipelines():
    path = os.path.join(os.path.dirname(__file__), 'pipelines.json')
    obj = read_json(path)
    pipelines = {}
    for name, steps in obj.items():
        pipelines[name] = [Step.from_dict(s) for s in steps]
    return pipelines
