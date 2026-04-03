import os
import contextlib

from .execution.json_io import write_json
from .execution.config import ConfigError
from .environments.local.environment_handler import LocalEnv
from .images import BaseImage, ExecutionImage


def _dictify(items, kind):
    if items is None:
        items = []
    item_dict = {}
    for item in items:
        if item.name in item_dict:
            raise ConfigError(f'Duplicate {kind} name {item.name}.')
        if kind == 'image' and item.parent is not None:
            if item.parent not in item_dict:
                raise ConfigError(
                    f'Parent image of {item.name} must be '
                    'listed before {item.name}.'
                )
            if isinstance(item_dict[item.parent], ExecutionImage):
                raise ConfigError(
                    f'Image {item.parent} cannot be the parent of {item.name} '
                    'since it is an execution image.'
                )
        item_dict[item.name] = item
    return item_dict


class Config:
    def __init__(
        self,
        project_name,
        user_name,
        project_root=None,
        docker_namespace='localhost/csub',
        images=None,
        pipelines=None,
        environments=None,
        build_default=None,
        submit_default=None,
    ):
        self.project_name = project_name
        self.user_name = user_name
        self.project_root = project_root
        self.docker_namespace = docker_namespace
        self.environments = _dictify(environments, 'environment')
        self.images = _dictify(images, 'image')
        self.pipelines = _dictify(pipelines, 'pipeline')
        self.build_default = build_default
        self.submit_default = submit_default

        if self.project_root is None:
            self.project_root = os.getcwd()
        self.project_root = os.path.abspath(self.project_root)

        # Create local environment if no environments are given.
        if not self.environments:
            self.environments['local'] = LocalEnv(
                name='local',
                project=self.project_name,
                user=self.user_name,
            )

        # Set default environment to first item in the list if not given.
        firstenv = next(iter(self.environments.keys()))
        if self.build_default is None:
            self.build_default = firstenv
        if self.submit_default is None:
            self.submit_default = firstenv

        # check if default environments exist
        if self.build_default not in self.environments:
            raise ConfigError(
                f'Default build environment {repr(self.build_default)} '
                'not found in environment list.'
            )
        if self.submit_default not in self.environments:
            raise ConfigError(
                f'Default submit environment {repr(self.submit_default)} '
                'not found in environment list.'
            )

        # check pipelines
        for pipeline_name, pipeline in self.pipelines.items():
            for step in pipeline.steps:
                if step.image not in self.images:
                    raise ConfigError(
                        f'Step {repr(step.name)} in pipeline '
                        f'{repr(pipeline_name)} requires unknown image '
                        f'{repr(step.image)}.'
                    )
                image = self.images[step.image]
                if not isinstance(image, ExecutionImage):
                    raise ConfigError(
                        f'Step {repr(step.name)} in pipeline '
                        f'{repr(pipeline_name)} requires image '
                        f'{repr(step.image)}, which is not an execution image.'
                    )

    def export_pipelines(self, path):
        obj = {}
        for name, pipeline in self.pipelines.items():
            obj[name] = [step.to_dict() for step in pipeline.steps]
        write_json(obj, path)

    def _get_env_handler(self, env, purpose):
        if env is None:
            if purpose == 'build':
                env = self.build_default
            elif purpose == 'submit':
                env = self.submit_default
            else:
                raise ValueError(f'Invalid `purpose` argument {repr(purpose)}.')
        try:
            return self.environments[env]
        except KeyError:
            raise ValueError(f'Unknown environment {env}.')

    def get_build_env(self, env=None):
        return self._get_env_handler(env, 'build')

    def get_submit_env(self, env=None):
        return self._get_env_handler(env, 'submit')

    def get_image_ancestry(self, image_name):
        images = []
        while image_name is not None:
            image = self.images[image_name]
            images.append(image_name)
            image_name = image.parent

        return [self.images[name] for name in images[::-1]]

    def in_project_root(self):
        return contextlib.chdir(self.project_root)

