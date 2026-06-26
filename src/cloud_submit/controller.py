import os
import shutil
import datetime as dt

from .utils import ensure_path, CloudSubmitError, parse_image_ref
from .images import ExecutionImage, BaseImage
from .execution.config import to_utc

class Controller:
    def __init__(self, config):
        self._config = config

    def _install_execution_modules(self, path):
        sourcedir = os.path.dirname(__file__)
        shutil.copyfile(
            os.path.join(sourcedir, 'execution', 'config.py'),
            os.path.join(path, 'config.py'),
        )
        shutil.copyfile(
            os.path.join(sourcedir, 'execution', 'base_execution_handler.py'),
            os.path.join(path, 'base_execution_handler.py'),
        )
        shutil.copyfile(
            os.path.join(sourcedir, 'execution', 'json_io.py'),
            os.path.join(path, 'json_io.py'),
        )
        shutil.copyfile(
            os.path.join(sourcedir, 'execution', 'execute.py'),
            os.path.join(path, 'execute.py'),
        )
        self._config.export_pipelines(os.path.join(path, 'pipelines.json'))
        self._config.export_artifacts(os.path.join(path, 'artifacts.json'))

    def _build_image(self, image, build_id, env, rebuild=False):
        if not rebuild:
            ref = env.get_image_ref(image)
            if ref is not None:
                print(f'Using existing build of {image.name}: {ref}')
                return ref

        is_execution_image = isinstance(image, ExecutionImage)
        path = os.path.join('build', image.name)
        if is_execution_image:
            path = os.path.join(path, env.name)
            ensure_path(path, clear=True)
            shutil.copytree('src', os.path.join(path, 'src'))
            csub_path = os.path.join(path, 'src', 'csub')
            ensure_path(csub_path, clear=True)
            self._install_execution_modules(csub_path)
            env.install_execution_handler(csub_path)
        else:
            ensure_path(path, clear=True)

        parent_ref = None
        if image.parent is not None:
            parent_image = self._config.images.get(image.parent)
            if parent_image is None:
                raise CloudSubmitError(
                    f'Could not find declaration for image {image.parent}.')
            parent_ref = env.get_image_ref(parent_image)
            if parent_ref is None:
                raise CloudSubmitError(
                    f'Could not find reference for image {image.parent}.')
            env.pull_image(parent_ref)
        image.setup_builddir(path, parent_ref)

        print(f'Building image {image.name}.')
        ref = env.build_image(path, image, build_id)
        if ref is None:
            raise CloudSubmitError(
                'build_image method of environment handler did not '
                'return a valid image reference.'
            )
        env.save_image_ref(image, ref)

    def build(self, images, build_id=None, env=None):
        now = dt.datetime.now(tz=dt.UTC)
        env_handler = self._config.get_build_env(env)
        build_id = env_handler.generate_build_id(now, build_id)

        images = set(images)
        for image in images:
            if image not in self._config.images:
                raise CloudSubmitError(
                    f'No definition found for image {image}.'
                )
        all_images = set(
            i.name for image in images
            for i in self._config.get_image_ancestry(image)
        )

        with self._config.in_project_root():
            for image_name, image in self._config.images.items():
                if image_name not in all_images:
                    continue
                rebuild = image_name in images
                self._build_image(
                    image, build_id, env_handler, rebuild=rebuild)

    def list_image_refs(self, images=None, ids=None, env=None, remote=False):
        env_handler = self._config.get_build_env(env)
        if images is None:
            images = list(self._config.images.keys())
        else:
            images = set(images)
            images = [
                i for i in self._config.images.keys()
                if i in images
            ]
        if ids is not None:
            ids = set(ids)

        results = []
        for image in images:
            image = self._config.images[image]
            repo_name = env_handler.get_image_repo_name(image)
            if remote:
                tags = env_handler.list_remote_image_tags(repo_name)
            else:
                tags = env_handler.list_local_image_tags(repo_name)
            if ids is not None:
                tags = [t for t in tags if t in ids]
            for tag in tags:
                results.append(':'.join([repo_name, tag]))
        return results

    def list_images(self, images=None, env=None):
        env_handler = self._config.get_build_env(env)
        if images is not None:
            images = set(images)
        result = []
        with self._config.in_project_root():
            for i in self._config.images.values():
                if images is not None and i.name not in images:
                    continue
                if isinstance(i, BaseImage):
                    tpe = 'base'
                elif isinstance(i, ExecutionImage):
                    tpe = 'execution'
                else:
                    raise ValueError('Unknown image type.')
                ref = env_handler.get_image_ref(i)
                result.append((i.name, tpe, ref))
        return result

    def remove_image_refs(self, refs, remote=False, env=None):
        env_handler = self._config.get_build_env(env)
        if remote:
            env_handler.remove_remote_image_refs(refs)
        else:
            env_handler.remove_local_image_refs(refs)

    def set_image(self, image_name, ref, env=None):
        env_handler = self._config.get_build_env(env)
        image = self._config.images.get(image_name)
        if image is None:
            raise CloudSubmitError(f'Unknow image name {image_name}.')
        with self._config.in_project_root():
            env_handler.save_image_ref(image, ref)

    def unset_image(self, image_name, env=None):
        env_handler = self._config.get_build_env(env)
        image = self._config.images.get(image_name)
        if image is None:
            raise CloudSubmitError(f'Unknow image name {image_name}.')
        with self._config.in_project_root():
            env_handler.clear_image_ref(image)

    def run(
        self,
        pipeline,
        steps=None,
        run_id=None,
        timestamp=None,
        env=None,
        build_env=None,
    ):
        try:
            pipeline = self._config.pipelines[pipeline]
        except KeyError:
            raise CloudSubmitError(f'Pipeline not found: {pipeline}')

        now = dt.datetime.now(tz=dt.UTC)
        if isinstance(timestamp, str) and timestamp == 'now':
            timestamp = now
        if timestamp is None:
            timestamp = pipeline.default_submit_timestamp
        if timestamp is None:
            timestamp = now
        timestamp = to_utc(timestamp)
        env_handler = self._config.get_submit_env(env)
        run_id = env_handler.generate_run_id(now, run_id)
        if steps is None:
            steps = [step.name for step in pipeline.steps]
        steps = set(steps)
        steps = [step for step in pipeline.steps if step.name in steps]

        with self._config.in_project_root():
            images = [step.image for step in steps]
            self.build(images, build_id=run_id, env=build_env)
            refs = {}
            for step in steps:
                image = self._config.images.get(step.image)
                if image is None:
                    raise CloudSubmitError(
                        f'Cannot find declaration for image {step.image}.')
                ref = env_handler.get_image_ref(image)
                if ref is None:
                    raise CloudSubmitError(
                        f'Could not find image ref for image {step.image}')
                refs[step.name] = ref

            env_handler.submit(pipeline, refs, timestamp, run_id)
