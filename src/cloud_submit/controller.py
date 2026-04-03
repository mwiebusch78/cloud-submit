import os
import shutil

from .utils import ensure_path, CloudSubmitError
from .images import ExecutionImage

class Controller:
    def __init__(self, config):
        self._config = config

    def _get_image_ref(self, image_name):
        ensure_path('images')
        path = os.path.join('images', image_name)
        try:
            with open(path, 'r') as stream:
                ref = stream.read().strip()
        except FileNotFoundError:
            return None
        return ref

    def _save_image_ref(self, image_name, ref):
        ensure_path('images')
        path = os.path.join('images', image_name)
        with open(path, 'w') as stream:
            stream.write(ref)

    def _install_execution_modules(self, path):
        sourcedir = os.path.dirname(__file__)
        shutil.copyfile(
            os.path.join(sourcedir, 'execution', 'config.py'),
            os.path.join(path, 'config.py'),
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

    def _build_image(self, image, build_id, env, rebuild=False):
        if not rebuild:
            ref = self._get_image_ref(image.name)
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
            parent_ref = self._get_image_ref(image.parent)
            if parent_ref is None:
                raise CloudSubmitError(
                    f'Could not find reference for image {image.parent}.')
        image.setup_builddir(path, parent_ref)

        print(f'Building image {image.name}.')
        ref = env.build_image(path, image, build_id)
        if ref is None:
            raise CloudSubmitError(
                'build_image method of environment handler did not '
                'return a valid image reference.'
            )
        self._save_image_ref(image.name, ref)

    def build(self, image_name=None, build_id=None, env=None):
        env_handler = self._config.get_build_env(env)
        build_id = env_handler.generate_build_id(build_id)
        if image_name is None:
            images = list(self._config.images.values())
            build_all = True
        else:
            images = self._config.get_image_ancestry(image_name)
            build_all = False

        with self._config.in_project_root():
            for image in images[:-1]:
                self._build_image(
                    image, build_id, env_handler, rebuild=build_all)
            self._build_image(images[-1], build_id, env_handler, rebuild=True)

    def submit(
        self,
        pipeline,
        steps=None,
        run_id=None,
        env=None,
        build_env=None,
    ):
        env_handler = self._config.get_submit_env(env)
        run_id = env_handler.generate_run_id(run_id)
        try:
            pipeline = self._config.pipelines[pipeline]
        except KeyError:
            raise CloudSubmitError(f'Pipeline not found: {pipeline}')
        if steps is None:
            steps = [step.name for step in pipeline.steps]
        steps = set(steps)
        steps = [step for step in pipeline.steps if step.name in steps]

        images = sorted(set(step.image for step in steps))
        for image in images:
            self.build(image, build_id=run_id, env=build_env)
        refs = {}
        for step in steps:
            ref = self._get_image_ref(step.image)
            if ref is None:
                raise CloudSubmitError(
                    f'Could not find image ref for image {step.image}')
            refs[step.name] = ref

        with self._config.in_project_root():
            env_handler.submit(pipeline, refs, run_id)

