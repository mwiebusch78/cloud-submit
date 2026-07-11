import datetime as dt
import cloud_submit as cs

# csub automatically adds your project's src directory to sys.path, so
# you can import any modules defined there.
from parameters import PARAMS


PROJECT_NAME = 'basic'


def build_config(project_root, userconfig):
    """Create an instance of cloud_submit.Config describing your project.

    Note:
      The csub command expects a function with this name defined in
      src/csub/build_config.py. It calls this function to obtain a
      cloud_submit.Config object which describes the environments, images,
      artifacts and pipelines defined in your project.

    Args:
      project_root (str): The root directory of your project.
      userconfig (dict): A dict holding user-specific information. This
        corresponds to the content of the file userconfigs/default.yaml
        in your project directory (or some other yaml file in that directory
        if csub was called with the --user option).

    Returns:
      config (cloud_submit.Config): The config object describing the project.
    """

    # Images

    images = [
        cs.BaseImage(
            name='base',
            instructions="""
            FROM python
            RUN pip install --root-user-action=ignore polars numpy
            """,
        ),
        cs.ExecutionImage(
            name='exec',
            parent='base',
        ),
    ]

    # Artifacts

    artifacts = [
        cs.Artifact('train_data.parquet', kind='file', scope='run'),
        cs.Artifact('coefficients.json', kind='file', scope='run'),
        cs.Artifact('predictions.parquet', kind='file', scope='run'),
    ]

    # Pipelines

    train_pipeline = cs.Pipeline(
        name='train',
        steps=[
            cs.Step(
                name='generate',
                function='generate:generate_step',
                image='exec',
                params={
                    'random_seed': PARAMS['random_seed'],
                    'num_samples': PARAMS['num_samples'],
                    'alpha': PARAMS['alpha'],
                    'beta': PARAMS['beta'],
                    'sigma': PARAMS['sigma'],
                },
                outputs={
                    'train_data_path': cs.local('train_data.parquet'),
                },
            ),
            cs.Step(
                name='fit',
                function='fit:fit_step',
                image='exec',
                inputs={
                    'train_data_path': cs.local('train_data.parquet'),
                },
                outputs={
                    'coefficients_path': cs.local('coefficients.json'),
                    'predictions_path': cs.local('predictions.parquet'),
                },
            ),
        ],
    )


    config = cs.Config(
        project_name=PROJECT_NAME,
        user_name=userconfig['username'],
        project_root=project_root,
        images=images,
        artifacts=artifacts,
        pipelines=[train_pipeline],
    )

    return config

