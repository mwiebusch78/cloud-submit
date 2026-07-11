import datetime as dt
import cloud_submit as cs


PROJECT_NAME = 'multi-worker'


def build_config(project_root, userconfig):
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
                    'num_rows': 100,
                    'alpha': 1.0,
                    'beta': 2.0,
                    'sigma': 0.1,
                },
                outputs={
                    'train_data_path': cs.local('train_data.parquet'),
                },
                pass_submit_timestamp_as='submit_timestamp',
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
                num_workers=2,
                pass_num_workers_as='num_workers',
                pass_worker_index_as='worker_index',
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

