import datetime as dt
import cloud_submit as cs

import cloud_submit.envs.aws
from cloud_submit.envs.aws import LocalAWSEnv


PROJECT_NAME = 'basic-aws'


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

    train_data_af = cs.Artifact(
        'train_data.parquet', kind='file', scope='run')
    coefficients_af = cs.Artifact(
        'coefficients.json', kind='file', scope='run')
    predictions_af = cs.Artifact(
        'predictions.parquet', kind='file', scope='run')

    # Steps

    generate_step = cs.Step(
        name='generate',
        function='generate:generate_step',
        image='exec',
        params={
            'random_seed': 42,
            'num_rows': 100,
            'alpha': 1.0,
            'beta': 2.0,
            'sigma': 0.1,
        },
        outputs={
            'train_data_path': train_data_af.local,
        },
    )

    fit_step = cs.Step(
        name='fit',
        function='fit:fit_step',
        image='exec',
        inputs={
            'train_data_path': train_data_af.local,
        },
        outputs={
            'coefficients_path': coefficients_af.local,
            'predictions_path': predictions_af.local,
        },
    )


    config = cs.Config(
        project_name=PROJECT_NAME,
        user_name=userconfig['username'],
        project_root=project_root,
        images=images,
        pipelines=[
            cs.Pipeline(
                name='train',
                steps=[
                    generate_step,
                    fit_step,
                ],
            ),
        ],
        environments=[
            LocalAWSEnv(
                name='local',
                project=PROJECT_NAME,
                user=userconfig['username'],
                aws_account_id=userconfig['aws_account_id'],
                aws_region=userconfig['aws_region'],
                aws_profile=userconfig['aws_profile'],
                docker_namespace=userconfig['docker_namespace'],
                s3_prefix='s3://mybucket',
            ),
        ],
    )

    return config

