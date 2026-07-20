import datetime as dt
import cloud_submit as cs

# import AWS-specific environments that ship with cloud_submit.
import cloud_submit.envs.aws
from cloud_submit.envs.aws import LocalAWSEnv, RemoteAWSEnv

# csub automatically adds your project's src directory to sys.path, so
# you can import any modules defined there.
from parameters import PARAMS


PROJECT_NAME = 'basic-aws'


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
            instructions=r"""
            FROM python:3.12.3
            RUN \
              cd /opt && \
              curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" \
                -o "awscliv2.zip" && \
              unzip awscliv2.zip && \
              ./aws/install && \
              rm -rf awscliv2.zip aws
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
                spec=cs.Spec(
                    cpu=2,
                    memory=6.5,
                    disk=20,
                ),
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
                spec=cs.Spec(
                    cpu=2,
                    memory=6.5,
                    disk=20,
                ),
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
        environments=[
            LocalAWSEnv(
                name='local',
                project=PROJECT_NAME,
                user=userconfig['username'],
                aws_account_id=userconfig['aws_account_id'],
                aws_region=userconfig['aws_region'],
                aws_profile=userconfig['aws_profile'],
                docker_namespace=userconfig['docker_namespace'],
                docker_platforms=userconfig['docker_platforms'],
                s3_bucket=userconfig['s3_bucket'],
                s3_prefix=userconfig['s3_prefix'],
            ),
            RemoteAWSEnv(
                name='remote',
                project=PROJECT_NAME,
                user=userconfig['username'],
                aws_account_id=userconfig['aws_account_id'],
                aws_region=userconfig['aws_region'],
                aws_profile=userconfig['aws_profile'],
                docker_namespace=userconfig['docker_namespace'],
                docker_platforms=userconfig['docker_platforms'],
                s3_bucket=userconfig['s3_bucket'],
                s3_prefix=userconfig['s3_prefix'],
                ecs_cluster_arn=userconfig['ecs_cluster_arn'],
                ecs_capacity_provider=userconfig['ecs_capacity_provider'],
                ecs_infrastructure_role_arn=
                    userconfig['ecs_infrastructure_role_arn'],
                ecs_execution_role_arn=userconfig['ecs_execution_role_arn'],
                ecs_task_role_arn=userconfig['ecs_task_role_arn'],
                stepfunctions_role_arn=userconfig['stepfunctions_role_arn'],
            ),
        ],
    )

    return config

