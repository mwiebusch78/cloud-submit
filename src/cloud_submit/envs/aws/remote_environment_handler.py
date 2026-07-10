import datetime as dt
import os
import shutil
import json
import subprocess

from cloud_submit import run_command, write_json, CloudSubmitError

from .local_environment_handler import LocalAWSEnv


class RemoteAWSEnv(LocalAWSEnv):
    def __init__(
        self,
        name,
        project,
        user,
        aws_account_id,
        aws_region,
        aws_profile,
        s3_bucket,
        s3_prefix,
        ecs_cluster_arn,
        ecs_capacity_provider,
        ecs_infrastructure_role_arn,
        ecs_execution_role_arn,
        ecs_task_role_arn,
        stepfunctions_role_arn,
        docker_command='docker',
        docker_namespace='csub',
        docker_login_refresh_hours=6,
        aws_command='aws',
        log_group_prefix='/ecs/csub/',
        container_aws_command='aws',
    ):
        super().__init__(
            name=name,
            project=project,
            user=user,
            aws_account_id=aws_account_id,
            aws_region=aws_region,
            aws_profile=aws_profile,
            s3_bucket=s3_bucket,
            s3_prefix=s3_prefix,
            docker_command=docker_command,
            docker_namespace=docker_namespace,
            docker_login_refresh_hours=docker_login_refresh_hours,
            aws_command=aws_command,
        )
        self._ecs_cluster_arn = str(ecs_cluster_arn)
        self._ecs_capacity_provider = str(ecs_capacity_provider)
        self._ecs_infrastructure_role_arn = str(ecs_infrastructure_role_arn)
        self._ecs_execution_role_arn = str(ecs_execution_role_arn)
        self._ecs_task_role_arn = str(ecs_task_role_arn)
        self._stepfunctions_role_arn = str(stepfunctions_role_arn)
        self._log_group_prefix = str(log_group_prefix)
        self._container_aws_command = str(container_aws_command)

    def install_execution_handler(self, path):
        sourcedir = os.path.dirname(__file__)
        shutil.copyfile(
            os.path.join(sourcedir, 'remote_execution_handler.py'),
            os.path.join(path, 'execution_handler.py'),
        )
        shutil.copyfile(
            os.path.join(sourcedir, 's3_tools.py'),
            os.path.join(path, 's3_tools.py'),
        )
        write_json(
            {
                'project': self._project,
                'user': self._user,
                'container_aws_command': self._container_aws_command,
                's3_bucket': self._s3_bucket,
                's3_prefix': self._s3_prefix,
            },
            os.path.join(path, 'execution_config.json'),
        )

    def _make_workflow_name(self, run_id):
        return '--'.join([self._project, self._user, run_id])

    def _make_workflow_arn(self, workflow_name):
        return ':'.join([
            'arn', 'aws', 'states',
            self._aws_region, self._aws_account_id,
            'stateMachine', workflow_name,
        ])

    def _check_workflow_exists(self, workflow_name):
        workflow_arn = self._make_workflow_arn(workflow_name)
        result = run_command(
            [
                self._aws_command,
                'stepfunctions',
                'list-state-machines',
                '--profile', self._aws_profile,
                '--region', self._aws_region,
            ],
            stdout=subprocess.PIPE,
            text=True,
        )
        result = json.loads(result.stdout)
        arns = set(s['stateMachineArn'] for s in result['stateMachines'])
        return workflow_arn in arns

    def _group_steps(self, pipeline, image_refs):
        groups = []
        last_spec = None
        for i, step in enumerate(pipeline.steps):
            if step.name in image_refs:
                spec = (image_refs[step.name], step.spec)
                if spec is None or spec != last_spec:
                    groups.append([])
                groups[-1].append(i)
                last_spec = spec
        return groups

    def _build_task(
        self,
        workflow_name,
        task_index,
        image_ref,
        spec,
        pipeline_name,
        step_names,
        timestamp,
        run_id,
        is_last,
    ):
        cpu = spec.get('cpu', 1)
        memory = spec.get('memory', 2)
        disk = spec.get('disk', 10)
        return {
            f"RegisterTask{task_index}": {
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:ecs:registerTaskDefinition",
                "Parameters": {
                    "Family": f"{workflow_name}--{task_index}",
                    "RequiresCompatibilities": ["EC2"],
                    "NetworkMode": "host",
                    "Cpu": f'{cpu} vCPU',
                    "Memory": f'{memory} GB',
                    "ExecutionRoleArn": self._ecs_execution_role_arn,
                    "TaskRoleArn": self._ecs_task_role_arn,
                    "ContainerDefinitions": [
                        {
                            "Name": "main",
                            "Image": image_ref,
                            "Command": [pipeline_name, ','.join(step_names)],
                            "Essential": True,
                            "MountPoints": [
                                {
                                    "SourceVolume": "artifacts",
                                    "ContainerPath": "/mnt/artifacts"
                                }
                            ],
                            "Environment": [
                                {
                                    "Name": "CSUB_TIMESTAMP",
                                    "Value": timestamp.isoformat(),
                                },
                                {
                                    "Name": "CSUB_RUN_ID",
                                    "Value": run_id,
                                },
                                {
                                    "Name": "CSUB_WORKER_INDEX",
                                    "Value": "0",
                                },
                            ],
                            "LogConfiguration": {
                                "LogDriver": "awslogs",
                                "Options": {
                                    "awslogs-group":
                                        self._log_group_prefix + workflow_name,
                                    "awslogs-region": self._aws_region,
                                    "awslogs-stream-prefix": "ecs",
                                    "awslogs-create-group": "true"
                                }
                            }
                        }
                    ],
                    "Volumes": [
                        {
                            "Name": "artifacts",
                            "ConfiguredAtLaunch": True
                        }
                    ]
                },
                "ResultPath": "$.RegisteredTask",
                "Next": f"RunTask{task_index}"
            },
            f"RunTask{task_index}": {
                "Type": "Task",
                "Resource": "arn:aws:states:::ecs:runTask.sync",
                "Parameters": {
                    "Cluster": self._ecs_cluster_arn,
                    "TaskDefinition.$": "$.RegisteredTask.TaskDefinition.TaskDefinitionArn",
                    "CapacityProviderStrategy": [
                        {
                            "CapacityProvider": self._ecs_capacity_provider,
                            "Weight": 1,
                            "Base": 0
                        }
                    ],
                    "VolumeConfigurations": [
                        {
                            "Name": "artifacts",
                            "ManagedEBSVolume": {
                                "SizeInGiB": disk,
                                "VolumeType": "gp3",
                                "Encrypted": False,
                                "FilesystemType": "ext4",
                                "TerminationPolicy": {
                                    "DeleteOnTermination": True
                                },
                                "RoleArn": self._ecs_infrastructure_role_arn,
                            }
                        }
                    ]
                },
                "ResultPath": "$.ExecutionResult",
                "Catch": [
                    {
                        "ErrorEquals": ["States.ALL"],
                        "ResultPath": "$.ExecutionError",
                        "Next": f"DeregisterTask{task_index}OnFailure"
                    }
                ],
                "Next": f"DeregisterTask{task_index}OnSuccess"
            },
            f"DeregisterTask{task_index}OnSuccess": {
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:ecs:deregisterTaskDefinition",
                "Parameters": {
                    "TaskDefinition.$": "$.RegisteredTask.TaskDefinition.TaskDefinitionArn"
                },
                ("End" if is_last else "Next"): (
                    True if is_last else f"RegisterTask{task_index + 1}"),
            },
            f"DeregisterTask{task_index}OnFailure": {
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:ecs:deregisterTaskDefinition",
                "Parameters": {
                    "TaskDefinition.$": "$.RegisteredTask.TaskDefinition.TaskDefinitionArn"
                },
                "Next": "WorkflowFailed"
            },
        }

    def _build_workflow(
        self,
        pipeline,
        image_refs,
        workflow_name,
        timestamp,
        run_id,
    ):
        groups = self._group_steps(pipeline, image_refs)
        states = {}
        for task_index, group in enumerate(groups):
            steps = [pipeline.steps[i] for i in group]
            for step in steps:
                if step.num_workers is not None and step.num_workers != 1:
                    raise CloudSubmitError(
                        f'Step {step.name} requests multiple workers '
                        'but multi-worker steps are not supported by the '
                        'environment.'
                    )
            step_names = [step.name for step in steps]
            image_ref = image_refs[step_names[0]]
            spec = pipeline.steps[group[0]].spec
            states.update(
                self._build_task(
                    workflow_name=workflow_name,
                    task_index=task_index,
                    image_ref=image_ref,
                    spec=spec,
                    pipeline_name=pipeline.name,
                    step_names=step_names,
                    timestamp=timestamp,
                    run_id=run_id,
                    is_last=(task_index == len(groups) - 1),
                )
            )
        states["WorkflowFailed"] = {
            "Type": "Fail",
            "Cause": "The ECS container task failed execution.",
            "Error": "TaskExecutionError"
        }

        return {
            "StartAt": "RegisterTask0",
            "States": states,
        }

    def run_pipeline(self, pipeline, image_refs, timestamp, run_id, temp_path):
        workflow_name = self._make_workflow_name(run_id)
        workflow = self._build_workflow(
            pipeline,
            image_refs,
            workflow_name,
            timestamp,
            run_id,
        )

        workflow_path = os.path.join(temp_path, f'{workflow_name}.json')
        with open(workflow_path, 'w') as stream:
            json.dump(workflow, stream)

        if self._check_workflow_exists(workflow_name):
            workflow_arn = self._make_workflow_arn(workflow_name)
            run_command([
                self._aws_command,
                'stepfunctions',
                'update-state-machine',
                '--profile', self._aws_profile,
                '--region', self._aws_region,
                '--state-machine-arn', workflow_arn,
                '--role-arn', self._stepfunctions_role_arn,
                '--definition', f'file://{workflow_path}',
            ])
        else:
            run_command([
                self._aws_command,
                'stepfunctions',
                'create-state-machine',
                '--profile', self._aws_profile,
                '--region', self._aws_region,
                '--name', workflow_name,
                '--role-arn', self._stepfunctions_role_arn,
                '--definition', f'file://{workflow_path}',
            ])

        result = run_command(
            [
                self._aws_command,
                'stepfunctions',
                'start-execution',
                '--profile', self._aws_profile,
                '--region', self._aws_region,
                '--state-machine-arn', workflow_arn,
            ],
            stdout=subprocess.PIPE,
            text=True,
        )
        result = json.loads(result.stdout)
        print(f'Workflow execution: {result["executionArn"]}')
        print(f'Started at {result["startDate"]}')

    def print_logs(self, run_id, start_timestamp, stream=False):
        print('\nStreaming logs...')
        workflow_name = self._make_workflow_name(run_id)
        command = [
            self._aws_command,
            'logs',
            'tail',
            '--profile', self._aws_profile,
            '--region', self._aws_region,
            '--since', start_timestamp.isoformat(),
            '--format', 'short',
        ]
        if stream:
            command.append('--follow')
        command.append(self._log_group_prefix + workflow_name)
        run_command(command)
