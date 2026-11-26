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
        adhoc_log_group,
        deployed_log_group,
        envvars=None,
        secrets=None,
        docker_command='docker',
        docker_namespace='csub',
        docker_platforms=None,
        docker_login_refresh_hours=6,
        aws_command='aws',
        container_aws_command='aws',
        scheduler_timezone=None,
        scheduler_role_arn=None,
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
            docker_platforms=docker_platforms,
            docker_login_refresh_hours=docker_login_refresh_hours,
            aws_command=aws_command,
            container_aws_command=container_aws_command,
        )
        self._ecs_cluster_arn = str(ecs_cluster_arn)
        self._ecs_capacity_provider = str(ecs_capacity_provider)
        self._ecs_infrastructure_role_arn = str(ecs_infrastructure_role_arn)
        self._ecs_execution_role_arn = str(ecs_execution_role_arn)
        self._ecs_task_role_arn = str(ecs_task_role_arn)
        self._stepfunctions_role_arn = str(stepfunctions_role_arn)

        self._scheduler_timezone = None
        if scheduler_timezone is not None:
            self._scheduler_timezone = str(scheduler_timezone)
        self._scheduler_role_arn = None
        if scheduler_role_arn is not None:
            self._scheduler_role_arn = str(scheduler_role_arn)

        self._adhoc_log_group = str(adhoc_log_group)
        self._deployed_log_group = str(deployed_log_group)

        del self._envvars['CSUB_AWS_PROFILE']
        del self._envvars['CSUB_AWS_REGION']
        if envvars is not None:
            self._envvars.update(envvars)
        if secrets is not None:
            self._secrets.update(secrets)

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

    def build_image(self, path, image, pipeline, build_id, docker_flags):
        return self._build_image(
            path=path,
            image=image,
            pipeline=pipeline,
            build_id=build_id,
            docker_flags=docker_flags,
            push=True,
        )

    def list_remote_image_tags(self, image):
        repo_name = self.get_image_repo(image)
        return self._list_remote_image_tags(repo_name)

    def _make_workflow_name(self, pipeline_name, run_id):
        if run_id is None:
            parts = ['deployed', self._project, self._user, pipeline_name]
        else:
            parts = ['adhoc', self._project, self._user, run_id]
        return '--'.join(parts)

    def _make_schedule_name(self, pipeline_name):
        parts = [self._project, self._user, pipeline_name]
        return '--'.join(parts)

    def _make_log_prefix(self, run_id, pipeline_name):
        if pipeline_name is None:
            parts = [self._project, self._user]
        else:
            parts = [self._project, self._user, pipeline_name]
        if run_id is None:
            parts.append('')
        else:
            parts.append(run_id)
        return '--'.join(parts)

    def _make_workflow_arn(self, workflow_name):
        return ':'.join([
            'arn', 'aws', 'states',
            self._aws_region, self._aws_account_id,
            'stateMachine', workflow_name,
        ])

    def _make_schedule_arn(self, schedule_name):
        return ':'.join([
            'arn', 'aws', 'scheduler',
            self._aws_region, self._aws_account_id,
            f'schedule/default/{schedule_name}',
        ])

    def _list_workflows(self):
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
            hide_stderr=True,
        )
        return json.loads(result.stdout)

    def _check_workflow_exists(self, workflow_name):
        workflow_arn = self._make_workflow_arn(workflow_name)
        result = self._list_workflows()
        arns = set(s['stateMachineArn'] for s in result['stateMachines'])
        return workflow_arn in arns

    def _check_schedule_exists(self, schedule_name):
        schedule_arn = self._make_schedule_arn(schedule_name)
        result = run_command(
            [
                self._aws_command,
                'scheduler',
                'list-schedules',
                '--profile', self._aws_profile,
                '--region', self._aws_region,
            ],
            stdout=subprocess.PIPE,
            text=True,
            hide_stderr=True,
        )
        result = json.loads(result.stdout)
        arns = set(s['Arn'] for s in result['Schedules'])
        return schedule_arn in arns

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
        run_steps,
        is_last,
    ):
        cpu = spec.get('cpu', 1)
        memory = spec.get('memory', 2)
        disk = spec.get('disk', 10)
        envvars = [
            {"Name": str(name), "Value": str(value)}
            for name, value in self._envvars.items()
            if value is not None
        ]
        secrets = [
            {"Name": str(name), "ValueFrom": str(value)}
            for name, value in self._secrets.items()
            if value is not None
        ]
        if timestamp is None or run_id is None:  # deployed workflow
            timestamp = "{% $states.context.Execution.StartTime %}"
            run_id = (
                "{% $fromMillis($toMillis($states.context.Execution.StartTime), "
                "'[Y0001][M01][D01]-[H01][m01][s01]') %}"
            )
            log_group = self._deployed_log_group
            log_prefix = (
                "{% "
                f"'{self._make_log_prefix(None, pipeline_name)}' & "
                "$fromMillis($toMillis($states.context.Execution.StartTime), "
                "'[Y0001][M01][D01]-[H01][m01][s01]') "
                "%}"
            )
        else:  # adhoc workflow
            timestamp = timestamp.isoformat()
            log_group = self._adhoc_log_group
            log_prefix = self._make_log_prefix(run_id, None)

        return {
            f"RegisterTask{task_index}": {
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:ecs:registerTaskDefinition",
                "Arguments": {
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
                                    "Value": timestamp,
                                },
                                {
                                    "Name": "CSUB_RUN_ID",
                                    "Value": run_id,
                                },
                                {
                                    "Name": "CSUB_WORKER_INDEX",
                                    "Value": "-1",
                                },
                                {
                                    "Name": "CSUB_RUN_STEPS",
                                    "Value": run_steps,
                                },
                                *envvars,
                            ],
                            "Secrets": secrets,
                            "LogConfiguration": {
                                "LogDriver": "awslogs",
                                "Options": {
                                    "awslogs-group": log_group,
                                    "awslogs-region": self._aws_region,
                                    "awslogs-stream-prefix": log_prefix,
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
                "Output": "{% {'RegisteredTask': $states.result} %}",
                "Next": f"RunTask{task_index}"
            },
            f"RunTask{task_index}": {
                "Type": "Task",
                "Resource": "arn:aws:states:::ecs:runTask.sync",
                "Arguments": {
                    "Cluster": self._ecs_cluster_arn,
                    "TaskDefinition":
                        "{% $states.input.RegisteredTask.TaskDefinition.TaskDefinitionArn %}",
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
                "Output": "{% $merge([$states.input, {'ExecutionResult': $states.result}]) %}",
                "Catch": [
                    {
                        "ErrorEquals": ["States.ALL"],
                        "Output": "{% $merge([$states.input, {'ExecutionError': $states.errorOutput}]) %}",
                        "Next": f"DeregisterTask{task_index}OnFailure"
                    }
                ],
                "Next": f"DeregisterTask{task_index}OnSuccess"
            },
            f"DeregisterTask{task_index}OnSuccess": {
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:ecs:deregisterTaskDefinition",
                "Arguments": {
                    "TaskDefinition": "{% $states.input.RegisteredTask.TaskDefinition.TaskDefinitionArn %}"
                },
                ("End" if is_last else "Next"): (
                    True if is_last else f"RegisterTask{task_index + 1}"),
            },
            f"DeregisterTask{task_index}OnFailure": {
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:ecs:deregisterTaskDefinition",
                "Arguments": {
                    "TaskDefinition": "{% $states.input.RegisteredTask.TaskDefinition.TaskDefinitionArn %}"
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
        run_steps = ','.join(image_refs.keys())
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
                    run_steps=run_steps,
                    is_last=(task_index == len(groups) - 1),
                )
            )
        states["WorkflowFailed"] = {
            "Type": "Fail",
            "Cause": "The ECS container task failed execution.",
            "Error": "TaskExecutionError"
        }

        return {
            "QueryLanguage": "JSONata",
            "StartAt": "RegisterTask0",
            "States": states,
        }

    def run_pipeline(
        self,
        pipeline,
        image_refs,
        overwrite_artifacts,
        timestamp,
        run_id,
        temp_path
    ):
        workflow_name = self._make_workflow_name(None, run_id)
        workflow = self._build_workflow(
            pipeline=pipeline,
            image_refs=image_refs,
            workflow_name=workflow_name,
            timestamp=timestamp,
            run_id=run_id,
        )

        workflow_path = os.path.join(temp_path, f'{workflow_name}.json')
        with open(workflow_path, 'w') as stream:
            json.dump(workflow, stream, indent=4)

        workflow_arn = self._make_workflow_arn(workflow_name)
        if self._check_workflow_exists(workflow_name):
            run_command(
                [
                    self._aws_command,
                    'stepfunctions',
                    'update-state-machine',
                    '--profile', self._aws_profile,
                    '--region', self._aws_region,
                    '--state-machine-arn', workflow_arn,
                    '--role-arn', self._stepfunctions_role_arn,
                    '--definition', f'file://{workflow_path}',
                ],
                stdout=subprocess.DEVNULL,
            )
        else:
            run_command(
                [
                    self._aws_command,
                    'stepfunctions',
                    'create-state-machine',
                    '--profile', self._aws_profile,
                    '--region', self._aws_region,
                    '--name', workflow_name,
                    '--role-arn', self._stepfunctions_role_arn,
                    '--definition', f'file://{workflow_path}',
                ],
                stdout=subprocess.DEVNULL,
            )

        print('Clearing output and temporary artifacts.')
        for kind, artifacts in overwrite_artifacts.items():
            self.remove_remote_artifacts(
                kind, artifacts, [[run_id]]*len(artifacts))

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

    def deploy_pipeline(
        self,
        pipeline,
        image_refs,
        temp_path,
        schedule=True,
    ):
        workflow_name = self._make_workflow_name(pipeline.name, None)
        workflow = self._build_workflow(
            pipeline=pipeline,
            image_refs=image_refs,
            workflow_name=workflow_name,
            timestamp=None,
            run_id=None,
        )

        workflow_path = os.path.join(temp_path, f'{workflow_name}.json')
        with open(workflow_path, 'w') as stream:
            json.dump(workflow, stream, indent=4)

        workflow_arn = self._make_workflow_arn(workflow_name)
        if self._check_workflow_exists(workflow_name):
            run_command(
                [
                    self._aws_command,
                    'stepfunctions',
                    'update-state-machine',
                    '--profile', self._aws_profile,
                    '--region', self._aws_region,
                    '--state-machine-arn', workflow_arn,
                    '--role-arn', self._stepfunctions_role_arn,
                    '--definition', f'file://{workflow_path}',
                ],
                stdout=subprocess.DEVNULL,
            )
        else:
            run_command(
                [
                    self._aws_command,
                    'stepfunctions',
                    'create-state-machine',
                    '--profile', self._aws_profile,
                    '--region', self._aws_region,
                    '--name', workflow_name,
                    '--role-arn', self._stepfunctions_role_arn,
                    '--definition', f'file://{workflow_path}',
                ],
                stdout=subprocess.DEVNULL,
            )
        print(f'Created step function {workflow_arn}.')

        if schedule and pipeline.schedule is not None:
            if self._scheduler_timezone is None:
                raise CloudSubmitError(
                    'You must specify the schedule timezone '
                    'to use automatic scheduling.'
                )
            if self._scheduler_role_arn is None:
                raise CloudSubmitError(
                    'You must specify the scheduler role ARN '
                    'to use automatic scheduling.'
                )
            schedule_name = self._make_schedule_name(pipeline.name)
            target = json.dumps({
                'Arn': workflow_arn,
                'RoleArn': self._scheduler_role_arn,
            })
            options = [
                '--profile', self._aws_profile,
                '--region', self._aws_region,
                '--name', schedule_name,
                '--schedule-expression', f'cron({pipeline.schedule})',
                '--schedule-expression-timezone', self._scheduler_timezone,
                '--flexible-time-window', '{"Mode": "OFF"}',
                '--target', target,
            ]
            if self._check_schedule_exists(schedule_name):
                run_command(
                    [
                        self._aws_command,
                        'scheduler',
                        'update-schedule',
                        *options
                    ],
                    stdout=subprocess.DEVNULL,
                )
            else:
                run_command(
                    [
                        self._aws_command,
                        'scheduler',
                        'create-schedule',
                        *options,
                    ],
                    stdout=subprocess.DEVNULL,
                )
            print(f'Created schedule {schedule_name}.')

    def list_deployed_pipelines(self):
        prefix = '--'.join(['deployed', self._project, self._user, ''])
        workflows = self._list_workflows()
        result = []
        for workflow in workflows['stateMachines']:
            name = workflow['name']
            if name.startswith(prefix):
                result.append(name[len(prefix):])
        return result

    def undeploy_pipeline(self, pipeline):
        workflow_name = self._make_workflow_name(pipeline.name, None)
        workflow_arn = self._make_workflow_arn(workflow_name)
        if self._check_workflow_exists(workflow_name):
            run_command(
                [
                    self._aws_command,
                    'stepfunctions',
                    'delete-state-machine',
                    '--profile', self._aws_profile,
                    '--region', self._aws_region,
                    '--state-machine-arn', workflow_arn,
                ],
                stdout=subprocess.DEVNULL,
            )

        schedule_name = self._make_schedule_name(pipeline.name)
        if self._check_schedule_exists(schedule_name):
            run_command(
                [
                    self._aws_command,
                    'scheduler',
                    'delete-schedule',
                    '--profile', self._aws_profile,
                    '--region', self._aws_region,
                    '--name', schedule_name,
                ],
                stdout=subprocess.DEVNULL,
            )

    def print_logs(self, run_id, pipeline, start_timestamp, stream=False):
        print('\nStreaming logs...')
        if pipeline is None:  # print ad-hoc run logs
            log_group = self._adhoc_log_group
        else:                 # print deployed run logs
            log_group = self._deployed_log_group
        log_prefix = self._make_log_prefix(run_id, pipeline)
        if run_id is not None:
            log_prefix = log_prefix + '/'

        command = [
            self._aws_command,
            'logs',
            'tail',
            '--profile', self._aws_profile,
            '--region', self._aws_region,
            '--log-stream-name-prefix', log_prefix,
            '--since', start_timestamp.isoformat(),
            '--format', 'short',
        ]
        if stream:
            command.append('--follow')
        command.append(log_group)
        run_command(command)

    def list_runs(self, pipeline, start_timestamp):
        if pipeline is None:  # print ad-hoc run logs
            log_group = self._adhoc_log_group
        else:                 # print deployed run logs
            log_group = self._deployed_log_group
        log_prefix = self._make_log_prefix(None, pipeline)
        if start_timestamp is not None:
            start_timestamp = start_timestamp.timestamp()

        command = [
            self._aws_command,
            'logs',
            'describe-log-streams',
            '--profile', self._aws_profile,
            '--region', self._aws_region,
            '--log-group-name', log_group,
            '--log-stream-name-prefix', log_prefix,
        ]
        result = run_command(
            command,
            stdout=subprocess.PIPE,
            text=True,
        )
        result = json.loads(result.stdout)
        runs = set()
        for run in result['logStreams']:
            if start_timestamp is not None \
                    and run['firstEventTimestamp'] < start_timestamp:
                continue
            run_id = run['logStreamName']
            run_id = run_id[len(log_prefix):]
            run_id = run_id[:run_id.find('/')]
            runs.add(run_id)
        return list(runs)

