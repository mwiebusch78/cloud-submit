import os
import sys
import shutil
import subprocess
import json
import datetime as dt

from cloud_submit import (
    EnvironmentHandler,
    ensure_path,
    CloudSubmitError,
    build_docker_mount_option,
    run_command,
)


def build_artifacts_mount_option(path, scope):
    return build_docker_mount_option(path, f'/root/artifacts/{scope}')


class LocalAWSEnv(EnvironmentHandler):
    def __init__(
        self,
        name,
        project,
        user,
        aws_account_id,
        aws_region,
        aws_profile,
        s3_prefix,
        docker_command='docker',
        docker_namespace='csub',
        docker_login_refresh_hours=6,
        aws_command='aws',
    ):
        EnvironmentHandler.__init__(
            self,
            name=name,
            project=project,
            user=user,
            docker_command=docker_command,
            docker_registry=\
                f'{aws_account_id}.dkr.ecr.{aws_region}.amazonaws.com',
            docker_namespace=docker_namespace,
        )
        self._aws_account_id = str(aws_account_id)
        self._aws_region = str(aws_region)
        self._aws_profile = str(aws_profile)
        self._s3_prefix = str(s3_prefix)
        self._aws_command = str(aws_command)
        self._docker_login_refresh_hours = float(docker_login_refresh_hours)

        self._last_docker_login_ts = None

    def _docker_login(self):
        now = dt.datetime.now(tz=dt.UTC)
        if self._last_docker_login_ts is not None:
            seconds_since_last_login = \
                (now - self._last_docker_login_ts).total_seconds()
            if seconds_since_last_login < self._docker_login_refresh_hours*3600:
                return
            
        result = run_command(
            [
                self._aws_command,
                'ecr',
                'get-login-password',
                '--profile', self._aws_profile,
                '--region', self._aws_region,
            ],
            stdout=subprocess.PIPE,
            text=True,
        )
        password = result.stdout.strip()

        run_command([
            self._docker_command,
            'login',
            '--username', 'AWS',
            '--password', password,
            self._docker_registry,
        ])
        self._last_docker_login_ts = now

    def install_execution_handler(self, path):
        sourcedir = os.path.dirname(__file__)
        shutil.copyfile(
            os.path.join(sourcedir, 'local_execution_handler.py'),
            os.path.join(path, 'execution_handler.py'),
        )

    def build_image(self, path, image, build_id):
        ref = EnvironmentHandler.build_image(self, path, image, build_id)
        self._docker_login()

        # push image
        run_command([self._docker_command, 'push', ref])

        # get image digest
        result = run_command(
            [
                self._docker_command,
                'buildx', 'imagetools', 'inspect',
                '--format', '{{json .Manifest.Digest}}',
                ref,
            ],
            stdout=subprocess.PIPE,
            text=True,
        )
        digest = result.stdout.strip('"')
        return ref + '@' + digest

    def pull_image(self, ref):
        self._docker_login()
        run_command([
            self._docker_command,
            'pull',
            ref,
        ])

    def list_remote_image_tags(self, repo_name):
        self._docker_login()
        repo_name = '/'.join(repo_name.split('/')[1:])
        result = run_command(
            [
                self._aws_command,
                'ecr',
                'list-images',
                '--region', self._aws_region,
                '--profile', self._aws_profile,
                '--filter', 'tagStatus=TAGGED',
                '--repository-name', repo_name,
            ],
            stdout=subprocess.PIPE,
            text=True,
        )
        result = json.loads(result.stdout)
        tags = [img['imageTag'] for img in result['imageIds']]
        return tags

    def remove_remote_image_refs(self, refs):
        raise CloudSubmitError(
            'Removing remote images is not supported by local environment.'
        )

    def submit(self, pipeline, image_refs, timestamp, run_id):
        artifacts_project_path = os.path.join('artifacts', 'shared')
        artifacts_user_path = os.path.join(
            'artifacts', 'users', self._user, 'shared')
        artifacts_run_path = os.path.join(
            'artifacts', 'users', self._user, 'runs', pipeline.name, run_id)
        ensure_path(artifacts_project_path)
        ensure_path(artifacts_user_path)
        ensure_path(artifacts_run_path)
        for step in pipeline.steps:
            if step.name not in image_refs:
                continue
            ref = image_refs[step.name]

            worker_indices = [-1]
            if step.num_workers is not None:
                worker_indices = range(step.num_workers)

            for worker_index in worker_indices:
                run_command([
                    'docker',
                    'run',
                    '--rm',
                    '--mount',
                    build_artifacts_mount_option(
                        artifacts_project_path, 'project'),
                    '--mount',
                    build_artifacts_mount_option(artifacts_user_path, 'user'),
                    '--mount',
                    build_artifacts_mount_option(artifacts_run_path, 'run'),
                    '--env', f'CSUB_TIMESTAMP={timestamp.isoformat()}',
                    '--env', f'CSUB_RUN_ID={run_id}',
                    '--env', f'CSUB_WORKER_INDEX={worker_index}',
                    ref,
                    pipeline.name,
                    step.name,
                ])
