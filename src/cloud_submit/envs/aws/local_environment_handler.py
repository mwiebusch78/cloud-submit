import os
import sys
import shutil
import subprocess
import json
import datetime as dt

from cloud_submit import (
    LocalEnv,
    ensure_path,
    CloudSubmitError,
    build_docker_mount_option,
    run_command,
    parse_image_ref,
)


def build_artifacts_mount_option(path, scope):
    return build_docker_mount_option(path, f'/root/artifacts/{scope}')


class LocalAWSEnv(LocalEnv):
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
        super().__init__(
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
        ref = super().build_image(path, image, build_id)
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
        tags = {}
        for ref in refs:
            registry, repo, tag, digest = parse_image_ref(ref)
            if registry != self._docker_registry:
                raise CloudSubmitError(
                    f'Cannot delete image ref {ref} due to unknown registry.')
            tags[repo] = tags.get(repo, [])
            tags[repo].append(tag)

        for repo, tag_list in tags.items():
            run_command([
                self._aws_command,
                'ecr',
                'batch-delete-image',
                '--region', self._aws_region,
                '--profile', self._aws_profile,
                '--repository-name', repo,
                '--image-ids',
                *[f'imageTag={tag}' for tag in tag_list],
            ])
