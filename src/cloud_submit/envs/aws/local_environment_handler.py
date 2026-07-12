import os
import sys
import shutil
import subprocess
import json
import datetime as dt
import re

from cloud_submit import (
    LocalEnv,
    ensure_path,
    CloudSubmitError,
    build_docker_mount_option,
    run_command,
    parse_image_ref,
    BaseImage,
)

from .s3_tools import  get_remote_artifact_path



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
        s3_bucket,
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
        self._s3_bucket = str(s3_bucket)
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
            hide_stderr=True,
        )
        password = result.stdout.strip()

        run_command(
            [
                self._docker_command,
                'login',
                '--username', 'AWS',
                '--password', password,
                self._docker_registry,
            ],
            hide_stderr=True,
            stdout=subprocess.DEVNULL,
        )
        self._last_docker_login_ts = now

    def install_execution_handler(self, path):
        sourcedir = os.path.dirname(__file__)
        shutil.copyfile(
            os.path.join(sourcedir, 'local_execution_handler.py'),
            os.path.join(path, 'execution_handler.py'),
        )

    def _build_image(self, path, image, build_id, push):
        ref = super().build_image(path, image, build_id)
        self._docker_login()

        if push:
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
        return ref

    def build_image(self, path, image, build_id):
        return self._build_image(
            path=path,
            image=image,
            build_id=build_id, 
            push=isinstance(image, BaseImage),
        )

    def pull_base_image(self, ref):
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

    def get_remote_artifact_path(self, artifact, run_id=None):
        return get_remote_artifact_path(
            artifact,
            self._project,
            self._user,
            run_id,
            self._s3_bucket,
            self._s3_prefix,
        )

    def _get_s3_objects(self, prefix, recursive=False):
        command = [
            self._aws_command,
            's3',
            'ls',
            '--profile', self._aws_profile,
            '--region', self._aws_region,
        ]
        if recursive:
            command.append('--recursive')
        command.append(prefix)
        result = run_command(
            command,
            check=False,
            stdout=subprocess.PIPE,
            text=True,
        )
        lines = result.stdout.strip()
        if not lines:
            return []
        lines = lines.split('\n')
        return [l.split()[-1] for l in lines]

    def list_remote_artifacts(self, artifacts, run_ids=None):
        if run_ids is not None:
            run_ids = set(run_ids)

        project_artifacts = set()
        user_artifacts = set()
        run_artifacts = set()
        for a in artifacts:
            if a.kind != 'file':
                continue
            if a.scope == 'project':
                project_artifacts.add(a.name)
            elif a.scope == 'user':
                user_artifacts.add(a.name)
            elif a.scope == 'run':
                run_artifacts.add(a.name)

        results = {a.name: set() for a in artifacts}
        if project_artifacts:
            keys = self._get_s3_objects('/'.join([
                's3:/', self._s3_bucket, self._s3_prefix, self._project,
                'shared/',
            ]))
            for key in keys:
                if key.endswith('/'):
                    key = key[:-1]
                artifact = key
                if artifact in project_artifacts:
                    results[artifact].add(None)
        if user_artifacts:
            keys = self._get_s3_objects('/'.join([
                's3:/', self._s3_bucket, self._s3_prefix, self._project,
                'users', self._user, 'shared/',
            ]))
            for key in keys:
                if key.endswith('/'):
                    key = key[:-1]
                artifact = key
                if artifact in user_artifacts:
                    results[artifact].add(None)
        if run_artifacts:
            prefix = '/'.join(
                [self._s3_prefix, self._project, 'users', self._user, 'runs/'])
            keys = self._get_s3_objects(
                '/'.join(['s3:/', self._s3_bucket, prefix]),
                recursive=True,
            )
            for key in keys:
                key = key[len(prefix):].split('/')
                run_id = key[0]
                if run_ids is not None and run_id not in run_ids:
                    continue
                if len(key) > 1 and key[1] in run_artifacts:
                    artifact = key[1]
                    results[artifact].add(run_id)
    
        results = [sorted(results[a.name]) for a in artifacts]
        return results

    def remove_remote_artifacts(self, artifacts, run_ids):
        path = '/'.join(
            ['s3:/', self._s3_bucket, self._s3_prefix, self._project])
        command = [
            self._aws_command,
            's3',
            'rm',
            '--profile', self._aws_profile,
            '--region', self._aws_region,
            '--recursive',
            path,
            '--exclude', '*',
        ]
        for artifact, runs in zip(artifacts, run_ids):
            if artifact.kind != 'file':
                continue
            if artifact.scope == 'project':
                for run_id in runs:
                    command.extend([
                        '--include',
                        '/'.join(['shared', artifact.name]),
                        '--include',
                        '/'.join(['shared', artifact.name, '*']),
                    ])
            elif artifact.scope == 'user':
                for run_id in runs:
                    command.extend([
                        '--include',
                        '/'.join([
                            'users', self._user, 'shared', artifact.name]),
                        '--include',
                        '/'.join([
                            'users', self._user, 'shared', artifact.name, '*']),
                    ])
            elif artifact.scope == 'run':
                for run_id in runs:
                    command.extend([
                        '--include',
                        '/'.join([
                            'users', self._user, 'runs', run_id,
                            artifact.name,
                        ]),
                        '--include',
                        '/'.join([
                            'users', self._user, 'runs', run_id,
                            artifact.name, '*',
                        ]),
                    ])

        run_command(command)

    def push_artifacts(self, artifacts, run_ids):
        self.remove_remote_artifacts(artifacts, run_ids)

        remote_path = '/'.join(
            ['s3:/', self._s3_bucket, self._s3_prefix, self._project])
        local_path = 'artifacts'
        project_dir = 'shared'
        user_dir = os.path.join('users', self._user, 'shared')
        run_dir = lambda run_id: os.path.join(
            'users', self._user, 'runs', run_id)

        command = [
            self._aws_command,
            's3',
            'cp',
            '--profile', self._aws_profile,
            '--region', self._aws_region,
            '--recursive',
            local_path,
            remote_path,
            '--exclude', '*',
        ]
        for artifact, runs in zip(artifacts, run_ids):
            if artifact.kind != 'file':
                continue
            if artifact.scope == 'project':
                for run_id in runs:
                    command.extend([
                        '--include',
                        os.path.join(project_dir, artifact.name),
                        '--include',
                        os.path.join(project_dir, artifact.name, '*'),
                    ])
            elif artifact.scope == 'user':
                for run_id in runs:
                    command.extend([
                        '--include',
                        os.path.join(user_dir, artifact.name),
                        '--include',
                        os.path.join(user_dir, artifact.name, '*'),
                    ])
            elif artifact.scope == 'run':
                for run_id in runs:
                    command.extend([
                        '--include',
                        os.path.join(run_dir(run_id), artifact.name),
                        '--include',
                        os.path.join(run_dir(run_id), artifact.name, '*'),
                    ])

        run_command(command)

    def pull_artifacts(self, artifacts, run_ids):
        self.remove_local_artifacts(artifacts, run_ids)

        remote_path = '/'.join(
            ['s3:/', self._s3_bucket, self._s3_prefix, self._project])
        local_path = 'artifacts'
        project_dir = 'shared'
        user_dir = os.path.join('users', self._user, 'shared')
        run_dir = lambda run_id: os.path.join(
            'users', self._user, 'runs', run_id)

        command = [
            self._aws_command,
            's3',
            'cp',
            '--profile', self._aws_profile,
            '--region', self._aws_region,
            '--recursive',
            remote_path,
            local_path,
            '--exclude', '*',
        ]
        for artifact, runs in zip(artifacts, run_ids):
            if artifact.kind != 'file':
                continue
            if artifact.scope == 'project':
                for run_id in runs:
                    command.extend([
                        '--include',
                        '/'.join([project_dir, artifact.name]),
                        '--include',
                        '/'.join([project_dir, artifact.name, '*']),
                    ])
            elif artifact.scope == 'user':
                for run_id in runs:
                    command.extend([
                        '--include',
                        '/'.join([user_dir, artifact.name]),
                        '--include',
                        '/'.join([user_dir, artifact.name, '*']),
                    ])
            elif artifact.scope == 'run':
                for run_id in runs:
                    command.extend([
                        '--include',
                        '/'.join([run_dir(run_id), artifact.name]),
                        '--include',
                        '/'.join([run_dir(run_id), artifact.name, '*']),
                    ])

        run_command(command)
