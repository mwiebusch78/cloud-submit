import os
import sys
import importlib
import copy
import datetime as dt

from .config import read_pipelines, read_artifacts
from .execution_handler import create_execution_handler


if __name__ == '__main__':
    if len(sys.argv) != 3:
        raise SystemExit(
            'Invalid number of command line arguments. '
            'Expected PIPELINE and STEPS.'
        )
    pipeline_name = sys.argv[1]
    steps = set(sys.argv[2].split(','))

    os.makedirs('/mnt/artifacts/project', exist_ok=True)
    os.makedirs('/mnt/artifacts/user', exist_ok=True)
    os.makedirs('/mnt/artifacts/run', exist_ok=True)

    pipelines = read_pipelines()
    artifacts = read_artifacts()
    try:
        pipeline = pipelines[pipeline_name]
    except KeyError:
        raise SystemExit(
            f'Invalid pipeline name: {pipeline_name}')

    def get_artifact(name):
        try:
            return artifacts[name]
        except KeyError:
            raise SystemExit(f'Invalid artifact name {name}')

    def get_artifact_path(eh, loc):
        artifact = get_artifact(loc.artifact_name)
        if loc.is_local:
            return eh.get_local_artifact_path(artifact)
        return eh.get_remote_artifact_path(artifact)

    eh = create_execution_handler()
    timestamp = eh.get_submit_timestamp()
    run_id = eh.get_run_id()
    worker_index = eh.get_worker_index()

    synced_artifacts = set()

    for step in pipeline:
        if step.name not in steps:
            continue
        print(f'Executing step: {step.name}')
        for loc in step.inputs.values():
            if loc.is_local and loc.artifact_name not in synced_artifacts:
                artifact = get_artifact(loc.artifact_name)
                eh.download_artifact(artifact)
                synced_artifacts.add(loc.artifact_name)

        module_name, function_name = step.function.split(':')
        module = importlib.import_module(module_name)
        function = getattr(module, function_name)

        kwargs = copy.deepcopy(step.params)
        for kw, loc in step.inputs.items():
            kwargs[kw] = get_artifact_path(eh, loc)
        for kw, loc in step.outputs.items():
            kwargs[kw] = get_artifact_path(eh, loc)
        for kw, loc in step.temporaries.items():
            kwargs[kw] = get_artifact_path(eh, loc)
        if step.num_workers is not None:
            if worker_index < 0:
                raise SystemExit(
                    f'Invalid worker index {worker_index} in step {step.name}.'
                )
            kwargs[step.pass_num_workers_as] = step.num_workers
            kwargs[step.pass_worker_index_as] = worker_index
        if step.pass_submit_timestamp_as is not None:
            kwargs[step.pass_submit_timestamp_as] = timestamp
        if step.pass_run_id_as is not None:
            kwargs[step.pass_run_id_as] = run_id

        try:
            function(**kwargs)
        finally:
            remote_outputs = set(
                l.artifact_name for l in step.outputs.values()
                if not l.is_local
            )
            for loc in step.outputs.values():
                artifact = get_artifact(loc.artifact_name)
                if loc.is_local:
                    synced_artifacts.add(loc.artifact_name)
                    if loc.artifact_name not in remote_outputs:
                        eh.upload_artifact(artifact)

