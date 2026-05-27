import sys
import importlib
import copy
import datetime as dt

from .config import read_pipelines
from .execution_handler import create_execution_handler


if __name__ == '__main__':
    if len(sys.argv) != 3:
        raise SystemExit(
            'Invalid number of command line arguments. '
            'Expected PIPELINE and STEPS.'
        )
    pipeline_name = sys.argv[1]
    steps = set(sys.argv[2].split(','))

    pipelines = read_pipelines()
    try:
        pipeline = pipelines[pipeline_name]
    except KeyError:
        raise SystemExit(
            f'Invalid pipeline name: {pipeline_name}')

    eh = create_execution_handler()
    timestamp = eh.get_submit_timestamp()
    run_id = eh.get_run_id()
    worker_index = eh.get_worker_index()

    synced_artifacts = set()

    for step in pipeline:
        if step.name not in steps:
            continue
        print(f'Executing step: {step.name}')
        for loc in step.temporaries.values():
            eh.clear_artifact(loc.artifact)
        for loc in step.outputs.values():
            eh.clear_artifact(loc.artifact)
        for loc in step.inputs.values():
            if loc.location_type == 'local' and \
                    loc.artifact.name not in synced_artifacts:
                eh.sync_artifact_location(loc)
                synced_artifacts.add(loc.artifact.name)

        module_name, function_name = step.function.split(':')
        module = importlib.import_module(module_name)
        function = getattr(module, function_name)

        kwargs = copy.deepcopy(step.params)
        for kw, loc in step.inputs.items():
            kwargs[kw] = eh.get_artifact_location(loc)
        for kw, loc in step.outputs.items():
            kwargs[kw] = eh.get_artifact_location(loc)
        for kw, loc in step.temporaries.items():
            kwargs[kw] = eh.get_artifact_location(loc)
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
                l.artifact.name for l in step.outputs.values()
                if l.location_type == 'remote'
            )
            for loc in step.outputs.values():
                if loc.location_type == 'local':
                    synced_artifacts.add(loc.artifact.name)
                    if loc.artifact.name not in remote_outputs:
                        eh.sync_artifact_location(loc.artifact.remote)

