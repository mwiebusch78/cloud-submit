import cloud_submit as cs


PROJECT_NAME = 'local-linreg'


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

    train_data_af = cs.Artifact('train_data.parquet', kind='file', scope='run')
    parameters_af = cs.Artifact('parameters.json', kind='file', scope='run')

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
            'parameters_path': parameters_af.local,
        }
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
    )

    return config

