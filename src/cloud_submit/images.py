import os


class Image:
    def __init__(
        self,
        name,
        parent=None,
        instructions=None,
        setup_builddir_callback=None,
    ):
        self.name = name
        self.parent = parent
        self._instructions = instructions if instructions is not None else ''
        self._setup_builddir_callback = setup_builddir_callback

    def setup_builddir(self, path, base_image):
        if self._setup_builddir_callback is not None:
            self._setup_builddir_callback(path, mode)
        with open(os.path.join(path, 'Dockerfile'), 'w') as dockerfile:
            if self.parent is not None:
                dockerfile.write(f'FROM {base_image}\n\n')
            dockerfile.write(self._instructions)
            dockerfile.write('\n')


class BaseImage(Image):
    pass


class ExecutionImage(Image):
    def __init__(
        self,
        name,
        parent=None,
        instructions=None,
        setup_builddir_callback=None,
        python_cmd='python',
    ):
        super().__init__(
            name=name,
            parent=parent,
            instructions=instructions,
            setup_builddir_callback=setup_builddir_callback,
        )
        self._python_cmd = python_cmd

    def setup_builddir(self, path, base_image):
        super().setup_builddir(path, base_image)
        with open(os.path.join(path, 'Dockerfile'), 'a') as dockerfile:
            dockerfile.write(
                '\nCOPY --chown=root:root src /root/src/\n'
                'RUN mkdir -p /root/artifacts/run && '
                    'mkdir -p /root/artifacts/user && '
                    'mkdir -p /root/artifacts/project\n'
                'ENV PYTHONPATH="/root/src:$PYTHONPATH"\n'
                'WORKDIR /root\n'
                f'ENTRYPOINT ["/usr/bin/env", "{self._python_cmd}", "-u", '
                '"-m", "csub.execute"]\n'
            )

