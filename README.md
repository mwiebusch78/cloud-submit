# cloud-submit

A tool for packaging Python code with Docker and executing it in local and
cloud environments.

## Installation

```bash
$ pip install cloud-submit
```

## Documentation

This package provides a Python module called `cloud_submit` as well as a
command-line tool called `csub`.

Have a look at the
[examples](https://github.com/mwiebusch78/cloud-submit/tree/main/docs/examples)
to see how to set up a project directory. To run any of the examples you will
have to copy `userconfig/template.yaml` to `userconfig/default.yaml` and fill
in the blanks.

To run the AWS example you need to have the AWS CLI (v2) installed locally
and you need an AWS account. The comments in `userconfig/template.yaml` tell
you how you need to configure your AWS account so that the example can run.

Run `csub --help` to explore the functionality of `csub` and its subcommands.

Proper documentation will be added as soon as I have the time. (Promise!)
