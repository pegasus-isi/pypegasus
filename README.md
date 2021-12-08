# Prerequisites

1. Python >= 3.6
1. HTCondor
1. Pegasus

# Install

```sh
pip install -e .
```

# Usage

## Task

* Annotate input files with `pypegaus.Input` type. Values can be `str` or `pathlib.Path` objects. See `task_1` in the below.
* Annotate a collection of input files with `typing.List[pypegaus.Input]` type. Values can be `str` or `pathlib.Path` objects. See `task_2` in the below.
* Annotate output files with `pypegaus.Output` type. Values can be `str` or `pathlib.Path` objects. See `task_1` in the below.
* Annotate a collection of input files with `typing.List[pypegaus.Output]` type. Values can be `str` or `pathlib.Path` objects. See `task_2` in the below.
* Body of the function is the logic of the task.

```py
@task()
def task_1(param: Any, input: Input, output: Output):
    # Read input
    # Write to output
    ...

@task()
def task_2(param: Any, input: List[Input], output: List[Output]):
    # Read input
    # Write to output
    ...
```

### Improvements

* Add support for `pip` package dependencies.
* Add support for `cores`, `memory`, etc. profiles.
* Add support for using Docker container as the task environment.

## Workflow

* Tasks invoked within workflow, will be added to those workflows.
* Workflow will be writtten, planned, run, and waited on until the execution finishes.
* All workflow files are created in `~/.pegasus/pypegasus/<uuid>` dirrectory.
* Workflow File -- `~/.pegasus/pypegasus/<uuid>/workflow.yml`
* Properties File -- `~/.pegasus/pypegasus/<uuid>/pegasus.properties`
* Submit Dir -- `~/.pegasus/pypegasus/<uuid>/submit`
* Outputs will end up in `~/.pegasus/pypegasus/<uuid>/wf-output`

### Using a decorator

```py
@workflow("adag")
def main(n: int):
    for i in range(n):
        task_1(Path(f"/../input-{i}.txt") output=f"f{i}.txt")
```

### Using a contextmanager

```py
with workflow("adag") as wf:
    for i in range(n):
        task_1(Path(f"/../input-{i}.txt") output=f"f{i}.txt")
```
