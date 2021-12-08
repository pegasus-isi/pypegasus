import inspect
import json
import shlex
from contextlib import contextmanager
from functools import wraps
from inspect import getmodule, isbuiltin, isfunction, ismodule
from pathlib import Path
from typing import Dict, List, NewType, Union
from uuid import uuid4

import cloudpickle
from Pegasus.api import (
    OS,
    Arch,
    Job,
    Properties,
    ReplicaCatalog,
    Site,
    SiteCatalog,
    Transformation,
    TransformationCatalog,
    Workflow,
)

from . import __exec__

Input = NewType("Input", Union[str, Path])
Output = NewType("Output", Union[str, Path])


_stack: List[Workflow] = []
_tr: Dict[str, Transformation] = {}


def _process_closure(c):
    mods = set()
    for v in c:
        v = v.cell_contents
        if ismodule(v):
            mods.add(v)
        elif isfunction(v):
            if not v.__module__.startswith("pypegasus.") and v.__module__ != "typing":
                print("Function", v.__module__)
                mods.update(_get_modules(v))

    return mods


def _get_modules(f, a=None):
    print("@" * 10, f.__code__.co_names, "-", f.__closure__)

    for k in dir(f.__code__):
        print(k, getattr(f.__code__, k))

    mods = {getmodule(f)}
    if f.__closure__:
        mods.update(_process_closure(f.__closure__))

    isclass = False
    if f.__code__.co_varnames and f.__code__.co_varnames[0] == "self":
        isclass = True

    for k in f.__code__.co_names:
        if isclass and a and hasattr(a[0], k):
            # Instance variable, do not look in __globals__
            print("--", k, getattr(a[0], k))
            continue

        v = f.__globals__.get(k, None)
        print("-", k, v)
        if v and not isbuiltin(v):
            if ismodule(v):
                print("Module", k)
                mods.add(v)
            elif isfunction(v):
                if (
                    v.__module__ != f.__module__
                    and not v.__module__.startswith("pypegasus.")
                    and v.__module__ != "typing"
                ):
                    print("Function", k, v.__module__)
                    mods.update(_get_modules(v))

    return mods


def _create_tr(f, a, kw):
    with f.__location__.open("wb") as _:
        mods = _get_modules(f)
        print([_.__name__ for _ in mods])

        for m in mods:
            cloudpickle.register_pickle_by_value(m)

        s = cloudpickle.dumps(f)
        _.write(s)

        for m in mods:
            cloudpickle.unregister_pickle_by_value(m)

    mods = _get_modules(f, a)
    print([_.__name__ for _ in mods])

    for m in mods:
        cloudpickle.register_pickle_by_value(m)

    s = cloudpickle.dumps((f, a, kw))

    for m in mods:
        cloudpickle.unregister_pickle_by_value(m)

    tr = Transformation(
        f.__name__,
        site="local",
        pfn=f.__location__.resolve(),
        is_stageable=True,
    )

    return tr


def task(*args, **kwargs):
    """Pegasus Python Task."""

    def decorator(f):
        @wraps(f)
        def wrapped_f(*a, **kw):
            wf = _stack[-1]

            (wf.base_dir / "tr").mkdir(parents=True, exist_ok=True)
            if f.__name__ not in _tr:
                f.__location__ = (wf.base_dir / "tr" / f.__name__).resolve()
                tr = _create_tr(f, a, kw)

                _tr[f.__name__] = _pypegasus = Transformation(
                    f"{f.__name__}_pypegasus",
                    site="local",
                    pfn=__exec__.__file__,
                    is_stageable=True,
                ).add_requirement(tr)

                wf.transformation_catalog.add_transformations(_pypegasus, tr)

            _pypegasus = _tr[f.__name__]

            j = (
                Job(_pypegasus.name)
                .add_args(f.__name__)
                .add_args(
                    shlex.quote(
                        json.dumps(
                            list(str(_) if isinstance(_, Path) else _ for _ in a),
                            default=vars,
                        )
                    )
                )
                .add_args(
                    shlex.quote(
                        json.dumps(
                            {
                                k: str(v) if isinstance(v, Path) else v
                                for k, v in kw.items()
                            },
                            default=vars,
                        )
                    )
                )
            )

            s = inspect.signature(f)
            for v, t in f.__annotations__.items():
                i = 0
                for m, p in s.parameters.items():
                    if m == v:
                        if p.kind == p.POSITIONAL_ONLY:
                            v = a[i]
                        if p.kind == p.POSITIONAL_OR_KEYWORD:
                            v = kw[v] if v in kw else a[i]
                        else:
                            v = kw[v]
                        break

                    i += 1

                if t == Input or t == List[Input]:
                    if not isinstance(v, list):
                        v = [v]

                    v = [Path(_) if isinstance(_, str) else _ for _ in v]

                    for _ in v:
                        name = _.name if _.is_absolute() else str(_)

                        if _.is_file() and _.exists():
                            wf.replica_catalog.add_replica(
                                "local", name, str(_.resolve())
                            )

                        j.add_inputs(name)

                elif t == Output or t == List[Output]:
                    if not isinstance(v, list):
                        v = [v]

                    names = [str(_) if isinstance(_, Path) else _ for _ in v]

                    j.add_outputs(
                        *names,
                        stage_out=True,
                        register_replica=False,
                    )

            wf.add_jobs(j)
            return j

        return wrapped_f

    return decorator


@contextmanager
def workflow(name, *args, **kwargs):
    """Workflow."""
    try:
        uuid = str(uuid4())

        wf = Workflow(name)
        wf.base_dir = base_dir = (
            Path.home() / ".pegasus" / "pypegasus" / uuid
        ).resolve()

        # Catalogs
        wf.add_site_catalog(_generate_sc())
        wf.add_replica_catalog(ReplicaCatalog())
        wf.add_transformation_catalog(TransformationCatalog())

        # Push workflow on context
        _stack.append(wf)

        yield wf

        _generate_properties().write((base_dir / "pegasus.properties").open("w"))
        wf.write((base_dir / "workflow.yml").open("w")).plan(
            dir=base_dir,
            output_dir=(base_dir / "wf-output").resolve(),
            relative_submit_dir="submit",
            verbose=1,
        ).run().wait()
    except Exception:
        raise
    finally:
        # Push workflow from context
        _stack.pop()


def _generate_properties():
    """Generate properties."""
    props = Properties()
    # props["pegasus.mode"] = "development"
    props["pegasus.integrity.checking"] = "none"
    return props


def _generate_sc():
    """Generate site catalog."""
    sc = SiteCatalog()

    condorpool = (
        Site("condorpool", arch=Arch.X86_64, os_type=OS.LINUX)
        .add_pegasus_profile(style="condor")
        .add_condor_profile(universe="local")
    )

    sc.add_sites(condorpool)

    return sc
