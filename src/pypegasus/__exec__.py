#!/usr/bin/env python3
"""Py pegasus_example Example."""

import json
import sys
from pathlib import Path
import base64 as b64
import pip


def install():
    """Install cloudpickle package."""
    try:
        import cloudpickle  # noqa: F401
    except ModuleNotFoundError:
        pkg = "cloudpickle"
        if hasattr(pip, "main"):
            pip.main(["install", "-t", "__xyz__", pkg])
        else:
            pip._internal.main(["install", "-t", "__xyz__", pkg])

        sys.path.insert(0, str(Path("__xyz__").resolve()))


def main():
    """Do main."""
    import cloudpickle

    f = cloudpickle.loads(open(sys.argv[1], "rb").read())
    a = json.loads(sys.argv[2])
    kw = json.loads(sys.argv[3])
    print(f.__module__, f.__name__, a, kw)
    f(*a, **kw)

    # print(sys.argv[1])
    # print(bytes(sys.argv[1], "utf-8"))
    # print(b64.b64decode(bytes(sys.argv[1], "utf-8")))
    # f, a, kw = cloudpickle.loads(b64.b64decode(bytes(sys.argv[1], "utf-8")))
    # print(f.__module__, f.__name__, a, kw)
    # f(*a, **kw)


if __name__ == "__main__":
    install()
    main()
