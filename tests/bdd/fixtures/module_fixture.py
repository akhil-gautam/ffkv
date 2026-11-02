"""Compile the independently authored public-ABI fixture as a native library.

Called by the isolated BDD runtime, never launches a server or uses user data.
The parent test runner owns compilation and every runtime check.
"""
from pathlib import Path
import os
import shlex
import subprocess
import sys


def build_module(directory):
    root = Path(__file__).resolve().parent
    output = Path(directory) / ("module_fixture.dylib" if sys.platform == "darwin" else "module_fixture.so")
    command = shlex.split(os.environ.get("CC", "cc"))
    command += ["-std=c11", "-D_POSIX_C_SOURCE=200809L", "-O2", "-fPIC"]
    command += ["-dynamiclib" if sys.platform == "darwin" else "-shared"]
    command += [str(root / "module_fixture.c"), "-o", str(output)]
    subprocess.run(command, check=True, capture_output=True, text=True)
    return output
