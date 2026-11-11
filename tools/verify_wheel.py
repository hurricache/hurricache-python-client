"""Install the wheel in isolated environments, with and without its LZ4 extra."""

import os
import subprocess
import venv
from pathlib import Path


def main():
    root = Path(__file__).resolve().parents[1]
    wheel = root / "dist/hurricache-0.2.0-py3-none-any.whl"
    for extra in (False, True):
        directory = root / "build" / ("wheel-0.2.0-lz4" if extra else "wheel-0.2.0-gzip")
        venv.EnvBuilder(with_pip=True, clear=False).create(directory)
        python = directory / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
        subprocess.run([python, "-m", "pip", "install", "--force-reinstall", str(wheel) + ("[lz4]" if extra else "")], check=True)
        subprocess.run([python, "-m", "pip", "check"], check=True)
        subprocess.run([python, "-I", root / "tools/smoke_install.py", *(["--lz4"] if extra else [])], cwd=directory, check=True)


if __name__ == "__main__":
    main()
