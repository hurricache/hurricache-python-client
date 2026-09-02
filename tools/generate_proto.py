"""Regenerate or verify the committed protobuf/gRPC Python modules."""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PROTO = ROOT / "proto"
OUTPUT = ROOT / "hurricache" / "grpc"
FILES = ("cache_pb2.py", "cache_pb2_grpc.py", "coordinator_pb2.py", "coordinator_pb2_grpc.py")
SCRATCH = ROOT / "tools" / "_generated"


def _normalize(name: str, text: str) -> str:
    text = text.replace("import cache_pb2 as cache__pb2", "from . import cache_pb2 as cache__pb2")
    text = text.replace("import coordinator_pb2 as coordinator__pb2", "from . import coordinator_pb2 as coordinator__pb2")
    text = text.replace("import warnings\n", "")
    text = re.sub(r"from google\.protobuf import runtime_version as _runtime_version\n", "", text)
    text = re.sub(r"_runtime_version\.ValidateProtobufRuntimeVersion\(.*?\n\)\n", "", text, flags=re.DOTALL)
    if name.endswith("_pb2_grpc.py"):
        text = re.sub(r"GRPC_GENERATED_VERSION = .*?\n\n(?=class )", "", text, flags=re.DOTALL)
        text = re.sub(r",\n\s+_registered_method=True", "", text)
        text = re.sub(
            r"(?m)^    server\.add_registered_method_handlers\((.*)\)$",
            '    if hasattr(server, "add_registered_method_handlers"):\n'
            r"        server.add_registered_method_handlers(\1)",
            text,
        )
    return text


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true", help="fail if committed files differ")
    args = parser.parse_args()
    subprocess.run(
        [
            sys.executable,
            "-m",
            "grpc_tools.protoc",
            f"-I{PROTO}",
            f"--python_out={SCRATCH}",
            f"--grpc_python_out={SCRATCH}",
            str(PROTO / "cache.proto"),
            str(PROTO / "coordinator.proto"),
        ],
        check=True,
    )
    changed: list[str] = []
    for name in FILES:
        generated = _normalize(name, (SCRATCH / name).read_text(encoding="utf-8"))
        destination = OUTPUT / name
        if not destination.exists() or destination.read_text(encoding="utf-8") != generated:
            changed.append(name)
            if not args.check:
                destination.write_text(generated, encoding="utf-8", newline="\n")
    if args.check and changed:
        print("generated protobuf files are stale: " + ", ".join(changed), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
