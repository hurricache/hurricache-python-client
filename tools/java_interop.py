"""Build pinned Java sources and exchange codec fixtures without a server.

This validates raw compression and ordered-map observer interoperability. It does
not replace live standalone validation. Requires JDK 17+, Maven and the Java
checkout's protoc plugins (populated by its normal Maven build).
"""

import argparse
import os
import shutil
import subprocess
import xml.etree.ElementTree as ET
from pathlib import Path

from hurricache import HurriCacheClient, OrderedPayload, Payload
from hurricache.grpc import cache_pb2 as pb
from hurricache.grpc.operation import operation
from hurricache.grpc.utils import create_ordered_key, create_value, decode_batch
from tools.parity import BASELINE

ROOT = Path(__file__).resolve().parents[1]


def data(size):
    return bytes(i % 251 for i in range(size))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("java_checkout", type=Path)
    parser.add_argument("--java-home", type=Path, required=True)
    args = parser.parse_args()
    checkout = args.java_checkout.resolve()
    assert subprocess.check_output(["git", "-C", str(checkout), "rev-parse", "HEAD"], text=True).strip() == BASELINE
    work = ROOT / "build/java-interop"
    work.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ, JAVA_HOME=str(args.java_home))
    env["PATH"] = str(args.java_home / "bin") + os.pathsep + env["PATH"]

    def run(command):
        subprocess.run([str(item) for item in command], check=True, env=env)

    pom = ET.parse(checkout / "pom.xml")
    ET.register_namespace("", "http://maven.apache.org/POM/4.0.0")
    ns = {"m": "http://maven.apache.org/POM/4.0.0"}
    for build in pom.getroot().findall("m:build", ns):
        pom.getroot().remove(build)
    pom.write(work / "pom.xml", encoding="utf-8", xml_declaration=True)
    run([shutil.which("mvn"), "-q", "-f", work / "pom.xml", "dependency:build-classpath",
         f"-Dmdep.outputFile={work / 'classpath.txt'}"])
    classpath = (work / "classpath.txt").read_text().strip()
    generated = work / "generated"
    generated.mkdir(exist_ok=True)
    suffix = "windows-x86_64.exe" if os.name == "nt" else "linux-x86_64.exe"
    plugins = checkout / "target/protoc-plugins"
    protoc = sorted(plugins.glob(f"protoc-[0-9]*-{suffix}"))[-1]
    grpc_plugin = sorted(plugins.glob(f"protoc-gen-grpc-java-*-{suffix}"))[-1]
    proto = checkout / "src/main/proto"
    run([protoc, f"-I{proto}", f"--java_out={generated}", f"--grpc-java_out={generated}",
         f"--plugin=protoc-gen-grpc-java={grpc_plugin}", proto / "cache.proto", proto / "coordinator.proto"])
    java = checkout / "src/main/java/com/hurricache"
    sources = [*(java / "client/intf").glob("*.java"), *(java / "utils").glob("*.java"),
               *(generated.rglob("*.java")), ROOT / "tools/CodecInterop.java"]
    source_list = work / "sources.txt"
    source_list.write_text("\n".join('"' + p.as_posix() + '"' for p in sources), encoding="utf-8")
    run([args.java_home / "bin/javac", "-encoding", "UTF-8", "-cp", classpath, "-d", work, "@" + str(source_list)])
    for codec in ("gzip", "lz4"):
        fixture = work / f"{codec}.pb"
        command = [args.java_home / "bin/java", f"-Dhurricache.compression={codec}",
                   "-cp", str(work) + os.pathsep + classpath, "CodecInterop"]
        run([*command, "write", fixture])
        with operation(HurriCacheClient(payload_compression=codec), 10):
            batch = pb.BatchValueResponse.FromString(fixture.read_bytes())
            decoded = decode_batch(batch)
            assert list(decoded) == [OrderedPayload(data(4098), 2), OrderedPayload(data(4097), 1)]
            assert list(decoded.values()) == [Payload(data(4098)), Payload(data(4097))]
            batch = pb.BatchValueResponse(
                key_ordered=[create_ordered_key(OrderedPayload(data(4096 + i), i)) for i in (2, 1)],
                value_unordered=[create_value(data(4096 + i), compress=True) for i in (2, 1)])
            fixture.write_bytes(batch.SerializeToString())
        run([*command, "read", fixture])
    print("Bidirectional Java/Python GZIP and raw LZ4 ordered-map fixtures passed")


if __name__ == "__main__":
    main()
