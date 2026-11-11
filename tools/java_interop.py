"""Build pinned Java sources and exchange codec fixtures without a server.

This validates raw compression and ordered-map observer interoperability. It does
not replace live standalone validation. Requires JDK 17+, Maven and the Java
checkout's protoc plugins (populated by its normal Maven build).
"""

import argparse
import os
import shutil
import subprocess
import sys
import uuid
import xml.etree.ElementTree as ET
from pathlib import Path

from hurricache import HurriCacheClient, KeyHintData, OrderedPayload, Variant
from hurricache.grpc import cache_pb2 as pb
from hurricache.grpc.operation import operation
from hurricache.grpc.utils import create_key, create_ordered_key, create_value, decode_batch, decode_cas, decode_key
from tools.parity import BASELINE

ROOT = Path(__file__).resolve().parents[1]


def data(size):
    return bytes(i % 251 for i in range(size))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("java_checkout", type=Path)
    parser.add_argument("--java-home", type=Path, required=True)
    parser.add_argument("--target", help="also run live bidirectional fixtures after successful server startup")
    parser.add_argument("--codecs", default="gzip,lz4", help="comma-separated gzip,lz4 selection for the server configuration")
    parser.add_argument("--cases", help="comma-separated live diagnostic cases; default is full acceptance")
    args = parser.parse_args()
    checkout = args.java_checkout.resolve()
    assert subprocess.check_output(["git", "-C", str(checkout), "rev-parse", "HEAD"], text=True).strip() == BASELINE
    work = ROOT / "build" / ("java-interop-" + BASELINE[:12])
    work.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ, JAVA_HOME=str(args.java_home))
    env["PATH"] = str(args.java_home / "bin") + os.pathsep + env["PATH"]

    def run(command):
        subprocess.run([str(item) for item in command], check=True, env=env, cwd=ROOT)

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
    sources = [*(java / "client/intf").glob("*.java"), java / "client/KeyValueUtils.java", *(java / "utils").glob("*.java"),
               java / "client/FastCacheAsyncSimpleClient.java", java / "client/FastCacheAsyncStandaloneClient.java",
               *(generated.rglob("*.java")), ROOT / "tools/CodecInterop.java", ROOT / "tools/LiveInterop.java"]
    source_list = work / "sources.txt"
    source_list.write_text("\n".join('"' + p.as_posix() + '"' for p in sources), encoding="utf-8")
    run([args.java_home / "bin/javac", "-encoding", "UTF-8", "-cp", classpath, "-d", work, "@" + str(source_list)])
    codecs = args.codecs.split(",")
    if not set(codecs) <= {"gzip", "lz4"}:
        parser.error("--codecs must select gzip and/or lz4")
    for codec in codecs:
        fixture = work / f"{codec}.pb"
        command = [args.java_home / "bin/java", f"-Dhurricache.compression={codec}",
                   "-cp", str(work) + os.pathsep + classpath, "CodecInterop"]
        run([*command, "write", fixture])
        with operation(HurriCacheClient(payload_compression=codec), 10):
            batch = pb.BatchValueResponse.FromString(fixture.read_bytes())
            decoded = decode_batch(batch)
            assert list(decoded) == [OrderedPayload(data(4098), 2), OrderedPayload(data(4097), 1)]
            assert list(decoded.values()) == [Variant.of_bytes(data(4098)), Variant.of_bytes(data(4097))]
            mixed = [Variant.none(), Variant.of_bytes(b""), Variant.of_long(0), Variant.of_double(0),
                     Variant.of_long(-(1 << 63)), Variant.of_long((1 << 63)-1), Variant.of_double(-0.0),
                     Variant.of_double(1.25), Variant.of_bytes(data(4096)), Variant.of_double(float("nan"))]
            assert decode_batch(pb.BatchValueResponse.FromString(Path(str(fixture) + ".typed").read_bytes())) == mixed
            observed = decode_cas(pb.AtomicCasRes.FromString(Path(str(fixture) + ".cas").read_bytes()))
            assert Variant.of_double(observed.expected_value) == Variant.of_double(-0.0)
            assert observed.hint == KeyHintData(weak_hash=0)
            Path(str(fixture) + ".typed").write_bytes(pb.BatchValueResponse(
                value_unordered=[create_value(v, compress=True) for v in mixed]).SerializeToString())
            Path(str(fixture) + ".cas").write_bytes(pb.AtomicCasRes(expected=pb.ValueResponse(
                value_unordered=create_value(-0.0), keyHint=pb.KeyHint(weak_hash=0))).SerializeToString())
            update_file = Path(str(fixture) + ".updates")
            commands = pb.BatchMessage.FromString(update_file.read_bytes()).batch_single_messages
            assert len(commands) == 3
            for index, command_item in enumerate(commands):
                update = command_item.batch_command.update_in_container_by_key
                assert update.WhichOneof("keys") == ("ordered_element_key" if index < 2 else "element_key")
                assert update.WhichOneof("values") == ("new_order" if index == 0 else "delta_order" if index == 1 else "value")
                if index < 2:
                    assert decode_key(update.ordered_element_key) == data(4096)
                    assert update.ordered_element_key.order == 10 and update.ordered_element_key.clientId == 17
                else:
                    assert update.value.value.intPayload == 0 and update.value.value_type == pb.INT
            assert commands[0].batch_command.update_in_container_by_key.new_order == 0
            assert commands[1].batch_command.update_in_container_by_key.delta_order == -5
            container = create_key(b"container", client_id=17)
            requests = [pb.UpdateContainerRequest(key=container,
                            ordered_element_key=create_ordered_key(data(4096), 10, client_id=17), new_order=0),
                        pb.UpdateContainerRequest(key=container,
                            ordered_element_key=create_ordered_key(data(4096), 10, client_id=17), delta_order=-5),
                        pb.UpdateContainerRequest(key=container, element_key=create_key(b"entry", client_id=17),
                                                  value=create_value(0))]
            update_file.write_bytes(pb.BatchMessage(batch_single_messages=[pb.BatchSingleMessage(
                replication_command_type=pb.UPDATE_IN_CONTAINER_BY_KEY,
                batch_command=pb.BatchCommand(update_in_container_by_key=request)) for request in requests]).SerializeToString())
            batch = pb.BatchValueResponse(
                key_ordered=[create_ordered_key(OrderedPayload(data(4096 + i), i)) for i in (2, 1)],
                value_unordered=[create_value(data(4096 + i), compress=True) for i in (2, 1)])
            fixture.write_bytes(batch.SerializeToString())
        run([*command, "read", fixture])
        if args.target:
            prefix = "python-java-02/" + uuid.uuid4().hex
            hints = work / (codec + ".hints")
            python = [sys.executable, "-m", "tools.interop"]
            java_live = [*command[:-1], "LiveInterop"]
            for writer, reader in (("python", "java"), ("java", "python")):
                key_prefix = prefix + "/" + writer
                def action(language, verb):
                    arguments = [verb, args.target, key_prefix, hints]
                    if language == "python":
                        return [*python, *arguments, "--payload-compression", codec, *(["--cases", args.cases] if args.cases else [])]
                    return [*java_live, *arguments, *([args.cases] if args.cases else [])]
                written = False
                try:
                    run(action(writer, "write"))
                    written = True
                    run(action(reader, "read"))
                finally:
                    if written:
                        run(action(writer, "cleanup"))
    scope = f"live cases={args.cases or 'full'}" if args.target else "offline only"
    print(f"Bidirectional Java/Python fixtures passed ({','.join(codecs)}; {scope})")


if __name__ == "__main__":
    main()
