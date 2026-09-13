"""Generate an exhaustive Java overload inventory from the pinned checkout."""

import argparse
import inspect
import re
import subprocess
from pathlib import Path

from hurricache import AsyncHurriCacheClient, HurriCacheClient
from hurricache.grpc.smart_client import _is_write

BASELINE = "833f2dd9a581a1984129b3f709f63afe3bfdc124"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("java_checkout", type=Path)
    args = parser.parse_args()
    assert subprocess.check_output(["git", "-C", str(args.java_checkout), "rev-parse", "HEAD"], text=True).strip() == BASELINE
    rename = {
        "getDefaultClientId": "default_client_id", "getDefaultTimeout": "default_timeout",
        "getDefaultTtl": "default_ttl", "getDefaultCompressionThreshold": "default_compression_threshold",
        "getTarget": "target", "shutdown": "close", "serializeKey": "UTF-8 str keys",
        "setMode": "mode / set_mode", "getReadyFlag": "ready",
    }
    rows = []
    root = args.java_checkout / "src/main/java/com/hurricache/client/intf"
    for path in sorted(root.glob("HurriCacheClient*.java")):
        source = path.read_text(encoding="utf-8")
        source = re.sub(r"/\*.*?\*/|//[^\n]*", lambda m: re.sub(r"[^\n]", " ", m[0]), source, flags=re.S)
        for match in re.finditer(r"(?:default\s+)?(?:CompletableFuture<.*?>|Duration|int|String|void|byte\[\])\s+(\w+)\s*\((.*?)\)", source, re.S):
            java = match[1]
            python = rename.get(java, re.sub(r"(?<!^)(?=[A-Z])", "_", java).lower())
            if java == "removeFromContainer" and "byte[] elementKey" in match[2]:
                python = "remove_container_key"
            operation = "CompletableFuture" in match[0]
            if operation:
                assert hasattr(HurriCacheClient, python), (java, python)
                assert inspect.iscoroutinefunction(getattr(AsyncHurriCacheClient, python)), python
            line = source.count("\n", 0, match.start()) + 1
            url = f"https://github.com/hurricache/hurricache-java-client/blob/{BASELINE}/src/main/java/com/hurricache/client/intf/{path.name}#L{line}"
            signature = " ".join(match[0].split()).replace("|", "\\|")
            policy = "master then backup" if _is_write(python) else "configured mode"
            test = f"test_parity.py::test_every_operation[{{sync,async,smart,async_smart}}-{python}]" if operation else "defaults / lifecycle tests"
            rows.append(f"| [{signature}]({url}) | `{python}` | {policy if operation else 'n/a'} | {test} |")
    output = [
        "# Java / Python API parity", "", f"Pinned Java jdk-16 commit `{BASELINE}`; {len(rows)} overloads.", "",
        "Every operation is available on all four clients. Async methods are native coroutines; smart clients route the same method signatures.",
        "String keys use UTF-8. Existing Python positional arguments and successful bytes, bool, int, dict, list, LockStatus and CasResult results are preserved.",
        "`add_element_ordered` retains its count result; `add_element_ordered_set` supplies the separate boolean convenience.",
        "Optional hint/client-ID/timeout/TTL overloads use Python defaults. TTL is milliseconds; lock duration and timeout are seconds.",
        "Jedis and application-level replication are excluded. Replication wire messages and stubs are included.", "",
        "| Java declaration | Python equivalent | Smart default | Test mapping |", "| --- | --- | --- | --- |", *rows, "",
    ]
    (Path(__file__).resolve().parents[1] / "API_PARITY.md").write_text("\n".join(output) + "\n## Standalone-wrapper behavior and compatibility adaptations\n\nThe pinned `FastCacheAsyncStandaloneClient` owns collection creation chunking:\none create, then tail additions for list/vector/queue, unpositioned additions for\nsets, weighted additions for ordered sets, and paired additions for maps.\nIt transfers the create response hint into subsequent requests. Both clients\npreflight complete serialized requests and continuation hints and retain a single\ncreation expiration across all chunks, including map values and ordered elements.\nThese are correctness adaptations where the Java wrapper estimates request sizes\nand rebuilds continuation values without the initial element expiration.\n\nHead/tail and relative-pivot RPCs return integer counts in the pinned schema.\nRetained boolean APIs are compatibility adaptations: empty input succeeds,\npositive chunk counts succeed, and zero stops with acknowledged partial progress.\nCount APIs sum integer responses, return zero for empty input, and reject overflow.\nFront aliases, generic addition and ordered boolean conveniences are retained\ncompatibility APIs even where Java removed their names. Python's pre-existing\n`add_element_ordered` remains count-returning.\n\nWeighted point reads and removals both use configured routing, as does the new\nordered-map range. Explicit caller-local overrides take precedence. No operation\nreplays after acknowledged progress; fallback/reroute is limited to one attempt.\n", encoding="utf-8")


if __name__ == "__main__":
    main()
