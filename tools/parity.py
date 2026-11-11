"""Generate or check an inventory of pinned Java public declarations."""

import argparse
import inspect
import re
import subprocess
from pathlib import Path

from hurricache import AsyncHurriCacheClient, AsyncHurriCacheSmartClient, HurriCacheClient, HurriCacheSmartClient
from hurricache.grpc.models import KeyHintData, OrderedPayload, OrderedVariant, Payload, Variant
from hurricache.grpc.smart_client import _is_write
from hurricache.jedis import Jedis, JedisPool

BASELINE = "0a2b1d851405744e9390bcf5b5e8592177b121e4"
ROOT = Path(__file__).resolve().parents[1]
RENAMES = {
    "getDefaultClientId": "default_client_id", "getDefaultTimeout": "default_timeout",
    "getDefaultTtl": "default_ttl", "getDefaultCompressionThreshold": "default_compression_threshold",
    "getTarget": "target", "shutdown": "close", "serializeKey": "UTF-8 str keys",
    "setMode": "set_mode", "getReadyFlag": "ready", "getResource": "get_resource",
    "toString": "__repr__",
    "equals": "__eq__", "hashCode": "__hash__",
    "and": "and_", "or": "or_",
    "getValue": "value", "getOrder": "order", "getWeek_hash": "weak_hash", "getStrong_hash": "strong_hash",
    "hasStrongHash": "strong_hash", "hasWeekHash": "weak_hash", "isWasRelayed": "was_relayed",
}
PARAMETERS = {
    "clientId": "client_id", "containerType": "type", "container_keys": "keys", "container_values": "values",
    "data": "values", "duration": "lock_duration", "elementKey": "element_key",
    "endPos": "end", "endWeight": "end", "expectedValue": "expected_value", "keyHint": "hint",
    "newValue": "new_value", "start": "pos", "startWeight": "pos",
    "o": "other",
    "containerKey": "container_key", "orderedKey": "selector", "val": "value", "type": "type",
    "bytesValue": "value", "intValue": "value", "doubleValue": "value", "longValue": "value",
    "wasRelayed": "was_relayed",
    "newOrder": "new_order",
}
CONSTRUCTOR_PARAMETERS = {
    "host": "host", "port": "port", "clientId": "default_client_id", "defaultClientId": "default_client_id",
    "timeout": "default_timeout", "duration": "default_timeout", "defaultCompressionThreshold": "default_compression_threshold",
    "coordinatorAddresses": "coordinators", "coordinatorHost": "coordinators", "coordinatorPort": "coordinator_port",
    "cacheClient": "client", "delegate": "integrated direct-client batching",
    "channel": "Python-owned channel (host/port, credentials, transport compression; no public channel injection)",
    "variant": "variant", "order": "order", "value": "value",
    "ownedChannel": "Python-owned channel lifecycle",
}
# Return types are unrestricted so a newly introduced method cannot disappear
# just because it returns a type not known to the inventory generator.
DECLARATION = re.compile(
    r"(?m)^[ \t]*(?P<mods>(?:(?:public|default|static|final|synchronized)\s+)*)"
    r"(?P<result>[\w.\[\]]+(?:<[^;{}()]+>)?)\s+(?P<name>\w+)\s*"
    r"\((?P<args>[^()]*)\)\s*(?:throws\s+[^;{]+)?(?=[;{])"
)


def declarations(source, *, interface=False):
    source = re.sub(r"/\*.*?\*/|//[^\n]*", lambda m: re.sub(r"[^\n]", " ", m[0]), source, flags=re.S)
    literals = r'"(?:\\.|[^"\\])*"' + r"|'(?:\\.|[^'\\])*'"
    masked = re.sub(literals, lambda m: " " * len(m[0]), source)
    for match in DECLARATION.finditer(source):
        prefix = masked[:match.start()]
        if prefix.count("{") - prefix.count("}") == 1 and (interface or "public" in match["mods"].split()):
            yield match, source.count("\n", 0, match.start()) + 1


def constructor_rows(path, checkout):
    source = path.read_text(encoding="utf-8")
    source = re.sub(r"/\*.*?\*/|//[^\n]*", lambda m: re.sub(r"[^\n]", " ", m[0]), source, flags=re.S)
    rows = []
    for match in re.finditer(r"public\s+" + path.stem + r"\s*\(([^()]*)\)", source):
        arguments = []
        for part in re.split(r",(?![^<]*>)", match[1]):
            name = part.split()[-1]
            if name not in CONSTRUCTOR_PARAMETERS:
                raise ValueError(f"unmapped constructor argument {path.stem}({name})")
            arguments.append(f"{name}→{CONSTRUCTOR_PARAMETERS[name]}")
        cls = {"Jedis": Jedis, "JedisPool": JedisPool, "FastCacheAsyncSmartClient": HurriCacheSmartClient,
               "OrderedVariant": OrderedVariant}.get(path.stem, HurriCacheClient)
        line = source.count("\n", 0, match.start()) + 1
        url = f"https://github.com/hurricache/hurricache-java-client/blob/{BASELINE}/{path.relative_to(checkout).as_posix()}#L{line}"
        declaration = " ".join(match[0].split())
        rows.append(f"| [{declaration}]({url}) | `{cls.__name__}` / async equivalent | {'; '.join(arguments)} | client lifecycle | n/a | test_current_java.py configuration/lifecycle tests |")
    return rows


def mapping(match):
    java = match["name"]
    python = RENAMES.get(java, re.sub(r"(?<!^)(?=[A-Z])", "_", java).lower())
    if java == "update" and "OrderedPayload elementKey" in match["args"]:
        python = "update_order"
    if java == "removeFromContainer" and "byte[] elementKey" in match["args"]:
        python = "remove_container_key"
    return python


def argument_mapping(match, method):
    if not callable(method):
        return "property / UTF-8 encoding"
    signature = inspect.signature(method)
    result = []
    for part in re.split(r",(?![^<]*>)", match["args"]):
        if not part.strip():
            continue
        java = part.split()[-1]
        if (match["name"], java) in {("removeFromContainerByKeyAndValue", "type")}:
            result.append(f"{java} ignored by Java convenience overload")
            continue
        python = PARAMETERS.get(java, java)
        if match["name"] == "equals":
            python = "other" if "other" in signature.parameters else "value"
        if match["name"] == "lock" and java == "type":
            python = "lock_type"
        if match["name"].endswith("InContainer") and match["name"].split("InContainer")[0] in ("exchange", "add", "sub", "and", "or", "xor", "compareAndSet") and java in ("containerKey", "pos"):
            python = "selector"
        if java == "initialValue":
            python = "keys + values" if "Map<" in part else "values"
        if match["name"] == "addElementWithWeight" and java == "keys":
            python = "values"
        if match["name"] == "removeFromContainerByKey" and java == "keys":
            python = "container_key"
        if java == "pos" and "Payload" in part:
            python = "pivot"
        if python == "timeout":
            result.append("timeout→timeout= (seconds; default_timeout)")
            continue
        for parameter in python.split(" + "):
            if parameter not in signature.parameters:
                raise ValueError(f"unmapped parameter {match['name']}({java}) → {python}: {signature}")
        result.append(f"{java}→{python}")
    return "; ".join(result) or "none"


def render(checkout):
    revision = subprocess.check_output(["git", "-C", str(checkout), "rev-parse", "HEAD"], text=True).strip()
    if revision != BASELINE:
        raise ValueError(f"Java checkout must be {BASELINE}, got {revision}")
    java_root = checkout / "src/main/java"
    groups = [
        ("Interfaces", sorted((java_root / "com/hurricache/client/intf").glob("HurriCacheClient*.java"))),
        ("Concrete wrappers", sorted((java_root / "com/hurricache/client").glob("FastCacheAsync*Client.java"))),
        ("Typed models", [java_root / "com/hurricache/client/intf" / (name + ".java") for name in
                          ("Variant", "OrderedVariant", "Payload", "OrderedPayload", "KeyHintData")]),
        ("Active Jedis adapters", sorted((java_root / "hurricache/clients/jedis").glob("*.java"))),
    ]
    sections = []
    for title, paths in groups:
        rows = []
        for path in paths:
            if title != "Interfaces":
                rows.extend(constructor_rows(path, checkout))
            for match, line in declarations(path.read_text(encoding="utf-8"), interface=title == "Interfaces"):
                python = mapping(match)
                cls = {"Jedis": Jedis, "JedisPool": JedisPool, "Variant": Variant, "OrderedVariant": OrderedVariant,
                       "Payload": Payload, "OrderedPayload": OrderedPayload, "KeyHintData": KeyHintData}.get(path.stem, HurriCacheClient)
                if python in ("set_mode", "clear_mode", "ready"):
                    cls = HurriCacheSmartClient
                # Dataclass field descriptors can have a None default.
                fields = getattr(cls, "__dataclass_fields__", {})
                method = fields[python] if python in fields else None if python == "UTF-8 str keys" else getattr(cls, python, None)
                if method is None and python != "UTF-8 str keys":
                    raise ValueError(f"unmapped Java declaration {path.name}:{line}: {match[0]}")
                operation = "CompletableFuture" in match["result"] and python != "ready"
                if operation:
                    for client in (AsyncHurriCacheClient, HurriCacheSmartClient, AsyncHurriCacheSmartClient):
                        if not hasattr(client.__new__(client), python):
                            raise ValueError(f"{client.__name__} lacks {python}")
                    if not inspect.iscoroutinefunction(getattr(AsyncHurriCacheClient, python)):
                        raise ValueError(f"{python} is not native async")
                args = argument_mapping(match, method)
                signature = str(inspect.signature(method)) if callable(method) else " property"
                adaptation = "decoded Python result"
                if python in ("__repr__", "__eq__", "__hash__"):
                    adaptation = "Python representation; Java type-sensitive value equality and hashing"
                for token, label in [("Integer", "int mutation count"),
                                     ("OrderedVariant", "OrderedVariant with nullable order; list for collections"),
                                     ("Map<", "dict[Payload or OrderedPayload, Variant]; insertion order preserved"),
                                     ("Variant", "Variant (NONE/BYTES/INT/DOUBLE); list for collections"),
                                     ("Payload", "byte-key Payload / OrderedPayload"),
                                     ("KeyHintData", "KeyHintData with nullable hashes and was_relayed; hash-only equality"),
                                     ("AtomicCasRes", "CasResult with observed value and hint"),
                                     ("Boolean", "bool"), ("Number", "native int or float"), ("Long", "int")]:
                    if token in match["result"]:
                        adaptation = label
                        break
                policy = ("master then backup" if _is_write(python) or (python == "remove" and "elementKey" in match["args"]) else "configured mode") if operation else "local / lifecycle"
                test = f"test_parity.py::test_every_operation[{python}]; test_typed_api.py family request/result assertions" if operation else "test_typed_api.py models; test_current_java.py defaults/lifecycle"
                if python == "remove_element_at_position":
                    test = "test_current_java.py::test_removal_range / test_invalid_removal_precedes_rpc"
                elif python == "remove_from_container_by_key_value":
                    test = "test_current_java.py::test_map_removal_preserves_python_order"
                elif cls in (Jedis, JedisPool):
                    test = "test_current_java.py::test_jedis_shared_lifecycle"
                elif python in ("update_order", "increment_order"):
                    test = "test_order_updates.py; test_integration.py::test_live_order_updates_exact_matching_and_bounds (live opt-in)"
                url = f"https://github.com/hurricache/hurricache-java-client/blob/{BASELINE}/{path.relative_to(checkout).as_posix()}#L{line}"
                declaration = " ".join(match[0].split()).replace("|", "\\|")
                signature = signature.replace("|", "\\|")
                rows.append(f"| [{declaration}]({url}) | `{python}{signature}` | {args} | {adaptation} | {policy} | {test} |")
        if title == "Interfaces":
            interface_count = len(rows)
        sections += [f"## {title}", "", f"{len(rows)} declarations.", "",
                     "| Java declaration | Python signature / defaults | Argument mapping | Result adaptation | Smart policy | Behavioral evidence |",
                     "| --- | --- | --- | --- | --- | --- |", *rows, ""]
    return "\n".join([
        "# Java / Python API parity", "", f"Pinned Java commit `{BASELINE}`; {interface_count} interface overloads.", "",
        "All operation names exist on sync, native async, and both smart clients. `python -m tools.parity JAVA_CHECKOUT --check`",
        "compares without rewriting and rejects unknown declarations and arguments. Concrete constructors map to Python constructors;",
        "the standalone delegate constructor maps to the direct client with integrated batching. Jedis/JedisPool take an existing",
        "synchronous direct or smart client; closing either wrapper closes that client. Commented Java Redis commands are unimplemented.",
        "Replication messages/stubs remain; Java has no application-level replication client. Live cluster/replication validation is deferred.", "",
        "Timeout/lock Duration arguments use Python seconds; TTL Duration arguments use milliseconds. Missing hint=None,",
        "client_id=None→configured ID, timeout→default_timeout and ttl=None→default_ttl retain Python defaults.",
        "Explicit zero selects the default ID unless zero_client_id_uses_default=False. Codec selection is explicit: gzip (default) or optional lz4.",
        "Each signature records Python defaults. Overloads collapse into named arguments; str keys encode as UTF-8.",
        "Current operations return typed values and integer counts. Only unambiguous deprecated aliases remain; see README.md.",
        "Request/result assertions cover values, collections, atomics/CAS, TTL, owners, ordering, deadlines, cancellation, routing and replay",
        "prevention in test_parity.py, test_gzip_counts.py, test_routing_parity.py and test_current_java.py.", "", *sections,
        "## Wrapper correctness adaptations", "",
        "Creation preflights serialized requests and continuation hints. Continuations retain encoded data, owner ID, response hints",
        "and a single expiration. Both codecs retain field eligibility and strict length > threshold behavior. Decoding validates wire size,",
        "raw size and the 64 MiB decoded limit; channels accept 64 MiB messages. No operation replays after partial progress.",
        "Count APIs sum counts and reject overflow; empty-input insertion returns zero without an RPC.",
        "Head chunks stay in input order because the server reverses each request, correcting Java's backward chunk traversal.",
        "Pivot chunks advance the anchor and preflight its full encoded size. Live split/single checks cover all four client forms.",
        "Weighted reads/removals and ordered-map ranges follow configured routing. See FINDINGS.md for validation evidence and limits.", "",
    ])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("java_checkout", type=Path)
    parser.add_argument("--check", action="store_true", help="compare only; never rewrite the inventory")
    args = parser.parse_args()
    result = render(args.java_checkout.resolve())
    output = ROOT / "API_PARITY.md"
    if args.check:
        if output.read_text(encoding="utf-8") != result:
            raise SystemExit("API_PARITY.md is stale; regenerate with tools/parity.py")
        print("Java parity inventory matches the pinned checkout")
    else:
        output.write_text(result, encoding="utf-8", newline="\n")


if __name__ == "__main__":
    main()
