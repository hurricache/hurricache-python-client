"""Live typed fixtures shared with LiveInterop.java; requires a matching server."""

import argparse
from pathlib import Path

from hurricache import HurriCacheClient, KeyHintData, OrderedPayload, OrderedVariant, Payload, Variant


def data(size, random=False):
    result = bytearray()
    value = 1234567
    for _ in range(size):
        value ^= (value << 13) & 0xFFFFFFFF
        value ^= value >> 17
        value ^= (value << 5) & 0xFFFFFFFF
        result.append(value & 255 if random else 42)
    return bytes(result)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["write", "read", "cleanup", "probe-longkey"])
    parser.add_argument("target")
    parser.add_argument("prefix")
    parser.add_argument("hints", type=Path)
    parser.add_argument("--payload-compression", choices=["gzip", "lz4"], default="gzip")
    parser.add_argument("--cases", help="comma-separated diagnostic selection; default runs the full acceptance matrix")
    args = parser.parse_args()
    host, port = args.target.rsplit(":", 1)
    if args.action == "probe-longkey":
        with HurriCacheClient(host, int(port), default_timeout=10, payload_compression=args.payload_compression) as client:
            key = args.prefix + "/longkey/" + "x" * 2048
            hint = client.create(key, value=data(4096))
            assert client.get(key, hint) == Variant.of_bytes(data(4096))
            client.remove(key, hint)
        print("Python compressed long-key round trip passed")
        return
    hints = {}
    if args.action != "write":
        for line in args.hints.read_text().splitlines():
            name, weak, strong = line.split()
            hints[name] = KeyHintData(int(weak), int(strong))
    with HurriCacheClient(host, int(port), default_client_id=77, default_timeout=10, payload_compression=args.payload_compression) as client:
        cases = ["empty", "small", "at", "compressed", "random", "list", "ordered", "map", "orderedmap", "large", "largeorderedmap", "none", "int", "double", "mixed", "storedmixed", "atomic", "longkey", "weightmap", "weightset"]
        selected = args.cases.split(",") if args.cases else cases
        if not set(selected) <= set(cases):
            parser.error("unknown interoperability case")
        for name in selected:
            key = f"{args.prefix}/{name}" + ("/" + "x" * 2048 if name == "longkey" else "")
            size = {"empty": 0, "small": 1023, "at": 1024}.get(name, 4096)
            expected = data(size, name == "random")
            if args.action == "cleanup":
                client.remove(key, hints[name])
            elif args.action == "write":
                if name == "list":
                    hint = client.create_list(key, values=[data(20), data(4096)])
                elif name == "ordered":
                    hint = client.create_ordered_set(key, values=[OrderedVariant.of_bytes(data(4096), 1), OrderedVariant.of_bytes(data(4097), 0xFFFFFFFFFFFFFFFF)])
                elif name == "large":
                    hint = client.create_list(key, values=[data(4096, True)] * 1024)
                elif name == "largeorderedmap":
                    hint = client.create_ordered_map(key, keys=[OrderedPayload(bytes([0, i >> 8, i & 255, 255]), i+1) for i in range(1024)], values=[data(4096, True)] * 1024)
                elif name == "orderedmap":
                    hint = client.create_ordered_map(key, keys=[OrderedPayload(data(4096), 1), OrderedPayload(data(4097), 2)], values=[data(4096, True), data(4097, True)])
                elif name == "map":
                    hint = client.create_map(key, keys=[b"entry"], values=[data(4096)])
                elif name in ("none", "int", "double", "atomic"):
                    hint = client.create(key, value={"none": Variant.none(), "int": -(1 << 63), "double": -0.0, "atomic": 10}[name])
                    if name == "atomic":
                        client.add(key, hint, delta=5)
                elif name in ("mixed", "storedmixed"):
                    mixed = [b"", 0, 0.0, 1.25, data(4096)] if name == "storedmixed" else [Variant.none(), b"", 0, 1.25]
                    hint = client.create_list(key, values=mixed)
                elif name in ("weightmap", "weightset"):
                    if name == "weightmap":
                        hint = client.create_ordered_map(key, keys=[OrderedPayload(b"dup", 1), OrderedPayload(b"dup", 2)], values=[1.25, 2])
                    else:
                        hint = client.create_ordered_set(key, values=[OrderedVariant.of_bytes(b"dup", 1), OrderedVariant.of_bytes(b"dup", 2)])
                    assert client.increment_order(key, hint, OrderedPayload(b"dup", 1), 4)
                    assert client.update_order(key, hint, OrderedPayload(b"dup", 2), 7)
                else:
                    hint = client.create(key, value=expected)
                hints[name] = hint
            elif name == "list":
                assert client.get_list(key, hints[name]) == [Variant.of_bytes(data(20)), Variant.of_bytes(data(4096))]
            elif name == "ordered":
                assert client.get_ordered_set(key, hints[name]) == [OrderedVariant.of_bytes(data(4096), 1), OrderedVariant.of_bytes(data(4097), 0xFFFFFFFFFFFFFFFF)]
            elif name == "large":
                assert client.get_list(key, hints[name]) == [Variant.of_bytes(data(4096, True))] * 1024
            elif name == "largeorderedmap":
                actual = client.get_ordered_map(key, hints[name])
                expected = {OrderedPayload(bytes([0, i >> 8, i & 255, 255]), i+1): Variant.of_bytes(data(4096, True)) for i in range(1024)}
                assert actual == expected and list(actual) == list(expected)
            elif name == "orderedmap":
                expected_map = {OrderedPayload(data(4096), 1): Variant.of_bytes(data(4096, True)), OrderedPayload(data(4097), 2): Variant.of_bytes(data(4097, True))}
                for reverse in (False, True):
                    actual = client.get_element_in_range_ordered_map(key, hints[name], 1, 2, reverse)
                    assert actual == expected_map
                    assert [k.order for k in actual] == ([2, 1] if reverse else [1, 2])
            elif name == "map":
                assert client.get_map(key, hints[name]) == {Payload(b"entry"): Variant.of_bytes(data(4096))}
            elif name in ("none", "int", "double", "atomic"):
                expected = {"none": Variant.none(), "int": Variant.of_long(-(1 << 63)), "double": Variant.of_double(-0.0), "atomic": Variant.of_long(15)}[name]
                assert client.get(key, hints[name]) == expected
                if name == "atomic":
                    assert client.exchange(key, hints[name], value=15) == 15
            elif name in ("mixed", "storedmixed"):
                expected = [Variant.of_bytes(b""), Variant.of_long(0), Variant.of_double(0), Variant.of_double(1.25), Variant.of_bytes(data(4096))] if name == "storedmixed" else [Variant.none(), Variant.of_bytes(b""), Variant.of_long(0), Variant.of_double(1.25)]
                assert client.get_list(key, hints[name]) == expected
            elif name in ("weightmap", "weightset"):
                actual = client.get_ordered_map(key, hints[name]) if name == "weightmap" else client.get_ordered_set(key, hints[name])
                assert [item.order for item in actual] == [5, 7]
                if name == "weightmap":
                    assert list(actual.values()) == [Variant.of_double(1.25), Variant.of_long(2)]
                else:
                    assert actual == [OrderedVariant.of_bytes(b"dup", 5), OrderedVariant.of_bytes(b"dup", 7)]
                assert client.increment_order(key, hints[name], OrderedPayload(b"dup", 5), 1)
                assert client.update_order(key, hints[name], OrderedPayload(b"dup", 6), 5)
            else:
                assert client.get(key, hints[name]) == Variant.of_bytes(expected)
    if args.action == "write":
        args.hints.write_text("".join(f"{name} {hint.weak_hash} {hint.strong_hash}\n" for name, hint in hints.items()))
    print(f"Python {args.action} interoperability passed ({args.payload_compression}; {','.join(selected)})")


if __name__ == "__main__":
    main()
