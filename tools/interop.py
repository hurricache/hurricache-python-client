"""Bidirectional fixtures shared with the pinned Java/Go interoperability harness."""

import argparse
from pathlib import Path

from hurricache import HurriCacheClient, KeyHintData, OrderedPayload, Payload


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
    args = parser.parse_args()
    host, port = args.target.rsplit(":", 1)
    if args.action == "probe-longkey":
        with HurriCacheClient(host, int(port), default_timeout=10) as client:
            key = args.prefix + "/longkey/" + "x" * 2048
            hint = client.create_key_value(key, value=data(4096))
            assert client.get_value(key, hint) == data(4096)
            client.remove(key, hint)
        print("Python compressed long-key round trip passed")
        return
    hints = {}
    if args.action != "write":
        for line in args.hints.read_text().splitlines():
            name, weak, strong = line.split()
            hints[name] = KeyHintData(int(weak), int(strong))
    with HurriCacheClient(host, int(port), default_client_id=77, default_timeout=10) as client:
        for name in ["empty", "small", "at", "compressed", "random", "list", "ordered", "map", "orderedmap", "large"]:
            key = f"{args.prefix}/{name}"
            size = {"empty": 0, "small": 1023, "at": 1024}.get(name, 4096)
            expected = data(size, name == "random")
            if args.action == "cleanup":
                client.remove(key, hints[name])
            elif args.action == "write":
                if name == "list":
                    hint = client.create_list(key, values=[data(20), data(4096)])
                elif name == "ordered":
                    hint = client.create_ordered_set(key, values=[OrderedPayload(data(4096), 1), OrderedPayload(data(4097), 0xFFFFFFFFFFFFFFFF)])
                elif name == "large":
                    hint = client.create_list(key, values=[data(4096, True)] * 1024)
                elif name == "orderedmap":
                    hint = client.create_ordered_map(key, keys=[OrderedPayload(data(4096), 1), OrderedPayload(data(4097), 2)], values=[data(4096, True), data(4097, True)])
                elif name == "map":
                    hint = client.create_map(key, keys=[b"entry"], values=[data(4096)])
                else:
                    hint = client.create_key_value(key, value=expected)
                hints[name] = hint
            elif name == "list":
                assert client.stream_list(key, hints[name]) == [Payload(data(20)), Payload(data(4096))]
            elif name == "ordered":
                assert client.stream_ordered_set(key, hints[name]) == [OrderedPayload(data(4096), 1), OrderedPayload(data(4097), 0xFFFFFFFFFFFFFFFF)]
            elif name == "large":
                assert client.stream_list(key, hints[name]) == [Payload(data(4096, True))] * 1024
            elif name == "orderedmap":
                expected_map = {OrderedPayload(data(4096), 1): Payload(data(4096, True)), OrderedPayload(data(4097), 2): Payload(data(4097, True))}
                for reverse in (False, True):
                    actual = client.stream_element_in_range_ordered_map(key, hints[name], 1, 2, reverse)
                    assert actual == expected_map
                    assert [k.order for k in actual] == ([2, 1] if reverse else [1, 2])
            elif name == "map":
                assert client.stream_map(key, hints[name]) == {Payload(b"entry"): Payload(data(4096))}
            else:
                assert client.get_value(key, hints[name]) == expected
    if args.action == "write":
        args.hints.write_text("".join(f"{name} {hint.week_hash} {hint.strong_hash}\n" for name, hint in hints.items()))
    print(f"Python {args.action} interoperability passed")


if __name__ == "__main__":
    main()
