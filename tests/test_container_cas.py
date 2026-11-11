"""Current Java container CAS: exact selectors, presence, types and no TTL."""
import pytest
from test_parity import invoke
from test_typed_api import reply

from hurricache import KeyHintData, OrderedPayload
from hurricache.grpc import cache_pb2 as pb
from hurricache.grpc.operation import operation
from hurricache.grpc.utils import decode_key

pytest_plugins = ["test_parity"]


@pytest.mark.parametrize("codec", ["gzip", "lz4"])
@pytest.mark.parametrize("selector", [b"", "field", b"field" * 100, OrderedPayload(b"field" * 100, 0),
                                     OrderedPayload(b"field", (1 << 64) - 1)])
@pytest.mark.parametrize("expected,new", [(0, (1 << 63) - 1), (-(1 << 63), 0), (1, 2.5)])
async def test_container_cas_fields(client_form, codec, selector, expected, new):
    client, stub = client_form
    if codec == "lz4":
        pytest.importorskip("lz4.block")
    client._payload_compression = codec
    for node in getattr(client, "_clients", {}).values():
        node._payload_compression = codec
    response = pb.AtomicCasRes(result=False, expected=pb.ValueResponse(
        value_unordered=pb.Value(value=pb.BinaryPayload(doublePayload=1.25), value_type=pb.DOUBLE),
        keyHint=pb.KeyHint(weak_hash=0, strong_hash=2)))
    reply(stub, "atomicCompareAndSet", response)
    result = await invoke(client, "compare_and_set_in_container", "map", KeyHintData(0, 1),
                          selector=selector, expected_value=expected, new_value=new, client_id=7)
    _, request, options = stub.calls[-1]
    assert options["timeout"] > 0 and request.key.clientId == 7
    assert request.key.keyHint.weak_hash == 0
    ordered = isinstance(selector, OrderedPayload)
    assert request.WhichOneof("keys") == ("ordered_element_key" if ordered else "element_key")
    entry = request.ordered_element_key if ordered else request.element_key
    raw = selector.value if ordered else selector.encode() if isinstance(selector, str) else selector
    with operation(client, 2):
        assert decode_key(entry) == raw
    assert entry.clientId == 7 and entry.compressionInfo.enabled is (len(raw) > 8)
    if ordered:
        assert entry.HasField("order") and entry.order == selector.order
    for value in (request.expected, request.toSet):
        assert not value.HasField("ttl") and not value.HasField("compressionInfo")
        assert value.value_type == (pb.DOUBLE if isinstance(new, float) else pb.INT)
    assert result.success is False and result.expected_value == 1.25
    assert type(result.expected_value) is float and result.hint == KeyHintData(0, 2)
    reply(stub, "atomicCompareAndSet", pb.AtomicCasRes(result=True))
    result = await invoke(client, "compare_and_set_in_container", "map", selector=selector)
    assert result.success and result.expected_value is None


@pytest.mark.parametrize("selector,expected,new", [(0, 1, 2), (True, 1, 2), (None, 1, 2),
    (OrderedPayload(b"field", None), 1, 2), (b"field", True, 2),
    (OrderedPayload(b"field", True), 1, 2), (OrderedPayload(b"field", -1), 1, 2),
    (OrderedPayload(b"field", 1 << 64), 1, 2),
    (b"field", 1 << 63, 2), (b"field", 1, -(1 << 63)-1)])
async def test_container_cas_validation_before_rpc(client_form, selector, expected, new):
    client, stub = client_form
    with pytest.raises((TypeError, ValueError, OverflowError)):
        await invoke(client, "compare_and_set_in_container", "map", selector=selector,
                     expected_value=expected, new_value=new)
    assert not stub.calls
