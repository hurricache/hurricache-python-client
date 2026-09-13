"""Exact protobuf chunk planning, independent of synchronous/async transport."""

from __future__ import annotations

from hurricache.grpc import cache_pb2 as pb
from hurricache.grpc.operation import remaining
from hurricache.grpc.utils import MAX_RPC_SIZE


def check(request):
    remaining()
    if request.ByteSize() > MAX_RPC_SIZE:
        raise ValueError("request exceeds the maximum HurriCache request size")


def _items(request):
    if isinstance(request, pb.AddToValRequest):
        fields = ("value",)
    elif isinstance(request, pb.RemoveFromContainerRequest):
        # Removal keys and values are independent lists, not map pairs.
        return [((field, item),) for field in ("keys", "values") for item in getattr(request, field)], ("keys", "values")
    else:
        fields = ("key_unordered", "key_ordered", "value_unordered", "value_ordered")
    present = [field for field in fields if len(getattr(request, field))]
    if not present:
        return [], fields
    if len(present) > 1:
        if present not in (["key_unordered", "value_unordered"], ["key_ordered", "value_unordered"]):
            raise ValueError("collection fields do not form map pairs")
        if len(getattr(request, present[0])) != len(getattr(request, present[1])):
            raise ValueError("map keys and values must have equal lengths")
    return [tuple(zip(present, row)) for row in zip(*(getattr(request, f) for f in present), strict=True)], fields


def plan(request, *, creation=False):
    items, fields = _items(request)
    base = type(request)()
    base.CopyFrom(request)
    for field in fields:
        base.ClearField(field)
    reserve = type(base)()
    reserve.CopyFrom(base)
    if creation:
        reserve.key.keyHint.week_hash = 0xFFFFFFFF
        reserve.key.keyHint.strong_hash = 0xFFFFFFFF

    def build(start, end, reserved=True):
        result = type(base)()
        result.CopyFrom(reserve if reserved else base)
        if isinstance(result, pb.AddToRequest) and result.HasField("pos") and result.pos != 0xFFFFFFFF:
            if result.pos + end > 0xFFFFFFFF:
                raise ValueError("insertion position exceeds uint32")
            result.pos += start
        for item in items[start:end]:
            for field, value in item:
                getattr(result, field).append(value)
        return result

    check(build(0, 0))
    for index in range(len(items)):
        check(build(index, index + 1))
    chunks = []
    start = 0
    while start < len(items):
        remaining()
        lo, hi = start + 1, len(items)
        while lo < hi:
            middle = (lo + hi + 1) // 2
            if build(start, middle).ByteSize() <= MAX_RPC_SIZE:
                lo = middle
            else:
                hi = middle - 1
        chunks.append((build(start, lo, False), lo - start))
        start = lo
    if not chunks:
        chunks = [(build(0, 0, False), 0)]
    if isinstance(request, pb.AddToValRequest) and not request.isBefore:
        chunks.reverse()
    return chunks


def continuation(request, hint):
    result = pb.AddToRequest(key=request.key, type=request.type)
    if hint is not None:
        result.key.keyHint.CopyFrom(hint)
    for field in ("key_unordered", "key_ordered", "value_unordered", "value_ordered"):
        getattr(result, field).extend(getattr(request, field))
    return result
