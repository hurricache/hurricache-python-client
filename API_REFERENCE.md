# HurriCache Python Client — API Reference

## Overview

HurriCache Python gRPC client matching Java FastCacheAsyncSimpleClient API.
All methods follow the same pattern:
- First argument: `bytes` key
- Second argument: optional `KeyHintData | None`
- Additional args: `client_id`, `ttl`, etc.

## Common Parameter Rules

### TTL
- If `ttl > 0`: converted to absolute time (`current_time_ms + ttl`)
- If `ttl == 0`: field is omitted from request

### LockInfo
- If `client_id != 0`: `lock_info.type = NO_LOCK`, `lock_info.lockedBy = client_id`
- If `client_id == 0`: `lock_info` is not created

---

## Lock Management

### `lock_object(key, hint, lock_type, client_id, lock_duration)`
Acquire a lock on a key (key must exist).

| Parameter | Filled As |
|-----------|-----------|
| `key` | `_build_key(key, hint, client_id)` |
| `lockType` | `LockType.Value(LockType(lock_type).name)` |
| `clientId` | `_resolve_client_id(client_id)` |
| `lockDuration` | `lock_duration` (ms) |

Returns: `bool` — True if lock acquired.

### `unlock_object(key, hint, client_id)`
Release a lock (client_id must match lock creator).

| Parameter | Filled As |
|-----------|-----------|
| `key` | `_build_key(key, hint, client_id)` |
| `clientId` | `_resolve_client_id(client_id)` |

Returns: `bool` — True if unlock succeeded.

---

## TTL Management

### `set_ttl(key, hint, ttl, client_id)`
Set TTL for a key.

| Parameter | Filled As |
|-----------|-----------|
| `key` | `_build_key(key, hint, client_id)` |
| `ttl` | `int(time.time() * 1000) + ttl` (if > 0) |

Returns: `bool` — True if set.

### `get_ttl(key, hint, client_id)`
Get remaining TTL.

| Parameter | Filled As |
|-----------|-----------|
| `key` | `_build_get_req(key, hint, client_id)` |

Returns: `int` — TTL in ms, or 0.

---

## Key-Value Operations

### `create_key_value(key, hint, value, ttl, client_id)`
Create a scalar key-value entry.

| Parameter | Filled As |
|-----------|-----------|
| `key` | `_build_key(key, hint, client_id)` |
| `value` | `create_value(value, ttl, client_id)` |

`create_value` fills:
- `value.payload`, `value.size`
- `ttl` = `current_time + ttl` (if > 0)
- `lock_info.type = NO_LOCK`, `lock_info.lockedBy = client_id` (if client_id != 0)

Returns: `KeyHintData`

### `get_value(key, hint, client_id)`
Get value by key.

| Parameter | Filled As |
|-----------|-----------|
| `key` | `_build_get_req(key, hint, client_id)` |

Returns: `bytes`

### `get_and_delete_value(key, hint, client_id)`
Atomically read and delete.

| Parameter | Filled As |
|-----------|-----------|
| `key` | `_build_get_req(key, hint, client_id)` |

Returns: `bytes`

### `exist_key(key, hint, client_id)`
Check key existence.

| Parameter | Filled As |
|-----------|-----------|
| `key` | `_build_get_req(key, hint, client_id)` |

Returns: `bool`

### `update_value(key, hint, value, ttl, client_id)`
Update value, returns old value.

| Parameter | Filled As |
|-----------|-----------|
| `key` | `_build_key(key, hint, client_id)` |
| `value` | `create_value(value, ttl, client_id)` |

Returns: `bytes` (old value)

### `remove(key, hint, client_id)`
Remove a key.

| Parameter | Filled As |
|-----------|-----------|
| `key` | `_build_get_req(key, hint, client_id)` |

Returns: `bool`

---

## Container Creation

### `create_vector(key, hint, values, ttl, client_id)`
Create VECTOR container.

| Parameter | Filled As |
|-----------|-----------|
| `key` | `_build_key(key, hint, client_id)` |
| `type` | `ContainerType.VECTOR` |
| `value_unordered` | `[create_value(v, 0, cid) for v in values]` |
| `ttl` | `current_time + ttl` (if > 0) |

### `create_list(key, hint, values, ttl, client_id)`
Create LIST container. Same as vector.

### `create_queue(key, hint, values, ttl, client_id)`
Create QUEUE container. Same as vector.

### `create_set(key, hint, values, ttl, client_id)`
Create SET container. Same as vector.

### `create_map(key, hint, keys, values, ttl, client_id)`
Create MAP container (unordered).

| Parameter | Filled As |
|-----------|-----------|
| `key` | `_build_key(key, hint, client_id)` |
| `type` | `ContainerType.MAP` |
| `key_unordered` | `[create_key(k, None, cid) for k in keys]` |
| `value_unordered` | `[create_value(v, 0, cid) for v in values]` |
| `ttl` | `current_time + ttl` (if > 0) |

### `create_ordered_set(key, hint, values, ttl, client_id)`
Create ORDERED_SET container.

| Parameter | Filled As |
|-----------|-----------|
| `key` | `_build_key(key, hint, client_id)` |
| `type` | `ContainerType.ORDERED_SET` |
| `value_ordered` | `[create_ordered_value(v.value, v.order, 0, cid) for v in values]` |
| `ttl` | `current_time + ttl` (if > 0) |

`create_ordered_value` fills:
- `value.payload`, `value.size`, `order`
- `ttl` = `current_time + ttl` (if > 0)
- `lock_info.type = NO_LOCK`, `lock_info.lockedBy = client_id` (if client_id != 0)

### `create_ordered_map(key, hint, keys, values, ttl, client_id)`
Create ORDERED_MAP container.

| Parameter | Filled As |
|-----------|-----------|
| `key` | `_build_key(key, hint, client_id)` |
| `type` | `ContainerType.ORDERED_MAP` |
| `key_ordered` | `[create_ordered_key(k.value, k.order, 0, cid) for k in keys]` |
| `value_unordered` | `[create_value(v, 0, cid) for v in values]` |
| `ttl` | `current_time + ttl` (if > 0) |

`create_ordered_key` fills:
- `payload.payload`, `payload.size`, `order`
- `ttl` = `current_time + ttl` (if > 0)
- `lock_info.type = NO_LOCK`, `lock_info.lockedBy = client_id` (if client_id != 0)

---

## Container Reads

### `get_container(key, hint, client_id)`
Stream all elements from container.

| Parameter | Filled As |
|-----------|-----------|
| `key` | `_build_get_req(key, hint, client_id)` |

Returns: `stream BatchValueResponse`

### `get_size(key, hint, client_id)`
Get container size or value length.

| Parameter | Filled As |
|-----------|-----------|
| `key` | `_build_get_req(key, hint, client_id)` |

Returns: `int`

### `get_head(key, hint, client_id)`
Get first element.

| Parameter | Filled As |
|-----------|-----------|
| `key` | `_build_get_req(key, hint, client_id)` |

Returns: `bytes`

### `get_tail(key, hint, client_id)`
Get last element.

| Parameter | Filled As |
|-----------|-----------|
| `key` | `_build_get_req(key, hint, client_id)` |

Returns: `bytes`

### `get_front(key, hint, client_id)`
Alias for `get_head`.

### `get_value_in_container(key, element_key, hint, element_hint, client_id)`
Get value by element key in MAP/ORDERED_MAP.

| Parameter | Filled As |
|-----------|-----------|
| `key` | `_build_key(key, hint, client_id)` |
| `element_key` | `create_key(element_key, element_hint, client_id)` |

Returns: `bytes`

### `exist_key_in_container(key, element_key, hint, element_hint, client_id)`
Check if element key exists in container.

| Parameter | Filled As |
|-----------|-----------|
| `key` | `_build_key(key, hint, client_id)` |
| `element_key` | `create_key(element_key, element_hint, client_id)` |

Returns: `bool`

### `contains_container_key(key, element_key, hint, element_hint, client_id)`
Alias for `exist_key_in_container`.

### `get_element_at_position(key, hint, pos, type, client_id)`
Get element at position.

| Parameter | Filled As |
|-----------|-----------|
| `key` | `_build_key(key, hint, client_id)` |
| `type` | `type` |
| `pos` | `pos` |

Returns: `bytes`

### `get_element_in_range(key, hint, pos, end, type, reverse, client_id)`
Stream elements in range.

| Parameter | Filled As |
|-----------|-----------|
| `key` | `_build_key(key, hint, client_id)` |
| `type` | `type` |
| `pos` | `pos` |
| `end` | `end` |
| `reverse` | `reverse` |

Returns: `stream BatchValueResponse`

---

## Container Pop Operations

### `get_and_remove_front(key, hint, client_id)`
Pop first element (FIFO queue, LIST front).

| Parameter | Filled As |
|-----------|-----------|
| `key` | `_build_get_req(key, hint, client_id)` |

Returns: `bytes`

### `get_and_remove_tail(key, hint, client_id)`
Pop last element (LIST/VECTOR back).

| Parameter | Filled As |
|-----------|-----------|
| `key` | `_build_get_req(key, hint, client_id)` |

Returns: `bytes`

### `get_and_remove_element_at_position(key, hint, pos, type, client_id)`
Remove element at position.

| Parameter | Filled As |
|-----------|-----------|
| `key` | `_build_key(key, hint, client_id)` |
| `type` | `type` |
| `pos` | `pos` |

Returns: `bytes`

---

## Container Update/Delete

### `get_and_delete_value_in_container(key, element_key, hint, element_hint, client_id)`
Atomically read and delete element from container.

| Parameter | Filled As |
|-----------|-----------|
| `key` | `_build_key(key, hint, client_id)` |
| `element_key` | `create_key(element_key, element_hint, client_id)` |

Returns: `bytes`

### `update_value_in_container(key, element_key, value, hint, element_hint, ttl, client_id)`
Update value in container, returns old value.

| Parameter | Filled As |
|-----------|-----------|
| `key` | `_build_key(key, hint, client_id)` |
| `element_key` | `create_key(element_key, element_hint, client_id)` |
| `value` | `create_value(value, ttl, client_id)` |

Returns: `bytes` (old value)

### `remove_head(key, hint, client_id)`
Remove first element.

| Parameter | Filled As |
|-----------|-----------|
| `key` | `_build_get_req(key, hint, client_id)` |

Returns: `bool`

### `remove_tail(key, hint, client_id)`
Remove last element.

| Parameter | Filled As |
|-----------|-----------|
| `key` | `_build_get_req(key, hint, client_id)` |

Returns: `bool`

### `remove_element_at_position(key, hint, pos, type, client_id)`
Remove element at position.

| Parameter | Filled As |
|-----------|-----------|
| `key` | `_build_key(key, hint, client_id)` |
| `type` | `type` |
| `pos` | `pos` |

Returns: `bool`

### `remove_from_container_by_key_value(key, hint, type, values, keys, client_id)`
Remove elements by key or value.

| Parameter | Filled As |
|-----------|-----------|
| `key` | `_build_key(key, hint, client_id)` |
| `type` | `type` |
| `values` | `[create_value(v, 0, cid) for v in values]` |
| `keys` | `[create_key(k, None, cid) for k in keys]` |

Returns: `int` (count removed)

### `remove_in_container(key, element_key, hint, element_hint, client_id)`
Remove element by key from container.

| Parameter | Filled As |
|-----------|-----------|
| `key` | `_build_key(key, hint, client_id)` |
| `element_key` | `create_key(element_key, element_hint, client_id)` |

Returns: `int` (count removed)

---

## Container Insertion

### `add_element_to_tail(key, hint, values, ttl, client_id)`
Add elements to tail.

| Parameter | Filled As |
|-----------|-----------|
| `key` | `_build_key(key, hint, client_id)` |
| `value_unordered` | `[create_value(v, ttl, cid) for v in values]` |

Returns: `bool`

### `add_element_to_head(key, hint, values, ttl, client_id)`
Add elements to head.

| Parameter | Filled As |
|-----------|-----------|
| `key` | `_build_key(key, hint, client_id)` |
| `value_unordered` | `[create_value(v, ttl, cid) for v in values]` |

Returns: `bool`

### `add_element(key, hint, values, keys, ttl, client_id)`
Add elements to container (unordered).

| Parameter | Filled As |
|-----------|-----------|
| `key` | `_build_key(key, hint, client_id)` |
| `value_unordered` | `[create_value(v, ttl, cid) for v in values]` |
| `key_unordered` | `[create_key(k, None, cid) for k in keys]` |

Returns: `int` (count added)

### `add_element_hash_map(key, hint, values, keys, ttl, client_id)`
Alias for `add_element` with keys+values for MAP.

### `add_element_to_position_by_value(key, hint, pos, is_before, values, ttl, client_id)`
Insert elements before/after pivot value.

| Parameter | Filled As |
|-----------|-----------|
| `key` | `_build_key(key, hint, client_id)` |
| `ttl` | `ttl` |
| `is_before` | `is_before` |
| `pos` | `create_value(pos, 0, cid)` |
| `value` | `[create_value(v, 0, cid) for v in values]` |

Returns: `bool`

### `add_element_to_position_before(key, hint, pivot, values, client_id)`
Alias for `add_element_to_position_by_value` with `is_before=True`.

### `add_element_to_position_after(key, hint, pivot, values, client_id)`
Alias for `add_element_to_position_by_value` with `is_before=False`.

### `add_element_unordered(key, hint, values, keys, pos, ttl, client_id)`
Add elements to unordered containers (VECTOR, LIST, SET).

| Parameter | Filled As |
|-----------|-----------|
| `key` | `_build_key(key, hint, client_id)` |
| `key_unordered` | `[create_key(k, None, cid) for k in keys]` |
| `value_unordered` | `[create_value(v, ttl, cid) for v in values]` |
| `pos` | `pos if pos >= 0 else 0` |

Returns: `int` (count added)

### `add_element_ordered(key, hint, values, keys, pos, ttl, client_id)`
Add elements to ordered containers (ORDERED_SET, ORDERED_MAP).

| Parameter | Filled As |
|-----------|-----------|
| `key` | `_build_key(key, hint, client_id)` |
| `key_ordered` | `[create_ordered_key(k.value, k.order, 0, cid) for k in keys]` |
| `value_ordered` | `[create_ordered_value(v.value, v.order, 0, cid) for v in values]` |
| `pos` | `pos` |

Returns: `int` (count added)

---

## Atomic Primitives

### `atomic_load(key, hint, client_id)`
Atomic load of counter value.

| Parameter | Filled As |
|-----------|-----------|
| `key` | `_build_get_req(key, hint, client_id)` |

Returns: `int`

### `atomic_load_and_delete(key, hint, client_id)`
Atomic load and delete.

| Parameter | Filled As |
|-----------|-----------|
| `key` | `_build_get_req(key, hint, client_id)` |

Returns: `int`

### `atomic_create(key, hint, value, ttl, client_id)`
Create atomic counter.

| Parameter | Filled As |
|-----------|-----------|
| `key` | `_build_key(key, hint, cid)` |
| `val` | `AtomicValue(val=value)` |
| `ttl` | `current_time + ttl` (if > 0) |
| `lock_info` | `type=NO_LOCK`, `lockedBy=cid` (if cid != 0) |

Returns: `KeyHintData`

### `atomic_store(key, hint, value, ttl, client_id)`
Atomic store (write).

| Parameter | Filled As | Same as `atomic_create` |

Returns: `KeyHintData`

### `atomic_exchange(key, hint, value, ttl, client_id)`
Atomic exchange (swap), returns old value.

| Parameter | Filled As | Same as `atomic_create` |

Returns: `int` (old value)

### `atomic_add(key, hint, delta, ttl, client_id)`
Atomic add (fetch-and-add).

| Parameter | Filled As | Same as `atomic_create`, `val=delta` |

Returns: `int` (new value)

### `atomic_sub(key, hint, delta, ttl, client_id)`
Atomic subtract (fetch-and-sub).

| Parameter | Filled As | Same as `atomic_create`, `val=delta` |

Returns: `int` (new value)

### `atomic_or(key, hint, mask, ttl, client_id)`
Atomic bitwise OR.

| Parameter | Filled As | Same as `atomic_create`, `val=mask` |

Returns: `int` (new value)

### `atomic_and(key, hint, mask, ttl, client_id)`
Atomic bitwise AND.

| Parameter | Filled As | Same as `atomic_create`, `val=mask` |

Returns: `int` (new value)

### `atomic_xor(key, hint, mask, ttl, client_id)`
Atomic bitwise XOR.

| Parameter | Filled As | Same as `atomic_create`, `val=mask` |

Returns: `int` (new value)

### `atomic_compare_and_set(key, hint, expected_value, new_value, ttl, client_id)`
Atomic Compare-And-Swap.

| Parameter | Filled As |
|-----------|-----------|
| `key` | `_build_key(key, hint, cid)` |
| `expected` | `AtomicValue(val=expected_value)` |
| `toSet` | `AtomicValue(val=new_value)` |
| `ttl` | `current_time + ttl` (if > 0) |
| `lock_info` | `type=NO_LOCK`, `lockedBy=cid` (if cid != 0) |

Returns: `CasResult(success=bool, expected_value=int)`

---

## Test Results

All 52 integration tests pass against real server at 127.0.0.1:50000.
