---
title: Lists
description: Use ordered binary payloads, inclusive ranges, atomic moves, and blocking list operations.
---

Lists are ordered sequences of binary values. Duplicates are allowed. LPUSH/RPUSH insert on the left/right; LPOP/RPOP remove from those ends. They are useful for bounded histories and simple queues, but a pop is not an acknowledgement-based lease.

## Work with both ends

```sh
target/release/ffkv-cli -p 6382 RPUSH docs:list alpha beta gamma
target/release/ffkv-cli -p 6382 LRANGE docs:list 0 -1
target/release/ffkv-cli -p 6382 LINDEX docs:list -1
target/release/ffkv-cli -p 6382 LPOP docs:list
target/release/ffkv-cli -p 6382 RPOP docs:list 1
target/release/ffkv-cli -p 6382 LLEN docs:list
```

LRANGE bounds are inclusive and negative indexes count backward from the tail. LPOP/RPOP without count returns one bulk value; with count it returns an array. Counts must be nonnegative integers. LSET replaces an indexed element; LREM deletes matching values; LTRIM keeps only an inclusive range. These mutate the list, not a client-side copy.

An atomic move preserves an item while transferring it to a processing list:

```sh
target/release/ffkv-cli -p 6382 LMOVE docs:list docs:list:processing LEFT RIGHT
target/release/ffkv-cli -p 6382 LRANGE docs:list:processing 0 -1
```

LMOVE is atomic across source/destination, including different shards. This alone does not implement lease expiry or redelivery; an application using processing lists must define recovery itself.

## Blocking behavior

```sh
target/release/ffkv-cli -p 6382 BLPOP docs:list:empty 0.5
```

The timeout is nonnegative floating-point seconds; zero waits indefinitely. Multiple source keys are checked in request order. Waiters on a key are served in registration order. A blocked connection does not stall other clients, but its own later pipelined commands cannot overtake it. In EXEC/Lua, list blocking operations never wait.

List pushes/partial pops preserve TTL. Removing the last item removes the key and TTL; recreating the list begins without expiration. Commands on a non-list return WRONGTYPE. A change to an incompatible watched key is not itself a wakeup; a later push may trigger rechecking and a type error.

## Persistence and payload placement

Completed pops/moves are journaled before delivery when persistence is enabled. Restart does not put successfully popped items back; pending waits are not restored. A lost reply can therefore mean the item was already removed. In hybrid mode, large list elements can be offloaded to disk, while list indexes and small values stay in RAM. Reply budgets can reject oversized destructive count-pops before mutation.

Continue with [list queues](/guides/list-queues/), [native leased jobs](/guides/native-jobs/), [LPUSH](/reference/commands/lpush/), [BLPOP](/reference/commands/blpop/), or [hybrid storage](/operations/hybrid-storage/).
