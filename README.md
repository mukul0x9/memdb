# memdb

A Memcached-like in-memory key-value store written in Go — built from scratch with a custom byteArray allocator, sharded hash map, and background compaction worker.

## How it works
All key-value data lives in a contiguous []byte slab (byteArray) per shard,The hash table stores uint32 offsets into the byteArray rather than pointers.

nextOffset = 0 terminates a chain. Updates and deletes mark entries as wasted (valLen = 0). When wasted bytes exceed 25% of shard size, a background worker compacts the byteArray.


## feature

- GET/SET/DEL over TCP text protocol
- custom hash table based on byteArray allocator approach. where key and value lives in contiguous bytes in array. 
- 256 shard with rw-locked hash table to minimize mutex lock contention.
- background compaction worker - triggered when wasted bytes exceed 25% of byteArray size . reclaim space without blocking other shards.
- dynamic rehashing - hashtable doubles at 0.8 load factor.
- oom protection - using maxmemory which rejects writes.
- STATS command - returns live memory usage across all shards , key count.
- Zero external dependencies


### Why 256 shards?

A single global mutex locks all reads and writes. Sharding distributes lock ownership — each operation only locks 1 of 256 shards, giving ~256x reduction in contention at high concurrency.

## byteArray Layout
- bucketArray - > [[][][][][]..] each holding starting offset index of byteArray array
- byteArray array
- [keyLen | valueLen | nextOffset | keyBytes | valueBytes|keyLen | valueLen | nextOffset | keyBytes | valueBytes|....]

```
┌──────────────────────────────────────────────────────────────────────┐
│                           bucketArray                                │
│        (Hash table buckets store offsets into the byteArray)             │
└──────────────────────────────────────────────────────────────────────┘
Index      0        1        2        3        4        5
        ┌──────┬──────┬──────┬──────┬──────┬──────┐
Value   │  0   │  128 │  0   │  512 │  256 │  0   │
        └──────┴──────┴──────┴──────┴──────┴──────┘

┌──────────────────────────────────────────────────────────────────────┐
│                              byteArray                                   │
│                 (Append-only contiguous byte buffer)                 │
└──────────────────────────────────────────────────────────────────────┘


Offset 128
┌────────┬──────────┬────────────┬───────────┬─────────────┐
│ keyLen │ valueLen │ nextOffset │ keyBytes  │ valueBytes  │
│   3    │    5     │    384     │  "name"   │  "hello"    │
└────────┴──────────┴────────────┴───────────┴─────────────┘


Offset 384
┌────────┬──────────┬────────────┬───────────┬─────────────┐
│ keyLen │ valueLen │ nextOffset │ keyBytes  │ valueBytes  │
│   3    │    5     │     0      │  "hello"   │  "world"     │
└────────┴──────────┴────────────┴───────────┴─────────────┘

Collision Handling
bucketArray[1] = 128
128 ("foo") ───► 384 ("bar") ───► 0

    Physical Memory Layout
----------------------
byteArray = [
  keyLen | valueLen | nextOffset | keyBytes | valueBytes |
  keyLen | valueLen | nextOffset | keyBytes | valueBytes |
  keyLen | valueLen | nextOffset | keyBytes | valueBytes |
  ...
]
```

## Getting Started
- run server
```bash
git clone https://github.com/mukul0x9/memdb
cd memdb
go run .

```
Server listens on `:8888`.


- run client
``` bash
cd memdb/tcpClient
go run tcpClient.go
```


### Protocol

Line-delimited text protocol. Each response is terminated by `END\r\n`.

example-

```
SET <KEY> <VALUE>\n  -> ok\r\nEND\r\n
```

## TODO
- [ ] Add support for TTL
- [ ] Add support for persistence
- [ ] Add support for eviction policies
- [ ] Add real time frontend dashboard


## Benchmark

Load test: 100 concurrent TCP connections, mixed SET/GET/DEL, 5 second window.
- Throughput: 79k ops/sec

## Caveats
This is a learning project. Known gaps: no persistence, incomplete edge case handling

## References
- https://github.com/allegro/bigcache
- https://github.com/coocood/freecache
- memcached.org
- redis.io
