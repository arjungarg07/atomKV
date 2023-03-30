# atomKV

A Bitcask-style key-value store in Go. Append-only writes for durability, in-memory index for O(1) lookups. Thread-safe.

## Performance

```
Write: 320,000+ ops/sec
Read:  680,000+ ops/sec
```

(10 concurrent goroutines, 100K operations)

## Install

```bash
go build -o atomkv ./cmd/atomkv
go build -o atomkv-server ./cmd/atomkv-server
go build -o atomkv-bench ./cmd/atomkv-bench
```

## CLI

```bash
./atomkv set name alice   # OK
./atomkv get name         # alice
```

## HTTP Server

```bash
./atomkv-server 8080

curl -X POST localhost:8080/set -d '{"key":"name","value":"alice"}'
curl "localhost:8080/get?key=name"
curl localhost:8080/keys
curl -X POST localhost:8080/compact
```

## Library

```go
import "atomkv"

db, _ := atomkv.Open("data.db")
defer db.Close()

db.Load()                 // rebuild index on restart
db.Set("name", "alice")
val, _ := db.Get("name")  // "alice"
db.Compact()              // remove stale entries
```

## Design

- **Write path:** Buffer record, append to file, update in-memory index
- **Read path:** Lookup offset in index, pread from file (concurrent-safe)
- **Recovery:** Scan file sequentially, rebuild index (last write wins)
- **Compaction:** Write only latest values to new file, atomic swap

```
Record: | timestamp (8B) | key_len (4B) | val_len (4B) | key | value |
```
