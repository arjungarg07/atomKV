# atomKV

A Bitcask-style key-value store in Go. Append-only writes for durability, in-memory index for O(1) lookups. Thread-safe.

## Install

```bash
go build -o atomkv ./cmd/atomkv
```

## CLI

```bash
./atomkv set name alice   # OK
./atomkv get name         # alice
```

## Library

```go
import "atomkv"

db, _ := atomkv.Open("data.db")
defer db.Close()

db.Load()                 // rebuild index on restart
db.Set("name", "alice")
val, _ := db.Get("name")  // "alice"
```

## Design

- **Write path:** Append record to file, update in-memory index
- **Read path:** Lookup offset in index, seek and read from file
- **Recovery:** Scan file sequentially, rebuild index (last write wins)

```
Record: | timestamp (8B) | key_len (4B) | val_len (4B) | key | value |
```
