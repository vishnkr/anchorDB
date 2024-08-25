# anchor-db
A LSM tree based storage engine

## Basic Design Goals
- Support `Get(key)`, `Put(key,value)`, `Delete(key)` and `RangeScan(startKey,endKey)` operations
- Write Ahead Logs for better crash recovery
- Automatic compaction using a size-tiered compaction strategy
- Bloom filters for improved read performance
