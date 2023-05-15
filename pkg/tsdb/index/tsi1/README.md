# TSI details
## Reference
- https://docs.influxdata.com/influxdb/v1.8/concepts/tsi-details/  
- doc.go in the same folder
- https://zhuanlan.zhihu.com/p/85318358
### Index
Entire dataset for a shard
### Partition
Contains sharded partition of the data for a shard
### LogFile 
Contains newly written series as an in-memory index and is persisted as WAL in the format of log entry.
### IndexFile
Contains an immutable, memory-mapped index built from a LogFile or merged from two contiguous index files.
### SeriesFile
It could also be named as `SeriesKeyFile`, for it stores keys(measurement + tag set) for all series, and shared by all shards, across the entire database.

# Question:
1. This function returns a bitmap rather than list of series ID, how will it be used?  
Bitmap is not bloom filter.
2. The series block stores raw series keys in sorted order?  
Nope, the keys are not sorted.

3. tsi is from `tag value` to `series key`s or `series id`s?
