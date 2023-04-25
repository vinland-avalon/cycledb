# TSI details
## Reference
https://docs.influxdata.com/influxdb/v1.8/concepts/tsi-details/  
and doc.go in the same folder
### Index
Entire dataset for a shard
### Partition
Contains sharded partition of the data for a shard
### LogFile 
Contains newly written series as an in-memory index and is persisted as WAL.
### IndexFile
Contains an immutable, memory-mapped index built from a LogFile or merged from two contiguous index files？
### SeriesFile
It could also be named as `SeriesKeyFile`, for it stores keys(measurement + tag set) for all series, and shared by all shards, across the entire database.

# Question:
1. This function returns a bitmap rather than list of series ID, how will it be used?  
Bitmap is not bloom filter.

