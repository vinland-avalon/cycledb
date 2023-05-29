# TSI details
## Reference
- https://docs.influxdata.com/influxdb/v1.8/concepts/tsi-details/  
- doc.go in the same folder
- https://zhuanlan.zhihu.com/p/85318358
## Index
Entire dataset for a shard, including LogFile and an IndexFile objects.
## Partition
It contains sharded partition of the data for a shard.
## LogFile (WAL)
It contains newly written series and is persisted as WAL in the format of log entry. The data within will be indexed in memory to help with lookup. It consists of Log Entry which record the operation.
## IndexFile (SST)
It contains highly indexed series key information. They are built when compacting. It consists of a series block, one or more tag block and one measurement block.
### Series Block
It contains row series keys and some hash indexes to accelerate access.
### Tag Block
One tag block contains n tag key and corressponding tag value information for those n tag key for one measurement. It can be used to access offsets of SeriesKeys for a tag value. The `Value` field contains that list of offsets.
### Measurement Block
The measurement block stores a sorted list of measurements, their associated series offsets, and the offset to their tag block.
