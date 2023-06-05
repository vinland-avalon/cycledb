# Purpose
So far, this repo is create to build [Grid Index](./pkg/tsdb/index/tsi2/Grid_Index.md). Reach docs [here](./pkg/tsdb/index/tsi2/Grid_Index.md).
<!-- # How to run main.go:
remember go version >= 1.18
so use `gvm`
```sh
source /home/bohan_wu_ubuntu/.gvm/scripts/gvm (wsl2)
source /home/local/ASUAD/bohanwu/.gvm/scripts/gvm (ASUAD)
# optional: 
# gvm install go1.18
gvm use go1.18 --default
```
other configs
```sh
sudo apt install make clang pkg-config protobuf-compiler libprotobuf-dev build-essential
```
Then flux
```sh
go get github.com/influxdata/pkg-config
```
to build and run the program, follow [guide](https://github.com/influxdata/influxdb/blob/master/CONTRIBUTING.md#building-from-source) -->

# [Basic Concepts](https://docs.influxdata.com/influxdb/v1.8/concepts/storage_engine/#the-influxdb-storage-engine-and-the-time-structured-merge-tree-tsm)
## Shard
Influxdb will create a shard for each block of time

in-memory index <-1:n-> shard <-1:1-> storage engine database <-1:1-> wal <-1:n-> tsm files
## Storage Engine
Provides storage and query interface, has the following components
- In-Memory Index - It is shared across shards, providing quick access to measurement, tags and series
- WAL
- Cache - In-memory representation of WAL, be queried at runtime and merged with data stored in TSM.(memtable?)
- TSM Files - SST, columnar format, compressed
- FileStore - Deal with replaced and removed TSM files?
- Compactor - The Compactor is responsible for converting less optimized Cache and TSM data into more read-optimized formats. It does this by compressing series, removing deleted data, optimizing indices and combining smaller files into larger ones.
- Compactor Planner - Determines which TSM to compact and Handle concurrent compactions.
- Writers/Readers - Each file type (WAL segment, TSM files, tombstones, etc..) has Writers and Readers for working with the formats.

## WAL
_000001.wal, increased monatically, once reach 10MB, create another one.
Batching?
Each entry in the WAL follows a TLV standard with a single byte representing the type of entry (write or delete), a 4 byte uint32 for the length of the compressed block, and then the compressed block.

## Cache
Copy of WAL but in memory. No compression. key -> measurement+tag set+unique field.
Queries execute on copy of the data that is made from cache at Query processing time, so that it won't conflict with writes?
There are some limits:
- cache-snapshot-memory-size - When reach this limit, trigger a snapshot to TSM file and delete the corresponding WAL segment.
- cache-max-memory-size - When reach, block writes.

## TSM files
```go
// +--------+------------------------------------+-------------+--------------+
// | Header |               Blocks               |    Index    |    Footer    |
// |5 bytes |              N bytes               |   N bytes   |   4 bytes    |
// +--------+------------------------------------+-------------+--------------+  
```
One data block only contains data in just one time-series.  
Index in fact is `Indexes` containing index entry 1:1 to data block. An index entry is shown as below:
```go
// +-----------------------------------------------------------------------------+
// │                                   Index                                     │
// +-----------------------------------------------------------------------------+
// │ Key Len │   Key   │ Type │ Count │Min Time │Max Time │ Offset │  Size  │...│
// │ 2 bytes │ N bytes │1 byte│2 bytes│ 8 bytes │ 8 bytes │8 bytes │4 bytes │   │
// +-----------------------------------------------------------------------------+
```
Footer contains offset of Index.  
The data block is compressed as follows:
```go
// +--------------------------------------------------+
// | Type  |  Len  |   Timestamps    |      Values    |
// |1 Byte | VByte |     N Bytes     |    N Bytes     │
// +--------------------------------------------------+
// The Type is about how to compress.
```
## Compactions
There are several stages.
### Snapshots
Cache(memory) and WAL(disk storage) to TSM files, according to memory and time threshold.
### Level Compactions
Snapshots -> Level 1 files -> Level 2 files -> Level 3 files -> Level 4 files(end). Compactions in different level is of different strategy.
### Index Optimization
After the Level 4 files accumulate, the internal index are larger and harder to access. Such compactions are to make sure one file only contains points from minimum time-series.
### Full Compaction

## Operates
### Writes
### Updates
### Deletes
The engine now only support series level deletes?  
Delete will first write delete entry to WAL, then update the filestorage and cache. The cache evicts all relevant entries. The filestorage writes a tombstone for each TSM files containing the relevant entries.The tombstone file are used at startup time to ignore some data as well as during compaction time to remove entries.

# Line Protocol

The line protocol is a text based format for writing points to InfluxDB.  Each line defines a single point.
Multiple lines must be separated by the newline character `\n`. The format of the line consists of three parts:

```
[key] [fields] [timestamp]
```

Each section is separated by spaces.  The minimum required point consists of a measurement name and at least one field. Points without a specified timestamp will be written using the server's local timestamp. Timestamps are assumed to be in nanoseconds unless a `precision` value is passed in the query string.

## Key

The key is the measurement name and any optional tags separated by commas.  Measurement names, tag keys, and tag values must escape any spaces or commas using a backslash (`\`). For example: `\ ` and `\,`.  All tag values are stored as strings and should not be surrounded in quotes.

Tags should be sorted by key before being sent for best performance. The sort should match that from the Go `bytes.Compare` function (http://golang.org/pkg/bytes/#Compare).

### Examples

```
# measurement only
cpu

# measurement and tags
cpu,host=serverA,region=us-west

# measurement with commas
cpu\,01,host=serverA,region=us-west

# tag value with spaces
cpu,host=server\ A,region=us\ west
```

## Fields

Fields are key-value metrics associated with the measurement.  Every line must have at least one field.  Multiple fields must be separated with commas and not spaces.

Field keys are always strings and follow the same syntactical rules as described above for tag keys and values. Field values can be one of four types.  The first value written for a given field on a given measurement defines the type of that field for all series under that measurement.

* _integer_ - Numeric values that do not include a decimal and are followed by a trailing i when inserted (e.g. 1i, 345i, 2015i, -10i). Note that all values must have a trailing i. If they do not they will be written as floats.
* _float_ - Numeric values that are not followed by a trailing i. (e.g. 1, 1.0, -3.14, 6.0+e5, 10).
* _boolean_ - A value indicating true or false.  Valid boolean strings are (t, T, true, TRUE, f, F, false, and FALSE).
* _string_ - A text value.  All string values _must_ be surrounded in double-quotes `"`.  If the string contains
a double-quote or backslashes, it must be escaped with a backslash, e.g. `\"`, `\\`.


```
# integer value
cpu value=1i

cpu value=1.1i # will result in a parse error

# float value
cpu_load value=1

cpu_load value=1.0

cpu_load value=1.2

# boolean value
error fatal=true

# string value
event msg="logged out"

# multiple values
cpu load=10,alert=true,reason="value above maximum threshold"
```

## Timestamp

The timestamp section is optional but should be specified if possible.  The value is an integer representing nanoseconds since the epoch. If the timestamp is not provided the point will inherit the server's local timestamp.

Some write APIs allow passing a lower precision.  If the API supports a lower precision, the timestamp may also be
an integer epoch in microseconds, milliseconds, seconds, minutes or hours.

## Full Example
A full example is shown below.
```
cpu,host=server01,region=uswest value=1 1434055562000000000
cpu,host=server02,region=uswest value=3 1434055562000010000
```
In this example the first line shows a `measurement` of "cpu", there are two tags "host" and "region, the `value` is 1.0, and the `timestamp` is 1434055562000000000. Following this is a second line, also a point in the `measurement` "cpu" but belonging to a different "host".
```
cpu,host=server\ 01,region=uswest value=1,msg="all systems nominal"
cpu,host=server\ 01,region=us\,west value_int=1i
```
In these examples, the "host" is set to `server 01`. The field value associated with field key `msg` is double-quoted, as it is a string. The second example shows a region of `us,west` with the comma properly escaped. In the first example `value` is written as a floating point number. In the second, `value_int` is an integer.

# Distributed Queries
