# Index details
## Reference
https://cloud.tencent.com/developer/article/1397217
## File Organization
![organization](https://ask.qcloudimg.com/http-save/yehe-3003208/z0a5xwdxof.png?imageView2/2/w/2560/h/7000 "organization")
### SeriesFile
It could also be named as `SeriesKeyFile`, for it stores keys(measurement + tag set) for all series, and is shared by all shards, across the entire database.
### SeriesIndex
- Two maps there: sereisKey -> id, id -> offset
- seriesKey: measurement + tags;
- seriesID: in one DB, can be used to identify a unique sereis. About how to generate:  
Influxdb将paritition数量定死了为 8, 就是说所有的serieskey放在这8个桶里
如何确定放在哪个桶里呢？就是上面提到的计算SeriesKey的hash值然后取模parition个数 int(xxhash.Sum64(key) % SeriesFilePartitionN) 
所有这些partition的id是0 到 7, 每个partiton都有一个顺列号seq, 初始值为partition id + 1, 这个顺列号就是放入这个parition中的seriese key对应的id,每次增加 8, 比如对于1号partition, 第一个放入的series id就是2, 第二个就是10
有了上面的规则，从seriese id上就很容易得到它属于哪个 partition:int((id - 1) % SeriesFilePartitionN) 
将一系列的SeriesKey写入相应的Partiton, 写入哪个partition是计算SeriesKey的hash值然后取模parition个数 int(xxhash.Sum64(key) % SeriesFilePartitionN) 
- offset: upper32 bit -> segment id. lower32 bit -> offset in segment, It can be used to reach SeriesKey


# Question:
2. There's a Tag number field in SeriesSegment files, if so, how they handle series with different number of tags, or it does not mean so?

1. If the seriesFile is like https://cloud.tencent.com/developer/article/1397217, how could keys map to ids? I mean, there's no place to store id.  
They are not stored in SeriesFiles, in fact, it is stored in SeriesIndex in one of the map.

3. How seriesFile/seriesPartition make a key from mesurements and tag sets?  
In the order of input, see `GenerateSeriesKeys`

2. How will series ID be used?  
All in all, to reach SeriesKey. There are many functions:  
```go
func (idx *SeriesIndex) FindIDBySeriesKey(segments []*SeriesSegment, key []byte) uint64
func (idx *SeriesIndex) FindIDByNameTags(segments []*SeriesSegment, name []byte, tags models.Tags, buf []byte) uint64
func (idx *SeriesIndex) FindIDListByNameTags(segments []*SeriesSegment, names [][]byte, tagsSlice []models.Tags, buf []byte) (ids []uint64, ok bool)
func (idx *SeriesIndex) FindOffsetByID(id uint64) int64
```

3. However, it's not about invert index? They are from id to key?  
Yep...

