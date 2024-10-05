# Grid Index: an Optimized Index Addressing High Cardinality in IoT Time-Series

In the realm of IoT, devices generate vast quantities of time-series data with high cardinality. Cardinality, in the context of databases, refers to the number of unique values in a dataset's column.

High cardinality in time-series databases, especially in IoT applications, is a more complex challenge for two reasons. First, time-series data are typically organized into series [1]. A time series of data has the same values for tags but different timestamps. So for this data model, the initial step in a query operation is to identify the target series based on specified tag values, and TSDB normally builds an index on these tag values. For example, in a query like "depth > 10m and location = '41° N, 87° W'," identifying the corresponding series IDs is necessary before the actual data can be retrieved. As cardinality increases, designing a fast and efficient indexing method to locate these series IDs becomes increasingly difficult. Second, IoT data often contains numerous identifiers (e.g., sensor IDs, instances) and metadata tag values.

High cardinality can severely impact write, query, and especially, storage efficiency. To mitigate these issues, some time-series databases introduce inverted indexes to map tag values to series IDs [3]. However, while this approach can accelerate queries in most cases, it encounters significant storage overhead due to the redundancy of series IDs across different tags [4]. Specifically, if there are *k* tag sets and *n* series, the inverted index must expand to *k\*n*, requiring the series IDs to be stored separately in the posting lists for each tag set. Additionally, for optimal query speed, the index is typically expected to be fully loaded into memory or cache. However, as the index size grows, it may exceed available memory, causing it to swap to disk. This results in high disk I/O and a noticeable decline in query performance.

To address these issues, this research aims to propose a novel index to function similarly to an inverted index but with significantly reduced storage overhead. It should also map specified tag values to a list of corresponding series IDs. Our potential solution models multi-dimensional tag keys as a hyperdimensional space (a "grid"), where points represent series IDs. The points inside the space are given ordered serial ids which are exactly the series ids. The grid will store the order of tag keys and the order of tag values, through which the ids of points can be accessed with calculation. It is worth noticing that it is actually unnecessary to store the space and ids and thus save storage.

This approach limits the impact of high cardinality by compressing index storage and avoiding the redundancy inherent in traditional inverted indexes. Also, for queries specifying multiple tag values, the intersection operation can be pushed down and replaced with a step in computation, which may even bring benefits for reading.

By optimizing storage and query efficiency in time-series databases, this new index can significantly enhance the performance and scalability of IoT systems. This improvement is crucial for applications with large-scale sensor networks and real-time data analysis. The reduction in storage overhead will make it feasible to handle high-cardinality data, especially for those resource-limited devices, thus broadening the scope of IoT applications.

# Methods

Some time-series databases utilize inverted indexes to accelerate queries, which allows for efficient filtering and retrieval of time-series data based on specific tag values. However, an inverted index is a trade-off between storage usage and query performance. The posting list, a data structure to store series identifiers associated with each term in the index, will store redundant identifiers among different tag sets. Since time-series databases always deal with large-scale data in scenarios, e.g., IoT sensors, such storage overhead is extremely severe.

To address this issue, we propose the "Grid Index," which functions similarly to an inverted index but with more efficient storage usage. The Grid Index models multi-dimensional indexes as a hyperdimensional space where points represent time-series IDs. For example, the grid C for Measurement Y in Figure 1 illustrates this concept. If we are indexing http access data, there may be tags for *Status_Code* and *Method*, which together form a two-dimensional space used to pre-allocate possible series IDs.

**Data Structure:** A grid represents a multi-dimensional space where each dimension corresponds to a series of tag values for a specific tag key. Points within this space are assigned ordered serial identifiers, which directly correspond to the time-series data IDs. This structure allows the grid to avoid explicit storage of redundant identifiers, thereby reducing memory usage.

![Read and Write Path for Grid Index](https://github.com/vinland-avalon/cycledb/blob/master/Architecture.png?raw=true)

**Get Series ID:** This operation returns a list of "hit series" IDs, referring to series that match the input tag keys and tag values. The lookup process in a grid is straightforward: it maps the input tag keys and values to specific dimensions and coordinate values, pinpointing the area of the space that contains the relevant IDs. The algorithm then calculates the IDs for all points within that sub-space and compiles them into a list. It repeats this process for each grid, adding any matched identifiers to the final result.

**Set Series Key:** The purpose of this operation is to index a new series key, which typically consists of tag pairs, and output an assigned ID that will serve as the series ID. The algorithm first checks if the series key has already been indexed; if so, it bypasses the operation. If not, it searches for an existing grid capable of accommodating the new series. The grid must 1) have dimensions that match those of the new series, and 2) have available slots to append values in any dimension lacking the specified tag value. If a suitable grid is found, the algorithm appends the values to the dimensions, which triggers the pre-allocation of identifiers. If no such grid exists, the index is expanded by creating a new grid and inserting the series there. The new grid’s dimensions will mirror those of the incoming series, with its capacity for each dimension determined by an internal optimizer.

**Experiments:** InfluxDB indexes measurement and tag info with Time Series Index (TSI). This is a time-series implementation for an inverted index [5]. We are going to follow the interface definition of TSI to build the newly-proposed grid index. The first set of experiments will compare the module-level performance between TSI and grid index. The metrics will include query throughput and memory or disk usage. The second set of experiments will focus on the end-to-end data system. As for workloads, there are three levels - auto-generated data, TPC-C / TPC-H benchmark [6], and IoT-specific sensor data [7].

# Get Started
This project utilize some package from [InfluxDB v2.x](https://github.com/influxdata/influxdb/tree/main-2.x). Please please follow the guidance of [Install](https://github.com/influxdata/influxdb/blob/main-2.x/CONTRIBUTING.md#building-from-source) to install some prerequisite before trying with unit tests and benchmarks. Otherwise there maybe "flux.pc not found".

## References

1. Rinaldi, S. et al. (2019). Impact of Cardinality in Time-Series.  
2. Sendra, S. et al. (2015). Oceanographic Multisensor Buoy System.  
3. [Inverted Index](#).  
4. [Inverted Index TSDB](#).  
5. Naqvi, A. et al. (2017). Time Series Indexing.  
6. Leutenegger, S. et al. (1993). TPC-C / TPC-H Benchmark.  
7. Mainwaring, A. et al. (2002). Wireless Sensor Data.

