# A Summary of Work so far (By Sept. 3rd, 2023)
You should read readme.md first to get a basic  understanding of this project.
## The Project Structure
This repo is basically built from tsdb package of InfluxDB. Our implementation is within \pkg\tsdb\index\tsi2 folder. (Here, *tsi2* is an alias for *Grid Index*.) Here's some information about the tsi2  package.
- grid.go. Contains the grid struct.
- optimizer.go. Contains the optimizer struct. It is used for creating new grid.
- grid_index.go. Contains the grid_index struct. It contains multiple grids for a single measurement.
- index.go. Encapsulates the index with standard interface. Serves all measurements.
- grid_block.go & index_file & measurement block. For persistence.  
- Also, there's also a breaking change outside the package. Since now the ids are pre-allocated in index,  we should pass them to series file to store (rather than generated in series file, then pass to index). The related functions are normally named with suffix of *WithDesignatedIDs, for example, CreateSeriesListIfNotExistsWithDesignatedIDs.
## How to Build and Run
- It is not a service but a package, so you can just call any functions you want in main.go. 
- There's also a simple Makefile in tsi2 folder, including some commands to run test and benchmark.
## To-do  List
- Persistence. The file structure and 0-level compact function is completed, the remaining work is to implement trigger strategy. We should keep it just like tsi1.
- Compaction. So far, the flush (0-level compaction) is completed, while  

