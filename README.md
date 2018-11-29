# GeoRasterHadoop


  GeoRasterHadoop is a distributed storage and map algebraic parallel algorithm based on Hadoop distributed computing framework. It is divided into tile-based storage strategy on HDFS for raster data and map algebraic parallel algorithm based on distributed tile storage strategy. 

[Chinese-Introduction](https://github.com/CUGSoftware109/GeoRasterHadoop/blob/master/README_CN.md)

# Function

1.Implements HDFS-based tile raster file cutting and tile raster file compression.

2.For the storage strategy of tile raster files, a distributed raster data map algebra calculation based on Hadoop is designed and implemented, including Add, GreaterThan, LocalMax, and MountainReclassify algorithms.




# Project structure

/src/main/java/cug/hadoop/geo/fileInfo/：Record the original raster data header file information and tile partition information auxiliary class.

/src/main/java/cug/hadoop/geo/tile/split：The original raster data is divided into grid tile data suitable for HDFS distributed storage, which is divided into an uncompressed raster tile segmentation method and a compressed raster tile segmentation method.

/src/main/java/cug/hadoop/geo/tile/merge：Since distributed computing results are multiple raster data files and do not contain metadata information, this package is used to restore the distributed calculation result file to a standard raster file for map visualization.

/src/main/java/cug/hadoop/geo/fileFormat/：This package mainly realizes the specific reading mode and writing method of the input raster file when Hadoop is calculated. The main function of the reading class is to parse the input tile data into key value pairs for the Hadoop Map stage calculating; write classes is a special function of MapReduce output results persistence files

/src/main/java/cug/hadoop/geo/algorithm/：This package is an implementation class for Hadoop distributed map algebraic calculations, including distributed map algebraic algorithms such as Add, GreaterThan, LocalMax, and MountainReclassify.

/src/main/java/cug/hadoop/geo/utils/：Various tools used in the implementation process.




# Running Environment and Development Tools

Running Environment：Centos6.5、JDK1.7、Hadoop2.6.0、zookeeper-3.4.8、HBase-1.1.4

Development Tools：Eclipse、Maven


# How to run

Tile splitting: Run the ZIPTile class in the /src/main/java/cug/hadoop/geo/tile/split directory to split the target raster data file into tile files.

Distributed operator operation: Select a class under /src/main/java/cug/hadoop/geo/algorithm/ as the entry class to be packaged as a jar, upload the jar package to the cluster server, and run the jar using the Hadoop command.