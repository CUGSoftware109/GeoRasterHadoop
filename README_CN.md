# GeoRasterHadoop


  GeoRasterHadoop是基于Hadoop分布式计算框架的地理栅格数据的分布式存储与地图代数并行算法实现，主要分为基于HDFS的栅格数据的瓦片划分存储策略实现和基于瓦片划分数据实现的分布式地图代数算法。



# 功能点

1.实现了基于HDFS的瓦片栅格文件的切割以及瓦片栅格文件的压缩；

2.针对瓦片栅格文件的存储策略，设计并实现了基于Hadoop的分布式栅格数据地图代数计算，包括Add、GreaterThan、LocalMax以及MountainReclassify等算法；





# 项目结构

/src/main/java/cug/hadoop/geo/fileInfo/：记录原始栅格数据头文件信息和瓦片划分信息辅助类。

/src/main/java/cug/hadoop/geo/tile/split：将原始栅格数据切分为适用于HDFS分布式存储的栅格瓦片数据，分为未压缩的栅格瓦片切分方法以及压缩的栅格瓦片切分方法。

/src/main/java/cug/hadoop/geo/tile/merge：由于分布式计算结果为多个栅格数据文件且不包含元数据信息，此包用于将分布式计算结果文件还原为可进行地图可视化的标准栅格文件。

/src/main/java/cug/hadoop/geo/fileFormat/：此包主要实现了Hadoop计算时对输入栅格文件的特定的读取方式和计算结果的写出方式，读取类主要作用是将输入的瓦片数据解析为键值对形式供Hadoop的Map阶段进行计算；写出类是对MapReduce输出结果持久化文件的一些特殊处理。

/src/main/java/cug/hadoop/geo/algorithm/：此包下为Hadoop分布式地图代数计算的实现类，包括Add、GreaterThan、LocalMax以及MountainReclassify等分布式地图代数算法。

/src/main/java/cug/hadoop/geo/utils/：实现过程中用到的各种工具类。




# 运行环境和开发工具

运行环境：Centos6.5、JDK1.7、Hadoop2.6.0、zookeeper-3.4.8、HBase-1.1.4

开发工具：Eclipse、Maven


# 如何运行项目

瓦片切分：运行/src/main/java/cug/hadoop/geo/tile/split目录下的ZIPTile类，将目标栅格数据文件切分为瓦片文件。

分布式算子运行：选择/src/main/java/cug/hadoop/geo/algorithm/下的某个类作为入口类打包为jar，将jar包上传到集群服务器，使用Hadoop命令运行jar。