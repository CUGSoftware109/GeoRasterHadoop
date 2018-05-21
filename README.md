# HadoopGeoCompute

  HadoopGeoCompute是基于Hadoop分布式计算框架的地理栅格数据的分布式存储与地图代数并行算法实现，主要分为基于HDFS的栅格数据的瓦片划分存储策略实现和基于瓦片划分数据实现的分布式地图代数算法。


# 功能点

1.实现了基于HDFS的瓦片栅格文件的切割以及瓦片栅格文件的压缩；

2.针对瓦片栅格文件的存储策略，设计并实现了基于Hadoop的分布式栅格数据地图代数计算，包括Add、GreaterThan、LocalMax以及MountainReclassify等算法；

3.利用集群的GPU计算资源，采用JCuda结合Hadoop，实现能够利用集群GPU资源的Hadoop分布式栅格数据地图代数算法。


# 项目结构

/src/main/java/cug/hadoop/geo/fileInfo/：记录原始栅格数据头文件信息和瓦片划分信息辅助类。

/src/main/java/cug/hadoop/geo/tile/split：将原始栅格数据切分为适用于HDFS分布式存储的栅格瓦片数据，分为未压缩的栅格瓦片切分方法以及压缩的栅格瓦片切分方法。

/src/main/java/cug/hadoop/geo/tile/merge：由于分布式计算结果为多个栅格数据文件且不包含元数据信息，此包用于将分布式计算结果文件还原为可进行地图可视化的标准栅格文件。

/src/main/java/cug/hadoop/geo/fileFormat/：此包主要实现了Hadoop计算时对输入栅格文件的特定的读取方式和计算结果的写出方式，读取类主要作用是将输入的瓦片数据解析为键值对形式供Hadoop的Map阶段进行计算；写出类是对MapReduce输出结果持久化文件的一些特殊处理。

/src/main/java/cug/hadoop/geo/algorithm/：此包下为Hadoop分布式地图代数计算的实现类，，包括Add、GreaterThan、LocalMax以及MountainReclassify等分布式地图代数算法。

/src/main/java/cug/hadoop/geo/algorithm/gpu/：此包下为地图代数计算在Hadoop+GPU环境下的分布式算法实现，该算法在利用Hadoop集群并行加速计算任务的同时，充分发挥了单台计算节点上GPU物理资源，加速单个任务的计算速度，使计算任务的整体时间减少。

/src/main/java/cug/hadoop/geo/utils/：实现过程中用到的各种工具类。
