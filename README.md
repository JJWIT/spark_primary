spark项目 2019-01-30
====
### 说明
    记录学习scala、spark相关笔记
### 新建spark项目
1.使用maven或sbt方式，建议使用maven
2.参考网址 https://blog.csdn.net/u013963380/article/details/72677212

### 项目报错
1.Exception in thread "main" java.lang.NoSuchMethodError: scala.Predef$.refArrayOps([Ljava/lang/Object;)Lscala/collection/mutable/ArrayOps;
    -- 修改scala sdk 与 spark 版本兼容
2.运行val sc = new SparkContext(conf)时候报错：java.io.IOException: Could not locate executable null\bin\winutils.exe in the Hadoop binaries.
    -- 原因，spark源码中找winutils这个文件。windows中该文件用于模拟shell环境的
    -- 解决方案：https://stackoverflow.com/questions/35652665/java-io-ioexception-could-not-locate-executable-null-bin-winutils-exe-in-the-ha/46038984
    ①将winutils.exe放到hadoop_home\bin下
    正常来讲就可以正常运行了，有的时候需要重启电脑，如果还不行需要使用代码如下：
    ②代码变成添加System.setProperty("hadoop.home.dir", "D:\\WorkSoftware\\hadoop-2.8.5"); 添加hadoop.home.dir位置
3.

### spark编程(scala)
    spark代码流程
    1.	创建SparkConf对象
        	可以设置Application name。
        	可以设置运行模式及资源需求。
    2.	创建SparkContext对象
    3.	基于Spark的上下文创建一个RDD，对RDD进行处理。
    4.	应用程序中要有Action类算子来触发Transformation类算子执行。
    5.	关闭Spark上下文对象SparkContext。

1.初始化spark
    val conf = new SparkConf().setAppName(appName).setMaster(master)
    new SparkContext(conf)
2.弹性分布式数据集
    -- 并行集合通过调用创建SparkContext的parallelize一个现有的收集方法
    val data = Array(1, 2, 3, 4, 5) // 自己创建数据集
    val distData = sc.parallelize(data)
3.外部数据集
    -- Spark可以从Hadoop支持的任何存储源创建分布式数据集，包括本地文件系统，HDFS，Cassandra，HBase，Amazon S3等.
       Spark支持文本文件，SequenceFiles和任何其他Hadoop InputFormat。
    val distFile = sc.textFile("data.txt")  //  加载外部数据集
4.算子(常见32个，参考 http://spark.apache.org/docs/2.3.0/rdd-programming-guide.html)
Transformations转换算子(20个)
  	map
  将一个RDD中的每个数据项，通过map中的函数映射变为一个新的元素。
  特点：输入一条，输出一条数据。
  	mapPartitions
   与map相比，mapPartitions作用于每个partion中，如果用于持久化到数据中可以使用mapPartitions
  	flatMap
  先map后flat。与map类似，每个输入项可以映射为0到多个输出项。
  	filter
  过滤符合条件的记录数，true保留，false过滤掉。
  	sample
  随机抽样算子，根据传进去的小数按比例进行又放回或者无放回的抽样。
  	reduceByKey
  将相同的Key根据相应的逻辑进行处理。
  	sortByKey
  作用在K,V格式的RDD上，对key进行升序或者降序排序。
  	join,leftOuterJoin,rightOuterJoin,fullOuterJoin
  作用在K,V格式的RDD上。根据K进行连接，对（K,V）join(K,W)返回（K,(V,W)
  	join后的分区数与父RDD分区数多的那一个相同。
  	union
  合并两个数据集。两个数据集的类型要一致。
  	返回新的RDD的分区数是合并RDD分区数的总和。
  	intersection
  取两个数据集的交集
  	subtract
  取两个数据集的差集
  	mapPartiiton
  与map类似，遍历的单位是每个partition上的数据。
  	distinct(map+reduceByKey+map)
  	cogroup 
  当调用类型（K,V）和（K，W）的数据上时，返回一个数据集（K，（Iterable<V>,Iterable<W>））

Action算子
  	count
  返回数据集中的元素数。会在结果计算完成后回收到Driver端。
  	take(n)
  返回一个包含数据集前n个元素的集合。
  	first
  first=take(1),返回数据集中的第一个元素。
  	foreach
  循环遍历数据集中的每个元素，运行相应的逻辑。
  	collect
  将计算结果回收到Driver端。
控制算子
cache、persist、checkpoint

5.Spark中共享变量：广播变量和累加器
创建广播变量
    val broadcastVar = sc.broadcast(Array(1, 2, 3))
累加器

6.-- 有多少个action算子就会触发多少个job
  一个partition由一个task来处理
7.RDD持久化方式有三种：cache、persist、checkpoint

### spark优化
    1.rdd缓存、持久化
        多次使用同一个rdd可以对该rdd进行rdd.cache()或者rdd.persist()
    2.

### spark sql
    http://spark.apache.org/docs/latest/sql-programming-guide.html
    
#### 编码
1.

#### 错误
1.本地运行spark sql 报错Exception in thread "main" org.apache.spark.SparkException: A master URL must be set in your configuration
使用功能自己的conf 