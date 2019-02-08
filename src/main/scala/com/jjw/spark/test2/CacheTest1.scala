package com.jjw.spark.test2

import org.apache.spark.{SparkConf, SparkContext}

object CacheTest1 {

  def test1 (): Unit = {
    // 1.初始化spark配置文件
    val conf = new SparkConf().setMaster("local").setAppName("cacheTest1")

    // 2.获取上下文对象
    val sc = new SparkContext(conf)

    // Parallelized Collections
    val data = Array(1, 2, 3, 4, 5)
    // 复制集合的元素以形成可以并行操作的分布式数据集
    val distData = sc.parallelize(data)

    // distData.foreach(x => println(x))
    println(distData.count())
  }

  def test2 (): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("test2")
    val sc = new SparkContext(conf)

    val data = sc.textFile("./README.md")
    // data.map(x => x + "#").foreach(x => println(x))
    //  data.flatMap(x => x + "#").foreach(x => println(x))

  }

  def test3 () :Unit ={
    val conf = new SparkConf().setMaster("local").setAppName("test3")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(Array(1, 2, 3, 4), 1) // 后面1，代表有几个partition,默认为1
    rdd.foreach(x => println(x))
    rdd.map(x => x*x).foreach(println) // 1 4 9 16
  }

  def test4 (): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test4")
    val sc = new SparkContext(conf)
    /*sc.textFile("./README.md").flatMap(x => x.split(" ")).map(x => (x, 1)).reduceByKey((a, b) => {
      var a1 = a
      var b1 = b
      a+b
    }).foreach(x => println(x))*/
//    sc.textFile("./README.md").map(x => (x, 1)).sortByKey().saveAsTextFile("./abc")
    val broadcastVar = sc.broadcast(Array(1, 2, 3))
    println(broadcastVar.value)

  }

  def main(args: Array[String]): Unit = {
    test4()
  }
}