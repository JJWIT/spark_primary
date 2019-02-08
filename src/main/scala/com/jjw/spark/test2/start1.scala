package com.jjw.spark.test2

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jiajianwei1 on 2019/1/30.
  */
object start1 {

  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val conf = new SparkConf().setAppName("myapp").setMaster("local")
    // 2.创建上下文对象
    val sc = new SparkContext(conf)
    // 3.读取文件
    val data = Array(1, 2, 4, 3, 5, 6, 7)
    val rdd = sc.parallelize(data, 2)

    val rdd1 = rdd.mapPartitions(x => x ).foreach(println)
    sc.stop()
  }
}
