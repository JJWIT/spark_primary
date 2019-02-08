package com.jjw.spark.test2

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jiajianwei1 on 2019/2/1.
  */
object CacheTest2 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)
    val data = sc.textFile("./words.txt")
    data.take(10).foreach(x => println(x))

    sc.stop()

  }

}
