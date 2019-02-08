package com.jjw.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, sql}

/**
  * Created by jiajianwei1 on 2019/2/1.
  */
object Test {

  def main(args : Array[String]) : Unit = {
    // 1.创建自己的配置文件
    val conf = new SparkConf().setMaster("local").setAppName("test")

    // 2.创建SparkSession
    val sparkSession = SparkSession.builder().appName("Spark SQL basic example ...").config(conf).getOrCreate()
    val df = sparkSession.read.json("./people.json")
    df.show()

  }
}
