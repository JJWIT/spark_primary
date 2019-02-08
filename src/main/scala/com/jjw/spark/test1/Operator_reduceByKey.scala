package com.jjw.spark.test1

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Operator_reduceByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("reduceByKey")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("./words.txt")
    val flatMap = lines.flatMap { _.split(" ")}
    val map = flatMap.map {(_,1)}
    map.reduceByKey(_+_).foreach{println}
    sc.stop()
  }
}