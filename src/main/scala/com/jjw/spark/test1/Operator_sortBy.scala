package com.jjw.spark.test1

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Operator_sortBy {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("sortBy")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("./words.txt")
    val reduceResult = lines.flatMap { _.split(" ")}.map{ (_,1)}.reduceByKey(_+_)
    val result = reduceResult.sortBy(_._2,false)
    result.foreach{println}
    sc.stop()
  }
}