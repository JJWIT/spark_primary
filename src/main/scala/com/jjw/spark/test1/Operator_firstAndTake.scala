package com.jjw.spark.test1

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Operator_firstAndTake {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("firstAndTake")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("./words.txt")
    val reduceResult = lines.flatMap { _.split(" ")}.map { (_,1)}.reduceByKey(_+_)
    reduceResult.take(1).foreach(println)
    println(reduceResult.first()+"~")
    sc.stop()
  }
}