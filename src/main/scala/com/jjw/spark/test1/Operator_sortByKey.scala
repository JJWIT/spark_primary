package com.jjw.spark.test1

import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Operator_sortByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("sortByKey")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("./words.txt")
    val reduceResult = lines.flatMap { _.split(" ")}.map { (_,1)}.reduceByKey(_+_)
    reduceResult.map(f => {(f._2,f._1)}).sortByKey(false).map(f => {(f._2,f._1)}).foreach(println)
    
    sc.stop()
  }
}