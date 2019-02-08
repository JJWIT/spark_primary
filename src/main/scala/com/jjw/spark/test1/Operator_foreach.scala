package com.jjw.spark.test1

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Operator_foreach {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("foreach")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array(1,2,3,4))
    rdd1.foreach { println }
    
    sc.stop()
  }
}