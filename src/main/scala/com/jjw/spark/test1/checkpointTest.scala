package com.jjw.spark.test1

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object checkpointTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("checkpoint")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("./checkpoint")
    val lines = sc.textFile("./NASA_access_log_Aug95")
    lines.checkpoint()
    lines.foreach { println}
    sc.stop()
  }
}