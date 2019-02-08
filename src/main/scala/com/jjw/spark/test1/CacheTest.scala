package com.jjw.spark.test1

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object CacheTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("cacheTest")
    val sc = new SparkContext(conf)
    var lines = sc.textFile("./NASA_access_log_Aug95")
    lines = lines.cache()
    val startTime = System.currentTimeMillis()
    val count = lines.count()
    val endTime = System.currentTimeMillis()
    println("共"+count +"条数据，用时"+(endTime-startTime)+"ms")
    
    val startTime1 = System.currentTimeMillis()
    val count1 = lines.count()
    val endTime1 = System.currentTimeMillis()
    println("共"+count1 +"条数据，用时"+(endTime1-startTime1)+"ms")
    
    sc.stop()
  }
}