package com.jjw.spark.test1

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object test {
  def main(args: Array[String]): Unit = {
    /**
     * SparkConf可以配置：1.Spark的运行模式  2.Spark的运行参数 3.设置运行的appName
     */
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("test")
    /**
     * SparkContext是Spark上下文，是通往集群的唯一通道
     */
    val sc = new SparkContext(conf)
    val lineRDD = sc.textFile("./words.txt")
    val wordRDD = lineRDD.flatMap { line=>{line.split(" ")} }
    val pairRDD = wordRDD.map { word => {(word,1)} }
    val reduceRDD  = pairRDD.reduceByKey((v1:Int,v2:Int) =>{
      println("********")
      v1+v2
      })
      
    /*==================sortBykey=======================*/
    val mapRDD1 = reduceRDD.map(tuple=>{(tuple._2,tuple._1)})
    val sortRDD = mapRDD1.sortByKey(false)
    val mapRDD2 = sortRDD.map(tuple=>{(tuple._2,tuple._1)})
      
//    mapRDD2.foreach(println)
//    val result = mapRDD2.first()
    val result = mapRDD2.take(2)
    result.foreach(println)
    /*==================filter=======================*/
//    val filterRDD = lineRDD.filter { line => !line.equals("hello bjsxt") }
//    val result = filterRDD.collect()
//    result.foreach{
//      elem => {
//        println(elem)
//      }
//    }
    /*==================sample=======================*/
    /**
     * sample 第一个参数 true代表有放回抽样，false代表无放回抽样。第二个参数代表抽样比例
     * 第三个参数代表种子，如果对同一批数据相同种子随机抽样，那么收到的结果相同。
     */
//    val sampleRDD = lineRDD.sample(true, 0.1,10)
//    sampleRDD.foreach { println }
    
    
    /**
     * 释放资源
     */
    sc.stop()
   
  }
}