package com.jjw.spark.test1

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jiajianwei1 on 2019/2/8.
  */
object WordCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("wordcount").setMaster("local")
    val sc = new SparkContext(conf)
    // sc.textFile("./words.txt").flatMap(x => x.split(" ")).map(x => (x, 1)).reduceByKey((x, y) => x + y).foreach(println)
     sc.textFile("./words.txt").flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).foreach(println)
//    sc.textFile("./words.txt").flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _, 2).foreach(println) // reduceByKey中2代表2个分区
    /*val data = sc.textFile("./words.txt")
    val rdd1 = data.map(_.split(" ")).foreach(println)
    val rdd2 = data.flatMap(_.split(" ")).foreach(println)
    println()*/
  }
}
