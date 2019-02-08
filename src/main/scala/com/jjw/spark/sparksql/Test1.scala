package com.jjw.spark.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by jiajianwei1 on 2019/2/1.
  */
object Test1 {

  def main(args : Array[String]) : Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test1")
    val sparkSession = SparkSession
      .builder()
      .appName("Spark SQL basic example")
//      .config("spark.some.config.option", "some-value")
      .config(conf)
      .getOrCreate()

    import sparkSession.implicits._

    val df = sparkSession.read.json("./people.json")
    df.show()

    /*println("--------------------------")
    df.printSchema()
    println("--------------------------")
    df.select("name").show()
    println("--------------------------")
    df.select($"name", $"age" + 1).show()
    println("--------------------------")
    df.filter($"age" > 21).show()
    println("--------------------------")
    df.groupBy("age").count().show()*/

    // 以编程方式运行sql查询
    // 将DataFrame注册为sql查询
    /*df.createOrReplaceTempView("people")
    val sqlDF = sparkSession.sql("select * from people")
    sqlDF.show()*/

    // 临时试图作用域只是该session，可以创建全局临时试图。全局临时视图生命周期是application级别,查询视图要用global_temp.viewName
    df.createGlobalTempView("peopleGlobal")
    sparkSession.sql("select * from global_temp.peopleGlobal").show()

    sparkSession.newSession().sql("select * from global_temp.peopleGlobal").show()
  }
}
