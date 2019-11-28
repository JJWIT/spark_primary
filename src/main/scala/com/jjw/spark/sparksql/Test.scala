package com.jjw.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.collect_list
import org.apache.spark.{SparkConf, sql}

/**
  * Created by jiajianwei1 on 2019/2/1.
  */
object Test {

  def main(args : Array[String]) : Unit = {
    // 1.创建自己的配置文件
    val conf = new SparkConf().setMaster("local").setAppName("test")

    // 2.创建SparkSession
    val sparkSession = SparkSession.builder().appName("Spark SQL basic example ...").config(conf).getOrCreate()
    val originalDf = sparkSession.read.json("./temp1.json")
    originalDf.createOrReplaceTempView("app_dm_cate_4a_status_v5")
    originalDf.show()
    val originalRePartDf = originalDf.repartition(originalDf("cate_code"), originalDf("pt_key"))
    val groupByStatusDf = originalRePartDf.groupBy("cate_code", "pt_key", "status").agg(
      collect_list("touch_spot")
    )

    groupByStatusDf.show()

   /* import sparkSession.implicits._
    val touchSpotDrillDf = groupByStatusDf.map(
      row => {
        val actionKeysList = row.getAs[Seq[Seq[Int]]]("collect_list(touch_spot)")
        val status = row.getAs[Int]("status")
        println(actionKeysList + "--" + status)
      }
    ).toDF("cate_code", "status", "level1_touch_spot_result", "level2_touch_spot_result")
*/
  }
}
