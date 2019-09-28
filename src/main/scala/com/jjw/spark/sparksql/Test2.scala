package com.jjw.spark.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
  * Created by jiajianwei1 on 2019/2/1.
  */
object Test2 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test1")
    val sparkSession = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      //      .config("spark.some.config.option", "some-value")
      .config(conf)
      .getOrCreate()

    val df = sparkSession.read.json("./mac.json")
    System.out.print("mac.json......")
//    df.show()
    df.createOrReplaceTempView("app_m14_y_busdep_mac_pin")

    var dimensionStr = "pin1"
    val rowDatasetSource = sparkSession.read.json("./macresourse.json")
    rowDatasetSource.show()

    val deviceType = 48;
    val mappingDataset = sparkSession.read.table("app_m14_y_busdep_mac_pin ").filter("mac is not null and user_log_acct is not null").dropDuplicates("mac", "user_log_acct").selectExpr("mac as pin1", "user_log_acct as pin2").repartition(1000).persist(StorageLevel.MEMORY_AND_DISK_SER_2);
    mappingDataset.show()
    val matchingPinDataset = mappingDataset.filter("pin2 is not null").join(rowDatasetSource.selectExpr(dimensionStr), "pin1").select("pin2").persist(StorageLevel.MEMORY_AND_DISK_SER_2);
    matchingPinDataset.show()
    val mismatchingPinDataset = rowDatasetSource.selectExpr(dimensionStr).except(mappingDataset.select("pin1")).selectExpr("concat('d:" + deviceType + ":', pin1) as pin2").persist(StorageLevel.MEMORY_AND_DISK_SER_2);
    mismatchingPinDataset.show()
    val rowDataset = matchingPinDataset.union(mismatchingPinDataset).sortWithinPartitions("pin2").dropDuplicates("pin2").persist(StorageLevel.MEMORY_AND_DISK_SER_2);
    rowDataset.show()
  }
}
