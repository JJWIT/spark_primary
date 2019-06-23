package com.jjw.spark.test2

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jiajianwei1 on 2019/1/30.
  */
object Test1 {

  def main(args: Array[String]): Unit = {
    val FAST_RIDE_HAITOU_ACTION = Tuple2(40010010, 1)
    println(FAST_RIDE_HAITOU_ACTION)

    val SOURCE_AD_FIRST_LEVEL = 60010000
    val SOURCE_FAST_RIDE_HAITOU = 60010010

    val STATUS_TOUCH_SPOT_TRANS_DRILL_LEVEL_2_CODE_A1 = Map(
      FAST_RIDE_HAITOU_ACTION._1 -> Tuple2(SOURCE_AD_FIRST_LEVEL,SOURCE_FAST_RIDE_HAITOU)
    )

    println(STATUS_TOUCH_SPOT_TRANS_DRILL_LEVEL_2_CODE_A1)
    println(FAST_RIDE_HAITOU_ACTION)
  }
}
