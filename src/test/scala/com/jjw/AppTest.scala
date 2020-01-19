package com.jjw

import org.junit._
import Assert._

@Test
class AppTest {

  @Test
  def testOK() = assertTrue(true)

  //    @Test
  //    def testKO() = assertTrue(false)

  @Test
  def test01(): Unit = {
    val finalSelect = Seq(
      "main_brand_code",
      "brand_code",
      "third_cate_code",
      "sku_id",
      "channel_code",
      "user_pin",
      "placement_id",
      "plan_id",
      "group_id",
      "ad_id",
      "business_type",
      "ad_traffic_type",
      "campaign_type",
      "ad_billing_type",
      "bid_price",
      "activity_id",
      "activity_type",
      "delivery_system_type",
      "dt")

    val originalSelect = finalSelect.tail
    println(finalSelect)
    println(originalSelect)

    val txSelect = originalSelect.map(col => if(col == "user_pin") "open_user_pin AS user_pin" else col)

    println(txSelect)
  }

}


