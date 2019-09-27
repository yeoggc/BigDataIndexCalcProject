package com.atguigu.realtime.app

import com.atguigu.realtime.bean.AdsInfo
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * 统计黑名单
 * 
 */
object BlackListApp {

  def statBlackList(spark: SparkSession, adsInfoDS: Dataset[AdsInfo]) = {
    import spark.implicits._

  }

}
