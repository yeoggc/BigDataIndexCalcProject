package com.ggc.spark_streaming

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import com.ggc.spark_streaming.app.AreaAdsClickTop3
import com.ggc.spark_streaming.bean.AdsInfo
import com.ggc.spark_streaming.util.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object RealtimeApp extends App {

  val sparkConf = new SparkConf().setMaster("local[*]").setAppName(getClass.getSimpleName)
  val ssc = new StreamingContext(sparkConf, Seconds(5))
  ssc.checkpoint(s"SparkRealtimeProcessProject/spark-streaming-project/checkpoint/${getClass.getSimpleName}")

  // 从 kafka 读取数据, 为了方便后续处理, 封装数据到 AdsInfo 样例类中
  val dayFormatter: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
  val hmFormatter: SimpleDateFormat = new SimpleDateFormat("HH:mm")

  // 为了方便后面的计算, 把消费到的字符串封装到样例类中
  val adsInfoDStream =
    MyKafkaUtil.getKafkaStream(ssc, "ads_log")
      .map(record => { // 1569572798366,华南,深圳,101,2

        val arr = record.value().split(",")
        val date = new Date(arr(0).toLong)
        AdsInfo(
          arr(0).toLong,
          new Timestamp(arr(0).toLong),
          dayFormatter.format(date),
          hmFormatter.format(date),
          arr(1),
          arr(2),
          arr(3),
          arr(4)
        )
      })

//  adsInfoDStream.print()

  // 1. 需求1: 每天每地区热门广告 Top3
  AreaAdsClickTop3.statHotAdsClick4PerDayArea(adsInfoDStream)

  // 2. 需求2: 最近 1 小时广告点击量实时统计

  ssc.start()
  ssc.awaitTermination()
}
