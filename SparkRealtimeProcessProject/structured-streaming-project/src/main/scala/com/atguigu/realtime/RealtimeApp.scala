package com.atguigu.realtime

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.realtime.app.BlackListApp
import com.atguigu.realtime.bean.AdsInfo
import org.apache.spark.sql.SparkSession

object RealtimeApp extends App {

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName(getClass.getSimpleName)
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  val dayStringFormatter: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
  val hmStringFormatter: SimpleDateFormat = new SimpleDateFormat("HH:mm")

  // 得到的 df 的 schema 是固定的: key,value,topic,partition,offset,timestamp,timestampType
  val adsInfoDS = spark.readStream
    .format("kafka") // 设置 kafka 数据源
    .option("kafka.bootstrap.servers", "dw1:9092,dw2:9092,dw3:9092")
    .option("subscribe", "ads_log") // 也可以订阅多个主题:   "topic1,topic2"
    .load
    .select("value")
    .as[String]
    .map(v => {
      val split: Array[String] = v.split(",")
      val date: Date = new Date(split(0).toLong)
      AdsInfo(split(0).toLong,
        new Timestamp(split(0).toLong),
        dayStringFormatter.format(date), hmStringFormatter.format(date),
        split(1), split(2), split(3), split(4))
    })

  BlackListApp.statBlackList(spark, adsInfoDS)

//  adsInfoDS.writeStream
//    .format("console")
//    .outputMode("update")
//    .option("truncate", "false")
//    .start
//    .awaitTermination()

}
