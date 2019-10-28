package com.ggc.spark_streaming.app

import com.ggc.spark_streaming.bean.AdsInfo
import com.ggc.spark_streaming.util.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Minutes, Seconds}
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

object LastHourAdsClickApp {
  def statLastHourClick(adsInfoDStream: DStream[AdsInfo]) = {
    // 统计最近一小时的数据(每分钟点击量), 每 5 秒统计一次

    adsInfoDStream
      .window(Minutes(60), Seconds(5))
      //((adsId,hourMinute),1)
      .map(adsInfo => {
        ((adsInfo.adsId, adsInfo.hmString), 1)
      })
      //((adsId,hourMinute),count)
      .reduceByKey(_ + _)
      .map { //(adsId,(hourMinute,count))
        case ((adsId, hm), count) => (adsId, (hm, count))
      }
      // (adsId,Iterable[(hourMinute,count)])
      .groupByKey()
      .map { // (adsId,Iterable[(hourMinute,count)])
        // 按照 hh:mm 升序排列
        case (adsId, it) => (adsId, it.toList.sortBy(_._1))
      }
      .foreachRDD(rdd => {
        rdd.foreachPartition(it => {
          val client: Jedis = RedisUtil.getJedisClient

          it.foreach {
            case (ads, hmCountList) =>
              import org.json4s.JsonDSL._
              client.hset("last_hour_click", ads,
                JsonMethods.compact(JsonMethods.render(hmCountList)))
          }

          client.close()
        })
      })

  }

}
