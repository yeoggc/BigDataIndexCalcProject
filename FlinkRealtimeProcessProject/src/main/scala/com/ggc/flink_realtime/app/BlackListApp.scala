package com.ggc.flink_realtime.app

import com.ggc.flink_realtime.bean.AdsInfo
import com.ggc.flink_realtime.util.RedisUtil
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.table.api.scala._
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis


object BlackListApp {
  def statBlackList(tEnv: StreamTableEnvironment, adsInfoDS: DataStream[AdsInfo]) = {


    val filterDS = adsInfoDS.process(new FilterProcessFunction)
    tEnv.registerDataStream("AdsInfo", filterDS)

    val resultTable =
      tEnv.sqlQuery(
        s"""
           |SELECT
           |  dayString,
           |  userId
           |FROM
           |  AdsInfo
           |GROUP BY dayString,userId,adsId
           |HAVING count(*) >= 1
           |""".stripMargin)


    resultTable.toRetractStream[(String,String)]
      .filter( _ => true)
      .map(t=> t._2)
      .addSink(new RichSinkFunction[(String,String)] {
        lazy val jedis = RedisUtil.getJedisClient

        override def invoke(value: (String, String), context: SinkFunction.Context[_]): Unit = {
          val day = value._1
          val userId = value._2


          jedis.sadd(s"blacklist:$day", userId)
        }

        override def close(): Unit = {
          jedis.close()
        }

      })

  }
}


class FilterProcessFunction extends ProcessFunction[AdsInfo, AdsInfo] {
  lazy val jedis: Jedis = RedisUtil.getJedisClient

  import scala.language.implicitConversions

  override def processElement(value: AdsInfo,
                              ctx: ProcessFunction[AdsInfo, AdsInfo]#Context,
                              out: Collector[AdsInfo]): Unit = {

    val result: Boolean = jedis.sismember(s"blacklist:${value.dayString}", value.userId)

    result match {
      case false => out.collect(value)
      case _ =>
    }

  }

  override def close(): Unit = {
    jedis.close()
  }
}