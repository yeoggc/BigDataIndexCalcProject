package com.ggc.flink_realtime

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.ggc.flink_realtime.app.BlackListApp
import com.ggc.flink_realtime.bean.AdsInfo
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.table.api.TableEnvironment

object RealtimeApp {
  def main(args: Array[String]): Unit = {

    val dayStringFormatter: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val hmStringFormatter: SimpleDateFormat = new SimpleDateFormat("HH:mm")

    // set up execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)


    env.setParallelism(1)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "dw1:9092,dw2:9092,dw3:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "earliest") // earliest

    val inputDS: DataStream[AdsInfo] =
      env
        .addSource(new FlinkKafkaConsumer[String]("ads_log", new SimpleStringSchema(), properties))
        .map(v => {
          val split: Array[String] = v.split(",")
          val date: Date = new Date(split(0).toLong)
          AdsInfo(split(0).toLong,
            new Timestamp(split(0).toLong),
            dayStringFormatter.format(date), hmStringFormatter.format(date),
            split(1), split(2), split(3), split(4))
        })
        .assignAscendingTimestamps(_.ts)

        inputDS.print()

//    val ds1 =
//      inputDS
//        .map(t => (t, 1))
//        .keyBy(t => (t._1.dayString, t._1.userId, t._1.adsId))
//        .sum(1)
//
//
//    ds1.print()

    BlackListApp.statBlackList(tEnv, inputDS)


    env.execute(getClass.getSimpleName)

  }

}
