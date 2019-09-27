package com.ggc.spark_streaming.util

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object MyKafkaUtil {
  // kafka消费者配置
  val kafkaParams: Map[String, Object] = Map[String, Object](
    "bootstrap.servers" -> "dw1:9092,dw2:9092,dw3:9092", //用于初始化链接到集群的地址
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "SparkStreamingProjectGroup", //用于标识这个消费者属于哪个消费组
    //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
    //可以使用这个配置，latest自动重置偏移量为最新的偏移量
    "auto.offset.reset" -> "earliest",//earliest ， latest
    //如果是true，则这个消费者的偏移量会在后台自动提交,但是kafka宕机容易丢失数据
    //如果是false，会需要手动维护kafka偏移量. 本次我们仍然自动维护偏移量
    "enable.auto.commit" -> (true: java.lang.Boolean)
  )

  /*
    创建DStream，返回接收到的输入数据
    LocationStrategies：根据给定的主题和集群地址创建consumer
    LocationStrategies.PreferConsistent：持续的在所有Executor之间分配分区
    ConsumerStrategies：选择如何在Driver和Executor上创建和配置Kafka Consumer
    ConsumerStrategies.Subscribe：订阅一系列主题
   */
  def getKafkaStream(ssc: StreamingContext, topic: String): InputDStream[ConsumerRecord[String, String]] = {
    KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,// 标配. 只要 kafka 和 spark 没有部署在一台设备就应该是这个参数
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParams)
    )
  }

}
