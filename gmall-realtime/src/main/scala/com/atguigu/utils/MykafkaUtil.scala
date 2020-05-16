package com.atguigu.utils

import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
//对接kafka
object MykafkaUtil {

  //加载配置文件内容
  private val properties: Properties = PropertiesUtil.load("config.properties")

  //构建Kafka参数
  private val kafkaParams: Map[String, Object] = Map[String, Object](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> properties.getProperty("kafka.broker.list"),
    ConsumerConfig.GROUP_ID_CONFIG -> properties.getProperty("kafka.group.id"),
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> properties.getProperty("kafka.value.deserializer"))

  //与kafka对接，读取kafka某些主题创建流模板
  def getKafkaStream(offest:Map[TopicPartition,Long], ssc: StreamingContext, topics: Set[String]): InputDStream[ConsumerRecord[String, String]] = {

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      //位置策略
      LocationStrategies.PreferConsistent,
      //消费策略
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)),
      offest
    kafkaDStream
  }

}
