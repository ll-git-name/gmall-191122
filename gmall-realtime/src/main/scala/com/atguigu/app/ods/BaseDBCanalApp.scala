package com.atguigu.app.ods

import com.atguigu.utils.MykafkaUtil
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object BaseDBCanalApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("BaseDBCanalApp").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf,Seconds(3))
    val topic="ODS_DB_GMALL1122_C"
    val groupId="base_db_canal_group"
    val offset:Map[TopicPartition,Long]=OffsetManager



    MykafkaUtil.getKafkaStream()
  }
}
