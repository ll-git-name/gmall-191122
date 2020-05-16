package com.atguigu.app


import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.constants.GmallConstant
import com.atguigu.handler.DauHanlder
import com.atguigu.utils.MykafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._
object DauApp {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\Users\\luliang\\Desktop\\面试复习\\大数据学习文档资料\\01_尚硅谷大数据技术之hadoop\\2.资料\\01_jar包\\01_win10下编译过的hadoop jar包\\hadoop-2.7.2")
    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DauApp")

    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //3.读取Kafka启动日志主题创建流 从kafka接收的键/值对
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MykafkaUtil.getKafkaStream(
      ssc,
      Set(GmallConstant.GMALL_STARTUP)
    )
    //时间格式样类函数
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")

    //4.将JSON格式的数据转换为样例类对象startUpLogDStream
    val startUpLogDStream: DStream[StartUpLog] = kafkaDStream.map(record => {

      //a.将record中的Value转换为样例类对象
      val startUpLog: StartUpLog = JSON.parseObject(record.value(), classOf[StartUpLog])

      //b.将startUpLog中取出时间
      val ts: Long = startUpLog.ts
      //用时间模板函数，模板化时间
      val dateHourStr: String = sdf.format(new Date(ts))
      //把年月日与时间拆开
      val dateHourArr: Array[String] = dateHourStr.split(" ")

      startUpLog.logDate = dateHourArr(0)
      startUpLog.logHour = dateHourArr(1)

      //c.将完整的启动日志对象返回
      startUpLog
    })
    startUpLogDStream.cache()
    //5.根据Redis中的数据进行跨批次去重
      val filterByRedisLogDStream = DauHanlder.filterByRedis(ssc,startUpLogDStream)
      //因为他用俩次所以用cache缓冲一下
      filterByRedisLogDStream.cache()
    //6.同批次去重
    val filerByMidLogDStream: DStream[StartUpLog] = DauHanlder.filterByMid(filterByRedisLogDStream)
    filerByMidLogDStream.cache()
    //7.将两次去重之后的mid写入Redis,用于以后批次的去重
      DauHanlder.saveMidToRedis(filterByRedisLogDStream);

   //8.把启动任务写入hbase+phoenix
    filterByRedisLogDStream.foreachRDD{rdd=>
      rdd.saveToPhoenix("GMALL191122_DAU",
        Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE",
        "VS", "LOGDATE", "LOGHOUR", "TS") ,new Configuration,
        Some("hadoop105,hadoop106,hadoop107:2181"))
    }
    //启动任务
    ssc.start()
    ssc.awaitTermination()

  }

}