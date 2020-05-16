package com.atguigu.handler

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.atguigu.bean.StartUpLog
import com.atguigu.utils.RedisUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DauHanlder {

  private val sdf = new SimpleDateFormat("yyyy-MM-dd")

  /**
   * 对经过Redis去重之后的数据集再次在同批次内根据Mid去重
   *
   * @param filterByRedisLogDStream 经过Redis去重之后的数据集
   */
  def filterByMid(filterByRedisLogDStream: DStream[StartUpLog]): DStream[StartUpLog] = {

    //a.转换数据结构,log=>(date_mid,log)
    val dateMidToLogDStream: DStream[(String, StartUpLog)] = filterByRedisLogDStream.map(log => (s"${log.logDate}_${log.mid}", log))

    //b.按照key进行分组
    val dateMidToLogIterDStream: DStream[(String, Iterable[StartUpLog])] = dateMidToLogDStream.groupByKey()

    //c.对value按照ts排序,取第一条
    val filerByMidLogDStream: DStream[StartUpLog] = dateMidToLogIterDStream.flatMap { case (_, logIter) =>
      logIter.toList.sortWith(_.ts < _.ts).take(1)
    }

    //d.返回值
    filerByMidLogDStream
  }


  /**
   * 根据Redis中的数据进行跨批次去重(过滤),redis中只保留一天的mid
   *
   * @param startUpLogDStream 原始数据集
   */
  def filterByRedis(ssc: StreamingContext, startUpLogDStream: DStream[StartUpLog]): DStream[StartUpLog] = {

    startUpLogDStream.transform(rdd => {
      //方案二、每个批次执行一次 -- Driver 广播变量
      val redis: Jedis = RedisUtil.getJedisClient
      val ts: Long = System.currentTimeMillis()
      val date: String = sdf.format(new Date(ts))
      val mids: util.Set[String] = redis.smembers(s"dau:$date")
      val midsBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(mids)
      redis.close()

      //对数据过滤
      val value: RDD[StartUpLog] = rdd.filter(log => !midsBC.value.contains(log.mid))

      //方案一、根据每个分区做过滤,减少连接数的产生
      //      val value: RDD[StartUpLog] = rdd.mapPartitions(iter => {
      //        //a.获取连接
      //        val jedisClient: Jedis = RedisUtil.getJedisClient
      //        //b.对每条数据进行过滤,filiter是把true的结果留下来
      //        val logs: Iterator[StartUpLog] = iter.filter { log =>
      //          val redisKey = s"dau:${log.logDate}"
      //          !jedisClient.sismember(redisKey, log.mid)
      //        }
      //        //c.释放连接
      //        jedisClient.close()
      //        //d.根据Redis中过滤的结果集
      //        logs
      //      })
      //返回值
      value
    })
  }


  /**
   * 将经过两次过滤之后的Mid写入Redis,供当日以后的数据去重使用
   *
   * @param filerByMidLogDStream 经过两次过滤之后的数据集
   */
  def saveMidToRedis(filerByMidLogDStream: DStream[StartUpLog]): Unit = {

    filerByMidLogDStream.foreachRDD(rdd => {

      rdd.foreachPartition(iter => {
        //a.获取连接
        val redisClient: Jedis = RedisUtil.getJedisClient
        //b.对每条数据进行写库操作
        iter.foreach(log => {
          val redisKey = s"dau:${log.logDate}"
          redisClient.sadd(redisKey, log.mid)
        })
        //c.释放连接
        redisClient.close()
      })
    })
  }

}
