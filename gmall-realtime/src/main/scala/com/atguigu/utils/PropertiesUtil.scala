package com.atguigu.utils

import java.io.InputStreamReader
import java.util.Properties
//配置文件加载
object PropertiesUtil {

  def load(propertieName:String): Properties ={
    val prop=new Properties()
    prop.load(new InputStreamReader(Thread.currentThread()
      .getContextClassLoader.getResourceAsStream(propertieName) , "UTF-8"))
    prop
  }

//  def main(args: Array[String]): Unit = {
//  val properties = load("config.properties")
//  println(properties.getProperty("kafka.group.id"))
//}
}