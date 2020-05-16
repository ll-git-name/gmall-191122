package com.atguigu.gmallpublisher;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages ="com.atguigu.gmallpublisher.mapper.DauMapper")
public class GmallPublisherApplication {

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\Users\\luliang\\Desktop\\面试复习\\大数据学习文档资料\\01_尚硅谷大数据技术之hadoop\\2.资料\\01_jar包\\01_win10下编译过的hadoop jar包\\hadoop-2.7.2");
        SpringApplication.run(GmallPublisherApplication.class, args);
    }

}
