package com.atguigu.gmalllogger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.constants.GmallConstant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;


//用来接收请求
//@Controller    //代表他是controler与前端页面关联（服务员）
//默认controller返回的是一个页面，需要加ResponseBody返回字符串
@RestController  //相当于 @Controller +@ResponseBody
@Slf4j   //自动添加了一个对象
public class LoggerController {
    @Autowired   //自动注入，自己去new一个对象
    //会误报，在File | Settings | Editor | Inspections中设置
    //kafka的生产者
    private KafkaTemplate<String,String> kafkaTemplate;


    @RequestMapping("test1")    //在web页面访问的方法
    @ResponseBody
    public String getTest1(){
        System.out.println("1111");
        return "success";
    }

    @RequestMapping("test2")    //在web页面访问的方法
    // @ResponseBody  返回字符串
    //name是跟网页上绑定的一样
    //http://localhost:8080/test2?name=zhangsan  web访问
    //多个参数@RequestParam("name") String nm，@RequestParam("age") String age web用&分开
    public String getTest2(@RequestParam("name") String nm,@RequestParam("age") String age){
        System.out.println(nm+""+age);
        return "success";
    }
    @RequestMapping("log")
    public String getLogger(@RequestParam("logString") String logString){
        //1.将数据使用日志框架写入文件同时打印到控制台
        //        log.info(logString);
        //mocker不造时间，因为那是用户的时间，可以修改
        JSONObject jsonObject = JSON.parseObject(logString);
        jsonObject.put("ts",System.currentTimeMillis());
        String addTsLogStrring = jsonObject.toString();
        log.info(addTsLogStrring);
        //2.将数据推送到kafka,根据数据类型发往不同的主题
        if("startup".equals(jsonObject.getString("type"))){
            //发送至启动日志主题
            kafkaTemplate.send(GmallConstant.GMALL_STARTUP,addTsLogStrring);
        }else{
            kafkaTemplate.send(GmallConstant.GMALL_EVENT,addTsLogStrring);

        }

        return "success";
        }

}
