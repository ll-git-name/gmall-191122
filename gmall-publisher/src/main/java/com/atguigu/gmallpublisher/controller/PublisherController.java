package com.atguigu.gmallpublisher.controller;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmallpublisher.service.impl.PublisherService;
import com.google.gson.JsonObject;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
public class PublisherController {

   @Autowired
    private PublisherService publisherService;
    @RequestMapping("realtime-total")
    public String getRealTimeTotal(@RequestParam("date") String date){
        //1，创建集合，用于存放结果数据
            ArrayList<Map> result = new ArrayList<>();

            //2.创建日货需求的Map
            HashMap<String, Object> dauMap = new HashMap<>();
                dauMap.put("id","dau");
                dauMap.put("name","新增日活");
                dauMap.put("value",publisherService.getDauTotal(date));
            //3.创建新增需求的map
            HashMap<String, Object> newMidMap = new HashMap<>();
                newMidMap.put("id","new_mid");
                newMidMap.put("name","新增设置");
                newMidMap.put("value","233");
           //4.将俩个Map放集合
            result.add(dauMap);
            result.add(newMidMap);
            //5.将集合结果装为json字符串返回
            return JSONObject.toJSONString(result);
    }
}
