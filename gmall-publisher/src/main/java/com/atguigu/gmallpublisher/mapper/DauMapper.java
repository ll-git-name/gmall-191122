package com.atguigu.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

public interface DauMapper {
    //查询日活总数
    public Integer selectDauTotal(String date);//查询日活分时统计
    public List<Map> selectDauTotalHourMap(String date);

}
