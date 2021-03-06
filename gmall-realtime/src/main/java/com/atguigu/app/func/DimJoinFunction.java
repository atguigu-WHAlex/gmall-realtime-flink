package com.atguigu.app.func;

import com.alibaba.fastjson.JSONObject;

public interface DimJoinFunction<T> {

    //获取数据中的所要关联维度的主键
    String getKey(T input);

    //关联事实数据和维度数据
    void join(T input, JSONObject dimInfo) throws Exception;

}
