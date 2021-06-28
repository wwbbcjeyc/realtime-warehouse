package com.zjtd.app.func;

import com.alibaba.fastjson.JSONObject;

public interface JoinDimFunction<T> {

    //根据数据获取对应的维度ID
    String getId(T input);

    //将维度信息补充到事实数据上
    void join(T input, JSONObject dimInfo);
}
