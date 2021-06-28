package com.zjtd.utils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zjtd.common.GmallConfig;
import redis.clients.jedis.Jedis;

import java.util.List;

public class DimUtil {

    public static JSONObject getDimInfo(String tableName, String value) {

        //先查询Redis数据
        Jedis jedis = RedisUtil.getJedis();
        String redisKey = tableName + ":" + value;
        String jsonStr = jedis.get(redisKey);
        if (jsonStr != null) {
            JSONObject jsonObject = JSON.parseObject(jsonStr);
            jedis.expire(redisKey, 24 * 60 * 60);
            jedis.close();
            return jsonObject;
        }

        //当Redis中不存在该数据的时候
        //封装SQL语句
        String querySQL = "select * from " + GmallConfig.HBASE_SCHEMA + "." + tableName + " where id='" + value + "'";

        //查询Phoenix
        List<JSONObject> queryList = PhoenixUtil.queryList(querySQL, JSONObject.class, false);
        JSONObject jsonObject = queryList.get(0);

        //将数据写入Redis
        jedis.set(redisKey, jsonObject.toJSONString());
        jedis.expire(redisKey, 24 * 60 * 60);
        jedis.close();

        //返回结果
        return jsonObject;

    }

    public static void delRedisDim(String tableName, String value) {

        //获取Redis连接
        Jedis jedis = RedisUtil.getJedis();

        //拼接Key
        String redisKey = tableName + ":" + value;

        System.out.println(redisKey);

        //执行删除操作
        jedis.del(redisKey);

        //释放连接
        jedis.close();

    }

    public static void main(String[] args) {

        long start = System.currentTimeMillis();
        System.out.println(getDimInfo("DIM_BASE_TRADEMARK", "15"));
        long end = System.currentTimeMillis();

        System.out.println(getDimInfo("DIM_BASE_TRADEMARK", "15"));
        long end2 = System.currentTimeMillis();

        System.out.println(end - start);
        System.out.println(end2 - end);
    }

}

