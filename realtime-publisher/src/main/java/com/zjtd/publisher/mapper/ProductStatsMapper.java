package com.zjtd.publisher.mapper;

import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;

/**
 * Desc:  商品主题统计的Mapper接口
 */
public interface ProductStatsMapper {

    //获取某一天商品的交易额
    @Select("select sum(order_amount) from product_stats_201109 where toYYYYMMDD(stt)=#{date}")
    BigDecimal getGMV(int date);
}
