package com.zjtd.publisher.service;

import java.math.BigDecimal;

/**
 * Desc: 商品统计service接口
 */
public interface ProductStatsService {

    //获取某一天交易总额
    BigDecimal getGMV(int date);
}
