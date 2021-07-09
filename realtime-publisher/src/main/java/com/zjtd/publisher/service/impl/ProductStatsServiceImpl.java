package com.zjtd.publisher.service.impl;

import com.zjtd.publisher.mapper.ProductStatsMapper;
import com.zjtd.publisher.service.ProductStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

/**
 * Desc: 商品统计Service接口实现类
 */
@Service //标识是Spring的Sevice层组件，将对象的创建交给Spring的IOC管理
public class ProductStatsServiceImpl implements ProductStatsService {

    //自动注入   在容器中，寻找ProductStatsMapper类型的对象，赋值给当前属性
    @Autowired
    ProductStatsMapper productStatsMapper ;

    @Override
    public BigDecimal getGMV(int date) {
        return productStatsMapper.getGMV(date);
    }
}
