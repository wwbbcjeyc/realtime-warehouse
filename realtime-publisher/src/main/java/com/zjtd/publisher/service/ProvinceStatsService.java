package com.zjtd.publisher.service;

import com.zjtd.publisher.bean.ProvinceStats;

import java.util.List;

/**
 * Desc:  按照地区统计的业务接口
 */
public interface ProvinceStatsService {
    List<ProvinceStats> getProvinceStats(int date);
}

