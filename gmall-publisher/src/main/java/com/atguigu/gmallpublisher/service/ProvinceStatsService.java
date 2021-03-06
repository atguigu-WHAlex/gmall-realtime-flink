package com.atguigu.gmallpublisher.service;

import com.atguigu.gmallpublisher.bean.ProvinceStats;

import java.util.List;

/**
 * Desc:  按照地区统计的业务接口
 */
public interface ProvinceStatsService {
    List<ProvinceStats> getProvinceStats(int date);
}
