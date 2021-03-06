package com.atguigu.gmallpublisher.service;

import com.atguigu.gmallpublisher.bean.KeywordStats;

import java.util.List;

/**
 * Desc: 关键词统计接口
 */
public interface KeywordStatsService {
    public List<KeywordStats> getKeywordStats(int date, int limit);
}

