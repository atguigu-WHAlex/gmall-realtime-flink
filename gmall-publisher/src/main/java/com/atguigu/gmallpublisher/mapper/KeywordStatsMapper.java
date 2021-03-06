package com.atguigu.gmallpublisher.mapper;


import com.atguigu.gmallpublisher.bean.KeywordStats;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * Desc: 关键词统计Mapper
 */
public interface KeywordStatsMapper {

    @Select("select keyword," +
        "sum(keyword_stats_200821.ct * " +
        "multiIf(source='SEARCH',10,source='ORDER',3,source='CART',2,source='CLICK',1,0)) ct" +
        " from keyword_stats_200821 where toYYYYMMDD(stt)=#{date} group by keyword " +
        "order by sum(keyword_stats_200821.ct) desc limit #{limit} ")
    public List<KeywordStats> selectKeywordStats(@Param("date") int date, @Param("limit") int limit);
}

