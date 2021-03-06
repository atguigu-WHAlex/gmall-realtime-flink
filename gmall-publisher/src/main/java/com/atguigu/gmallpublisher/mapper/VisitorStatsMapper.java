package com.atguigu.gmallpublisher.mapper;

import com.atguigu.gmallpublisher.bean.VisitorStats;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * Desc: 访客统计接口
 */
public interface VisitorStatsMapper {
    @Select("select is_new,sum(uv_ct) uv_ct,sum(pv_ct) pv_ct,sum(sv_ct) sv_ct, sum(uj_ct) uj_ct,sum(dur_sum) dur_sum " +
        " from visitor_stats_200821 where toYYYYMMDD(stt)=#{date} group by is_new")
    List<VisitorStats> selectVisitorStatsByNewFlag(int date);

    /**
     *  ClickHouse的函数
     *      -if(条件表达式，满足返回的值，不满足返回的值)
     *      -toHour
     *         将日期转换为小时 （0~23）
     */
    @Select("select sum(if(is_new='1', visitor_stats_200821.uv_ct,0)) new_uv,toHour(stt) hr," +
        " sum(visitor_stats_200821.uv_ct) uv_ct, sum(pv_ct) pv_ct, sum(uj_ct) uj_ct " +
        " from visitor_stats_200821 where toYYYYMMDD(stt)=#{date} group by toHour(stt) order by toHour(stt)")
    List<VisitorStats> selectVisitorStatsByHr(int date);
}
