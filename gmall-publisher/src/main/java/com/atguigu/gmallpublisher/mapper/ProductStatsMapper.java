package com.atguigu.gmallpublisher.mapper;

import com.atguigu.gmallpublisher.bean.ProductStats;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.math.BigDecimal;
import java.util.List;

/**
 * Desc:  商品主题统计的Mapper接口
 */
public interface ProductStatsMapper {
    //获取某一天商品的交易额
    @Select("select sum(order_amount) from product_stats_200821 where toYYYYMMDD(stt)=#{date}")
    BigDecimal getGMV(int date);

    /*
    获取某一天不同品牌的交易额
    如果mybatis的方法中，有多个参数，每个参数前需要用@Param注解指定参数的名称*/
    @Select("select tm_id,tm_name,sum(order_amount) order_amount from product_stats_200821 " +
            "where toYYYYMMDD(stt)=#{date} group by tm_id,tm_name having order_amount >0 " +
            "order by order_amount desc limit #{limit}")
    List<ProductStats> getProductStatsByTrademark(@Param("date") int date, @Param("limit") int limit);

    /**
     * 获取某一天不同品类的交易额
     */
    @Select("select category3_id,category3_name,sum(order_amount) order_amount from product_stats_200821 " +
        " where toYYYYMMDD(stt)=#{date} group by category3_id,category3_name having order_amount >0 " +
        " order by order_amount desc limit #{limit}")
    List<ProductStats> getProductStatsByCategory3(@Param("date") int date, @Param("limit") int limit);


    /**
     * 获取某一天不同SPU的交易额
     */
    @Select("select spu_id,spu_name,sum(order_amount) order_amount,sum(order_ct) order_ct from product_stats_200821 " +
        "where toYYYYMMDD(stt)=#{date} group by spu_id,spu_name having order_amount >0 order by order_amount desc " +
        "limit #{limit}")
    List<ProductStats> getProductStatsBySPU(@Param("date") int date, @Param("limit") int limit);
}