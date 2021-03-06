package com.atguigu.gmallpublisher.service.impl;

import com.atguigu.gmallpublisher.bean.ProductStats;
import com.atguigu.gmallpublisher.mapper.ProductStatsMapper;
import com.atguigu.gmallpublisher.service.ProductStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;

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

    @Override
    public List<ProductStats> getProductStatsByTrademark(int date, int limit) {
        return productStatsMapper.getProductStatsByTrademark(date,limit);
    }

    @Override
    public List<ProductStats> getProductStatsByCategory3(int date, int limit) {
        return productStatsMapper.getProductStatsByCategory3(date,limit);
    }

    @Override
    public List<ProductStats> getProductStatsBySPU(int date, int limit) {
        return productStatsMapper.getProductStatsBySPU(date,limit);
    }
}
