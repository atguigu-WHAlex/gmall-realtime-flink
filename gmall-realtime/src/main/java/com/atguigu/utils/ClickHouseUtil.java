package com.atguigu.utils;

import com.atguigu.bean.TransientSink;
import com.atguigu.common.GmallConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ClickHouseUtil {

    public static <T> SinkFunction getSink(String sql) {
        return JdbcSink.sink(sql,
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, T obj) throws SQLException {

                        //反射的方式获取所有的属性名
                        Field[] fields = obj.getClass().getDeclaredFields();

                        //定义跳过的属性
                        int offset = 0;

                        for (int i = 0; i < fields.length; i++) {

                            //获取字段名
                            Field field = fields[i];

                            //获取字段上的注解
                            TransientSink transientSink = field.getAnnotation(TransientSink.class);
                            if (transientSink != null) {
                                offset++;
                                continue;
                            }

                            //设置可访问私有属性的值
                            field.setAccessible(true);

                            try {
                                Object o = field.get(obj);

                                //给站位符赋值
                                preparedStatement.setObject(i + 1 - offset, o);
                            } catch (IllegalAccessException e) {
                                e.printStackTrace();
                            }

                        }

                    }
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .build());
    }
}
