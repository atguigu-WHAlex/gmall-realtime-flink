package com.atguigu.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import com.atguigu.utils.DimUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

public class DimSink extends RichSinkFunction<JSONObject> {

    private Connection connection = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化Phoenix连接
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    //将数据写入Phoenix：upsert into t(id,name,sex) values(...,...,...)
    @Override
    public void invoke(JSONObject jsonObject, Context context) throws Exception {

        PreparedStatement preparedStatement = null;
        try {
            //获取数据中的Key以及Value
            JSONObject data = jsonObject.getJSONObject("data");
            Set<String> keys = data.keySet();
            Collection<Object> values = data.values();

            //获取表名
            String tableName = jsonObject.getString("sink_table");

            //创建插入数据的SQL
            String upsertSql = genUpsertSql(tableName, keys, values);
            System.out.println(upsertSql);
            //编译SQL
            preparedStatement = connection.prepareStatement(upsertSql);
            //执行
            preparedStatement.executeUpdate();
            //提交
            connection.commit();

            //判断如果是更新操作,则删除Redis中的数据保证数据的一致性
            String type = jsonObject.getString("type");
            if ("update".equals(type)) {
                DimUtil.deleteCached(tableName, data.getString("id"));
            }

        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("插入Phoenix数据失败！");
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }
    }

    //创建插入数据的SQL upsert into t(id,name,sex) values('...','...','...')
    private String genUpsertSql(String tableName, Set<String> keys, Collection<Object> values) {
        return "upsert into " + GmallConfig.HBASE_SCHEMA + "." +
                tableName + "(" + StringUtils.join(keys, ",") + ")" +
                " values('" + StringUtils.join(values, "','") + "')";
    }
}
