package com.atguigu.utils;

import com.atguigu.bean.TableProcess;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * ORM Object Relation Mapping
 */
public class MySQLUtil {

    public static <T> List<T> queryList(String sql, Class<T> cls, boolean underScoreToCamel) {

        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        try {
            //1.注册驱动
            Class.forName("com.mysql.jdbc.Driver");

            //2.获取连接
            connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/gmall-realtime-200821?characterEncoding=utf-8&useSSL=false",
                    "root",
                    "000000"
            );

            //3.编译SQL,并给占位符赋值
            preparedStatement = connection.prepareStatement(sql);

            //4.执行查询
            resultSet = preparedStatement.executeQuery();

            //6.解析查询结果
            ArrayList<T> list = new ArrayList<>();

            //取出列的元数据
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (resultSet.next()) {

                //封装JavaBean并加入集合
                T t = cls.newInstance();

                for (int i = 1; i <= columnCount; i++) {

                    //获取列名
                    String columnName = metaData.getColumnName(i);
                    if (underScoreToCamel) {
                        columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                    }
                    Object object = resultSet.getObject(i);

                    //给JavaBean对象赋值
                    BeanUtils.setProperty(t, columnName, object);

                }

                list.add(t);
            }

            //返回结果
            return list;

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("查询配置信息失败！！！");
        } finally {
            //7.释放资源
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    public static void main(String[] args) {
        List<TableProcess> tableProcesses = queryList("select * from table_process", TableProcess.class, true);

        System.out.println(tableProcesses);
    }

}
