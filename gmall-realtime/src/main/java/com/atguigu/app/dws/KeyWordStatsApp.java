package com.atguigu.app.dws;

import com.atguigu.app.func.KeyWordUDTF;
import com.atguigu.bean.KeywordStats;
import com.atguigu.common.GmallConstant;
import com.atguigu.utils.ClickHouseUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * MockLog -> Nginx -> Logger -> Kafka(ods_base_log) -> FlinkApp(BaseLogApp)
 * -> Kafka(dwd_page_log) -> KeyWordStatsApp -> ClickHouse
 */
public class KeyWordStatsApp {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        //1.获取执行环境(流、表)
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1.1 设置状态后端
        //env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/dwd_log/ck"));
        //1.2 开启CK
        //env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointTimeout(60000L);

        //1.3 获取表的执行环境
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        //2.读取Kafka主题数据创建动态表  dwd_page_log
        String sourceTopic = "dwd_page_log";
        String groupId = "keyword_stats_app";
        tableEnv.executeSql("CREATE TABLE page_view " +
                "(common MAP<STRING,STRING>, " +
                "page MAP<STRING,STRING>," +
                "ts BIGINT, " +
                "rowtime AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000, 'yyyy-MM-dd HH:mm:ss')) ," +
                "WATERMARK FOR  rowtime  AS  rowtime - INTERVAL '2' SECOND) " +
                "WITH (" + MyKafkaUtil.getKafkaDDL(sourceTopic, groupId)
        );

        //3.过滤数据,只需要搜索数据,搜索的关键词不能为空
        Table filterTable = tableEnv.sqlQuery("" +
                "select " +
                "    page['item'] fullWord, " +
                "    rowtime " +
                "from " +
                "    page_view " +
                "where page['item_type'] = 'keyword' and page['item'] IS NOT NULL");

        //4.使用UDTF函数进行切词处理  函数注册
        tableEnv.createTemporarySystemFunction("ik_analyze", KeyWordUDTF.class);
        Table analyzeWordTable = tableEnv.sqlQuery("SELECT" +
                "    keyword," +
                "    rowtime " +
                " FROM " + filterTable +
                " ,LATERAL TABLE(ik_analyze(fullWord)) AS T(keyword)");

//        tableEnv.createTemporaryView("aa",filterTable);
//        Table analyzeWordTable = tableEnv.sqlQuery("SELECT" +
//                "    word," +
//                "    rowtime " +
//                "FROM aa,LATERAL TABLE(ik_analyze(fullWord)) AS T(word)");

        //5.分组、开窗、聚合
        Table resultTable = tableEnv.sqlQuery("select keyword," +
                "count(*) ct, '"
                + GmallConstant.KEYWORD_SEARCH + "' source ," +
                "DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '2' SECOND),'yyyy-MM-dd HH:mm:ss') stt," +
                "DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '2' SECOND),'yyyy-MM-dd HH:mm:ss') edt," +
                "UNIX_TIMESTAMP()*1000 ts from " + analyzeWordTable
                + " GROUP BY TUMBLE(rowtime, INTERVAL '2' SECOND ),keyword"
        );

        DataStream<KeywordStats> keywordStatsDataStream = tableEnv.toAppendStream(resultTable, KeywordStats.class);
        keywordStatsDataStream.print();

        //6.将数据写入ClickHouse
        keywordStatsDataStream.addSink(ClickHouseUtil.getSink("insert into keyword_stats_200821(keyword,ct,source,stt,edt,ts) values(?,?,?,?,?,?)"));

        //7.执行任务
        env.execute();

    }

}
