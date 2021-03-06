package com.atguigu;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

public class Test {
    public static void main(String[] args) throws Exception {

        //1.获取执行环境,设置并行度,开启CK,设置状态后端(HDFS)
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //为Kafka主题的分区数
        env.setParallelism(1);
        //1.1 设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/dwd_log/ck"));
        //1.2 开启CK
        env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);

        env.setRestartStrategy(RestartStrategies.noRestart());

        //修改用户名
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //2.读取Kafka ods_base_log 主题数据
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource("test", "test1");
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

        //3.将每行数据转换为JsonObject
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                System.out.println(value);
                System.out.println(5 / 0);
                return JSON.parseObject(value);
            }
        });

        //4.打印数据
        kafkaDS.print("Kafka>>>>>>>");
        jsonObjDS.print("JSON>>>>>>");

        //5.执行任务
        env.execute();
    }
}
