package com.atguigu.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;

/**
 * 数据流：Mock(web/app) -> nginx -> SpringBoot -> Kafka -> FlinkApp(LogBaseApp) -> Kafka
 * FlinkApp(DauApp) -> Kafka
 * 服务：  Nginx  Logger  ZK  Kafka  LogBaseApp  DauApp  消费者(dwm_unique_visit)  MockLog
 */
public class DauApp {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1.1 设置状态后端
        //env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/dwd_log/ck"));
        //1.2 开启CK
        //env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointTimeout(60000L);

        //2.读取Kafka dwd_page_log主题数据创建流
        String groupId = "unique_visit_app";
        String sourceTopic = "dwd_page_log";
        String sinkTopic = "dwm_unique_visit";
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(sourceTopic, groupId);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

        //3.将每行数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    context.output(new OutputTag<String>("dirty") {
                    }, s);
                }
            }
        });

        //4.按照mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        //5.过滤掉不是今天第一次访问的数据
        SingleOutputStreamOperator<JSONObject> filterDS = keyedStream.filter(new UvRichFilterFunction());

        //6.写入DWM层Kafka主题中
        filterDS.print(">>>>>>>>>");
        filterDS.map(JSON::toString).addSink(MyKafkaUtil.getKafkaSink(sinkTopic));

        //7.启动任务
        env.execute();

    }

    public static class UvRichFilterFunction extends RichFilterFunction<JSONObject> {

        //声明状态
        private ValueState<String> firstVisitState;
        private SimpleDateFormat simpleDateFormat;

        @Override
        public void open(Configuration parameters) throws Exception {
            simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            ValueStateDescriptor<String> stringValueStateDescriptor = new ValueStateDescriptor<>("visit-state", String.class);

            //创建状态TTL配置项
            StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.days(1))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .build();
            stringValueStateDescriptor.enableTimeToLive(stateTtlConfig);
            firstVisitState = getRuntimeContext().getState(stringValueStateDescriptor);
        }

        @Override
        public boolean filter(JSONObject value) throws Exception {

            //取出上一次访问页面
            String lastPageId = value.getJSONObject("page").getString("last_page_id");

            //判断是否存在上一个页面
            if (lastPageId == null || lastPageId.length() <= 0) {

                //取出状态数据
                String firstVisitDate = firstVisitState.value();

                //取出数据时间
                Long ts = value.getLong("ts");
                String curDate = simpleDateFormat.format(ts);

                if (firstVisitDate == null || !firstVisitDate.equals(curDate)) {
                    firstVisitState.update(curDate);
                    return true;
                } else {
                    return false;
                }
            } else {
                return false;
            }

        }
    }

}
