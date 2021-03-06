package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;

public class LogBaseApp {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境,设置并行度,开启CK,设置状态后端(HDFS)
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //为Kafka主题的分区数
        env.setParallelism(1);
        //1.1 设置状态后端
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/dwd_log/ck"));
//        //1.2 开启CK
//        env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60000L);

        //修改用户名
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //2.读取Kafka ods_base_log 主题数据
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource("ods_base_log", "dwd_log");
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

        //3.将每行数据转换为JsonObject
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(data -> JSONObject.parseObject(data));

        //4.按照Mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(data -> data.getJSONObject("common").getString("mid"));

        //5.使用状态做新老用户校验
        SingleOutputStreamOperator<JSONObject> jsonWithNewFlagDS = keyedStream.map(new NewMidRichMapFunc());
        //打印测试
//        jsonWithNewFlagDS.print();

        //6.分流,使用ProcessFunction将ODS数据拆分成启动、曝光以及页面数据
        SingleOutputStreamOperator<String> pageDS = jsonWithNewFlagDS.process(new SplitProcessFunc());

        //7.将三个流的数据写入对应的Kafka主题
        DataStream<String> startDS = pageDS.getSideOutput(new OutputTag<String>("start") {
        });
        DataStream<String> displayDS = pageDS.getSideOutput(new OutputTag<String>("display") {
        });
        //打印测试
        pageDS.print("Page>>>>>>>>>");
        startDS.print("Start>>>>>>>>>>");
        displayDS.print("Display>>>>>>>>>>");
        pageDS.addSink(MyKafkaUtil.getKafkaSink("dwd_page_log"));
        startDS.addSink(MyKafkaUtil.getKafkaSink("dwd_start_log"));
        displayDS.addSink(MyKafkaUtil.getKafkaSink("dwd_display_log"));

        //8.执行任务
        env.execute();
    }

    public static class NewMidRichMapFunc extends RichMapFunction<JSONObject, JSONObject> {

        //声明状态用于表示当前Mid是否已经访问过
        private ValueState<String> firstVisitDateState;
        private SimpleDateFormat simpleDateFormat;

        @Override
        public void open(Configuration parameters) throws Exception {
            firstVisitDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("new-mid", String.class));
            simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        }

        @Override
        public JSONObject map(JSONObject value) throws Exception {

            //取出新用户标记
            String isNew = value.getJSONObject("common").getString("is_new");

            //如果当前前端传输数据表示为新用户,则进行校验
            if ("1".equals(isNew)) {

                //取出状态数据并取出当前访问时间
                String firstDate = firstVisitDateState.value();
                Long ts = value.getLong("ts");

                //判断状态数据是否为Null
                if (firstDate != null) {
                    //修复
                    value.getJSONObject("common").put("is_new", "0");
                } else {
                    //更新状态
                    firstVisitDateState.update(simpleDateFormat.format(ts));
                }
            }

            //返回数据
            return value;
        }
    }

    public static class SplitProcessFunc extends ProcessFunction<JSONObject, String> {

        @Override
        public void processElement(JSONObject jsonObject, Context context, Collector<String> collector) throws Exception {

            //提取"start"字段
            String startStr = jsonObject.getString("start");

            //判断是否为启动数据
            if (startStr != null && startStr.length() > 0) {
                //将启动日志输出到侧输出流
                context.output(new OutputTag<String>("start") {
                }, jsonObject.toString());
            } else {

                //为页面数据,将数据输出到主流
                collector.collect(jsonObject.toString());

                //不是启动数据,继续判断是否是曝光数据
                JSONArray displays = jsonObject.getJSONArray("displays");
                if (displays != null && displays.size() > 0) {
                    //为曝光数据,遍历写入侧输出流
                    for (int i = 0; i < displays.size(); i++) {
                        //取出单条曝光数据
                        JSONObject displayJson = displays.getJSONObject(i);
                        //添加页面ID
                        displayJson.put("page_id",
                                jsonObject.getJSONObject("page").getString("page_id"));
                        //输出到侧输出流
                        context.output(new OutputTag<String>("display") {
                        }, displayJson.toString());
                    }
                }

            }
        }
    }
}
