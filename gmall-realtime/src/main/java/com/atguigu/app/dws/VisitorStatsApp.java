package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.VisitorStats;
import com.atguigu.utils.ClickHouseUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;

/**
 * Mock -> Nginx -> Logger ->
 * Kafka(ods_base_log) ->
 * FlinkApp(LogBaseApp) ->
 * Kafka(dwd_page_log dwd_start_log dwd_display_log) ->
 * FlinkApp(UvApp UserJumpApp) ->
 * Kafka(dwm_unique_visit dwm_user_jump_detail) ->
 * VisitorStatsApp -> ClickHouse
 */
public class VisitorStatsApp {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //1.1 设置状态后端
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/dwd_log/ck"));
        //1.2 开启CK
//        env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60000L);

        //2.读取Kafka主题的数据
        // dwd_page_log(pv,访问时长, | 进入页面数)
        // dwm_unique_visit(uv)
        // dwm_user_jump_detail (跳出数)
        String groupId = "visitor_stats_app";
        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";
        DataStreamSource<String> pageLogDS = env.addSource(MyKafkaUtil.getKafkaSource(pageViewSourceTopic, groupId));
        DataStreamSource<String> uvDS = env.addSource(MyKafkaUtil.getKafkaSource(uniqueVisitSourceTopic, groupId));
        DataStreamSource<String> userJumpDS = env.addSource(MyKafkaUtil.getKafkaSource(userJumpDetailSourceTopic, groupId));

        //测试打印
//        pageLogDS.print("Page>>>>>>>>>>>>>>");
//        uvDS.print("UV>>>>>>>>>>>>>>>>>>>>>");
//        userJumpDS.print("UserJump>>>>>>>>>");

        //3.格式化流数据,使其字段统一  (JavaBean)
        //3.1 将页面数据流格式化为VisitorStats,  PV  during—time
        SingleOutputStreamOperator<VisitorStats> pvAndDtDS = pageLogDS.map(jsonStr -> {

            //将数据转换为JSON对象
            JSONObject jsonObject = JSON.parseObject(jsonStr);
            JSONObject commonObj = jsonObject.getJSONObject("common");

            return new VisitorStats("", "",
                    commonObj.getString("vc"),
                    commonObj.getString("ch"),
                    commonObj.getString("ar"),
                    commonObj.getString("is_new"),
                    0L,
                    1L,
                    0L,
                    0L,
                    jsonObject.getJSONObject("page").getLong("during_time"),
                    jsonObject.getLong("ts"));
        });

        //3.2 将页面数据流先过滤后格式化为VisitorStats  sv_ct
        SingleOutputStreamOperator<VisitorStats> svCountDS = pageLogDS.process(new ProcessFunction<String, VisitorStats>() {
            @Override
            public void processElement(String value, Context ctx, Collector<VisitorStats> out) throws Exception {

                //将数据转换为JSON对象
                JSONObject jsonObject = JSON.parseObject(value);

                //获取上一条页面数据
                String lastPage = jsonObject.getJSONObject("page").getString("last_page_id");

                if (lastPage == null || lastPage.length() <= 0) {
                    JSONObject commonObj = jsonObject.getJSONObject("common");
                    out.collect(new VisitorStats("", "",
                            commonObj.getString("vc"),
                            commonObj.getString("ch"),
                            commonObj.getString("ar"),
                            commonObj.getString("is_new"),
                            0L,
                            0L,
                            1L,
                            0L,
                            0L,
                            jsonObject.getLong("ts")));
                }
            }
        });

        //3.3 将uvDS格式化为VisitorStats uv
        SingleOutputStreamOperator<VisitorStats> uvCountDS = uvDS.map(jsonStr -> {

            //将数据转换为JSON对象
            JSONObject jsonObject = JSON.parseObject(jsonStr);
            JSONObject commonObj = jsonObject.getJSONObject("common");

            return new VisitorStats("", "",
                    commonObj.getString("vc"),
                    commonObj.getString("ch"),
                    commonObj.getString("ar"),
                    commonObj.getString("is_new"),
                    1L,
                    0L,
                    0L,
                    0L,
                    0L,
                    jsonObject.getLong("ts"));
        });

        //3.4 将userJumpDS格式化为VisitorStats  uj_ct
        SingleOutputStreamOperator<VisitorStats> ujCountDS = userJumpDS.map(jsonStr -> {

            //将数据转换为JSON对象
            JSONObject jsonObject = JSON.parseObject(jsonStr);
            JSONObject commonObj = jsonObject.getJSONObject("common");

            return new VisitorStats("", "",
                    commonObj.getString("vc"),
                    commonObj.getString("ch"),
                    commonObj.getString("ar"),
                    commonObj.getString("is_new"),
                    0L,
                    0L,
                    0L,
                    1L,
                    0L,
                    jsonObject.getLong("ts"));
        });

        //4.将多个流的数据进行union
        DataStream<VisitorStats> unionDS = pvAndDtDS.union(svCountDS, uvCountDS, ujCountDS);

        //5.分组,聚合计算
        SingleOutputStreamOperator<VisitorStats> visitorStatsSingleOutputStreamOperator = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(10L)).withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
            @Override
            public long extractTimestamp(VisitorStats element, long recordTimestamp) {
                return element.getTs();
            }
        }));
        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> keyedStream = visitorStatsSingleOutputStreamOperator.keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(VisitorStats value) throws Exception {
                return Tuple4.of(value.getVc(), value.getCh(), value.getAr(), value.getIs_new());
            }
        });

        //开窗
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowedStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(2)));

        //聚合操作
        SingleOutputStreamOperator<VisitorStats> result = windowedStream.reduce(new ReduceFunction<VisitorStats>() {
            @Override
            public VisitorStats reduce(VisitorStats value1, VisitorStats value2) throws Exception {
                return new VisitorStats("", "",
                        value1.getVc(),
                        value1.getCh(),
                        value1.getAr(),
                        value1.getIs_new(),
                        value1.getUv_ct() + value2.getUv_ct(),
                        value1.getPv_ct() + value2.getPv_ct(),
                        value1.getSv_ct() + value2.getSv_ct(),
                        value1.getUj_ct() + value2.getUj_ct(),
                        value1.getDur_sum() + value2.getDur_sum(),
                        System.currentTimeMillis());
            }
        }, new WindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<VisitorStats> input, Collector<VisitorStats> out) throws Exception {

                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                //取出数据
                VisitorStats visitorStats = input.iterator().next();

                //取出窗口的开始及结束时间
                long start = window.getStart();
                long end = window.getEnd();
                String stt = sdf.format(start);
                String edt = sdf.format(end);

                //设置时间数据
                visitorStats.setStt(stt);
                visitorStats.setEdt(edt);

                //将数据写出
                out.collect(visitorStats);
            }
        });

//        result.print(">>>>>>>>>>>");

        //不开窗统计测试
//        keyedStream.reduce(new ReduceFunction<VisitorStats>() {
//            @Override
//            public VisitorStats reduce(VisitorStats value1, VisitorStats value2) throws Exception {
//                return new VisitorStats("", "",
//                        value1.getVc(),
//                        value1.getCh(),
//                        value1.getAr(),
//                        value1.getIs_new(),
//                        value1.getUv_ct() + value2.getUv_ct(),
//                        value1.getPv_ct() + value2.getPv_ct(),
//                        value1.getSv_ct() + value2.getSv_ct(),
//                        value1.getUj_ct() + value2.getUj_ct(),
//                        value1.getDur_sum() + value2.getDur_sum(),
//                        System.currentTimeMillis());
//            }
//        }).print(">>>>>>>>>>>>>>");

        //6.将聚合之后的数据写入ClickHouse
        result.print(">>>>>>>>>>");
        result.addSink(ClickHouseUtil.getSink("insert into visitor_stats_200821 values(?,?,?,?,?,?,?,?,?,?,?,?)"));

        //7.执行任务
        env.execute();

    }

}
