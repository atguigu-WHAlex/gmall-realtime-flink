package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.bean.TableProcess;
import com.atguigu.utils.MyKafkaUtil;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Properties;

public class DbBaseApp3 {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //1.1 设置状态后端
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/dwd_log/ck"));
//        //1.2 开启CK
//        env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60000L);

        //2.读取Kafka数据
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource("ods_base_db_m", "ods_db_group");
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

        //3.将每行数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);

        //4.过滤
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                //获取data字段
                String data = value.getString("data");
                return data != null && data.length() > 0;
            }
        });

        //打印测试
//        filterDS.print();

        //2.创建MySQL CDC Source
        Properties properties = new Properties();
        properties.setProperty("scan.startup.mode", "initial");
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("000000")
                .databaseList("gmall-realtime-200821")
                .tableList("gmall-realtime-200821.table_process")
                .deserializer(new MySchema())
                .debeziumProperties(properties)
                .build();
        //3.读取MySQL数据
        DataStreamSource<String> tableProcessDS = env.addSource(sourceFunction);

        //将配置信息流作为广播流
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("table-process-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = tableProcessDS.broadcast(mapStateDescriptor);

        //5.分流,ProcessFunction
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>(TableProcess.SINK_TYPE_HBASE) {
        };

        //6.将主流和广播流进行链接
        BroadcastConnectedStream<JSONObject, String> connectedStream = filterDS.connect(broadcastStream);

        //7.根据广播过来的数据信息处理每一条数据
        SingleOutputStreamOperator<JSONObject> result = connectedStream.process(new BroadcastProcessFunction<JSONObject, String, JSONObject>() {

            @Override
            public void processElement(JSONObject jsonObject, ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {

                //获取状态
                ReadOnlyBroadcastState<String, TableProcess> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);

                //获取表名和操作类型
                String table = jsonObject.getString("table");
                String type = jsonObject.getString("type");
                String key = table + ":" + type;

                //取出对应的配置信息数据
                TableProcess tableProcess = broadcastState.get(key);

                if (tableProcess != null) {

                    jsonObject.put("sink_table", tableProcess.getSinkTable());
                    String sinkType = tableProcess.getSinkType();

                    if ("kafka".equals(sinkType)) {
                        collector.collect(jsonObject);
                    } else if ("hbase".equals(sinkType)) {
                        readOnlyContext.output(hbaseTag, jsonObject);
                    }

                } else {
                    System.out.println("没有找到对应Key：" + key + "的配置信息！");
                }

            }

            @Override
            public void processBroadcastElement(String jsonStr, Context context, Collector<JSONObject> collector) throws Exception {

                //获取状态
                BroadcastState<String, TableProcess> broadcastState = context.getBroadcastState(mapStateDescriptor);

                //将配置信息流中的数据转换为JSON对象{"database":"","table":"","type","","data":{"":""}}
                JSONObject jsonObject = JSON.parseObject(jsonStr);

                //取出数据中的表名以及操作类型封装key
                JSONObject data = jsonObject.getJSONObject("data");

                String table = data.getString("source_table");
                String type = data.getString("operate_type");
                String key = table + ":" + type;

                //取出Value数据封装为TableProcess对象
                TableProcess tableProcess = JSON.parseObject(data.toString(), TableProcess.class);

                System.out.println("Key:" + key + "," + tableProcess);

                //广播出去
                broadcastState.put(key, tableProcess);
            }
        });


        //打印数据
        result.print("Kafka>>>>>>>>>");
        result.getSideOutput(hbaseTag).print("HBase>>>>>>>>>");

        //执行任务
        env.execute();

    }


    public static class MySchema implements DebeziumDeserializationSchema<String> {

        //反序列化方法
        @Override
        public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {

            //库名&表名
            String topic = sourceRecord.topic();
            String[] split = topic.split("\\.");
            String db = split[1];
            String table = split[2];

            //获取数据
            Struct value = (Struct) sourceRecord.value();
            Struct after = value.getStruct("after");
            JSONObject data = new JSONObject();
            if (after != null) {
                Schema schema = after.schema();
                for (Field field : schema.fields()) {
                    data.put(field.name(), after.get(field.name()));
                }
            }

            //获取操作类型
            Envelope.Operation operation = Envelope.operationFor(sourceRecord);

            //创建JSON用于存放最终的结果
            JSONObject result = new JSONObject();
            result.put("database", db);
            result.put("table", table);
            result.put("type", operation.toString().toLowerCase());
            result.put("data", data);

            collector.collect(result.toJSONString());
        }


        //定义数据类型
        @Override
        public TypeInformation<String> getProducedType() {
            return TypeInformation.of(String.class);
        }
    }
}
