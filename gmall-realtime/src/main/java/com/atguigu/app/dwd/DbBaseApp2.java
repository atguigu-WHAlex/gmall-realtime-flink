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
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Properties;

public class DbBaseApp2 {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境+
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

        filterDS.print("filter");

        Properties properties = new Properties();
        properties.setProperty("debezium.snapshot.mode", "initial");
        DebeziumSourceFunction<String> mysqlSource = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("000000")
                .databaseList("gmall-realtime-200821")
                .tableList("gmall-realtime-200821.table_process")         //可选配置项,如果不指定该参数,则会读取上一个配置下的所有表的数据,注意：指定的时候需要使用"db.table"的方式
                .debeziumProperties(properties)
                .deserializer(new DebeziumDeserializationSchema<String>() {  //自定义数据解析器
                    @Override
                    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {

                        //获取主题信息,包含着数据库和表名  mysql_binlog_source.gmall-flink-200821.z_user_info
                        String topic = sourceRecord.topic();
                        String[] arr = topic.split("\\.");
                        String db = arr[1];
                        String tableName = arr[2];

                        //获取操作类型 READ DELETE UPDATE CREATE
                        Envelope.Operation operation = Envelope.operationFor(sourceRecord);

                        //获取值信息并转换为Struct类型
                        Struct value = (Struct) sourceRecord.value();

                        //获取变化后的数据
                        Struct after = value.getStruct("after");

                        //创建JSON对象用于存储数据信息
                        JSONObject data = new JSONObject();
                        for (Field field : after.schema().fields()) {
                            Object o = after.get(field);
                            data.put(field.name(), o);
                        }

                        //创建JSON对象用于封装最终返回值数据信息
                        JSONObject result = new JSONObject();
                        result.put("operation", operation.toString().toLowerCase());
                        result.put("data", data);
                        result.put("database", db);
                        result.put("table", tableName);

                        //发送数据至下游
                        collector.collect(result.toJSONString());
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return TypeInformation.of(String.class);
                    }
                })
                .build();

        //4.使用CDC Source从MySQL读取数据
        DataStreamSource<String> mysqlDS = env.addSource(mysqlSource);
        MapStateDescriptor<String, TableProcess> bcState = new MapStateDescriptor<>("bcState", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = mysqlDS
                .broadcast(bcState);

        BroadcastConnectedStream<JSONObject, String> connectedStream = filterDS.connect(broadcastStream);


        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>(TableProcess.SINK_TYPE_HBASE) {
        };
        SingleOutputStreamOperator<JSONObject> process = connectedStream.process(new BroadcastProcessFunction<JSONObject, String, JSONObject>() {

            @Override
            public void processElement(JSONObject jsonObject, ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {

                ReadOnlyBroadcastState<String, TableProcess> broadcastState = readOnlyContext.getBroadcastState(bcState);

                String table = jsonObject.getString("table");
                String type = jsonObject.getString("type");
                if ("bootstrap-server".equals(type)) {
                    type = "insert";
                    jsonObject.put("type", type);
                }

                String key = table + ":" + type;
                TableProcess tableProcess = broadcastState.get(key);

                if (tableProcess != null) {
                    String sinkType = tableProcess.getSinkType();
                    if (sinkType.equals("kafka")) {
                        collector.collect(jsonObject);
                    } else if (sinkType.equals("hbase")) {
                        readOnlyContext.output(hbaseTag, jsonObject);
                    }
                } else {
                    System.out.println("配置信息中没有对应的数据！");
                }
            }

            @Override
            public void processBroadcastElement(String jsonStr, Context context, Collector<JSONObject> collector) throws Exception {
                BroadcastState<String, TableProcess> broadcastState = context.getBroadcastState(bcState);
                JSONObject jsonObject = JSON.parseObject(jsonStr);

                String data = jsonObject.getString("data");
                System.out.println(data);

                TableProcess tableProcess = JSON.parseObject(data, TableProcess.class);

                String sourceTable = tableProcess.getSourceTable();
                String operateType = tableProcess.getOperateType();

                String key = sourceTable + ":" + operateType;

                broadcastState.put(key, tableProcess);
            }
        });

        process.print("Kafka>>>>>>>>");
        process.getSideOutput(hbaseTag).print("HBase>>>>>>>>");

        env.execute();

    }

}
