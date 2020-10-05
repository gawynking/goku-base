package com.pgman.goku;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sun.prism.PixelFormat;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.jdbc.JDBCUpsertTableSink;
import org.apache.flink.api.java.io.jdbc.writer.JDBCWriter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Properties;






public class TestCustomerTableSink {
    public static void main(String[] args) throws Exception{


        //2、设置运行环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, settings);
        streamEnv.setParallelism(1);

        //3、注册Kafka数据源
        Properties browseProperties = new Properties();
        browseProperties.put("bootstrap.servers","");
        browseProperties.put("group.id","");

        DataStream<JSONObject> browseStream=streamEnv
                .addSource(new FlinkKafkaConsumer010<>("", new SimpleStringSchema(), browseProperties))
                .process(new BrowseKafkaProcessFunction());


        // 原始table
        tableEnv.registerDataStream("source_kafka_browse_log",browseStream,"userID,eventTime,eventType,productID,productPrice,eventTimeTimestamp");


        //4、注册RetractStreamTableSink
        String[] sinkFieldNames={"userID","browseNumber"};
        DataType[] sinkFieldTypes={DataTypes.STRING(),DataTypes.BIGINT()};

        RetractStreamTableSink<Row> myRetractStreamTableSink = new MyRetractStreamTableSink(sinkFieldNames,sinkFieldTypes);

        tableEnv.registerTableSink("sink_stdout",myRetractStreamTableSink);


        //5、连续查询
        //统计每个Uid的浏览次数
        String sql="insert into sink_stdout " +
                "select " +
                "" +
                "userID," +
                "count(1) as browseNumber " +
                "" +
                "from source_kafka_browse_log " +
                "where userID in ('user_1','user_2') " +
                "group by userID " +
                "";

        tableEnv.sqlUpdate(sql);


        //6、开始执行
        tableEnv.execute(TestCustomerTableSink.class.getSimpleName());


    }


    /**
     * 解析Kafka数据
     * 将Kafka JSON String 解析成JavaBean: UserBrowseLog
     * UserBrowseLog(String userID, String eventTime, String eventType, String productID, int productPrice, long eventTimeTimestamp)
     */
    private static class BrowseKafkaProcessFunction extends ProcessFunction<String, JSONObject> {

        @Override
        public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
            try {

                JSONObject log = JSON.parseObject(value, JSONObject.class);

                // 增加一个long类型的时间戳
                // 指定eventTime为yyyy-MM-dd HH:mm:ss格式的北京时间
                java.time.format.DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                OffsetDateTime eventTime = LocalDateTime.parse(log.getString("create_time"), format).atOffset(ZoneOffset.of("+08:00"));
                // 转换成毫秒时间戳
                long eventTimeTimestamp = eventTime.toInstant().toEpochMilli();
                log.put("a",eventTimeTimestamp);

                out.collect(log);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }


    /**
     * 自定义RetractStreamTableSink
     *
     * Table在内部被转换成具有Add(增加)和Retract(撤消/删除)的消息流，最终交由DataStream的SinkFunction处理。
     * DataStream里的数据格式是Tuple2类型,如Tuple2<Boolean, Row>。
     * Boolean是Add(增加)或Retract(删除)的flag(标识)。Row是真正的数据类型。
     * Table中的Insert被编码成一条Add消息。如Tuple2<True, Row>。
     * Table中的Update被编码成两条消息。一条删除消息Tuple2<False, Row>，一条增加消息Tuple2<True, Row>。
     */
    private static class MyRetractStreamTableSink implements RetractStreamTableSink<Row> {

        private TableSchema tableSchema;

        public MyRetractStreamTableSink(String[] fieldNames, DataType[] fieldTypes) {
            this.tableSchema = TableSchema.builder().fields(fieldNames,fieldTypes).build();
        }


        @Override
        public TableSchema getTableSchema() {
            return tableSchema;
        }

        // 已过时
        @Override
        public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
            return null;
        }

        // 已过时
        @Override
        public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {}

        // 最终会转换成DataStream处理
        @Override
        public DataStreamSink<Tuple2<Boolean, Row>> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
            return dataStream.addSink(new SinkFunction());
        }

        @Override
        public TypeInformation<Row> getRecordType() {
            return new RowTypeInfo(tableSchema.getFieldTypes(),tableSchema.getFieldNames());
        }

        private static class SinkFunction extends RichSinkFunction<Tuple2<Boolean, Row>> {

            public SinkFunction() {
            }

            @Override
            public void invoke(Tuple2<Boolean, Row> value, Context context) throws Exception {
                Boolean flag = value.f0;
                if(flag){
                    System.out.println("增加... " + value);
                }else {
                    System.out.println("删除... " + value);
                }
            }

        }

    }

}

