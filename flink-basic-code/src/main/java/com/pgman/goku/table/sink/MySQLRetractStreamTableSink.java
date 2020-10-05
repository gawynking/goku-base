package com.pgman.goku.table.sink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

/**
 * 自定义 mysql retract table sink
 */
public class MySQLRetractStreamTableSink implements RetractStreamTableSink<Row> {


    private TableSchema tableSchema;

    // schema信息从外部SQL文件解析传入
    public MySQLRetractStreamTableSink(String[] fieldNames, DataType[] fieldTypes) {
        this.tableSchema = TableSchema
                .builder()
                .fields(fieldNames,fieldTypes)
                .build();
    }

    @Override
    public TableSchema getTableSchema() {
        return tableSchema;
    }



    /**
     * 调用 configure 方法可将Table的schema（字段名称和类型）传递给TableSink
     *
     * @param strings
     * @param typeInformations
     * @return
     */
    @Override
    public TableSink<Tuple2<Boolean, Row>> configure(String[] strings, TypeInformation<?>[] typeInformations) {
        return null;
    }

    @Override
    public TypeInformation<Row> getRecordType() {
        return null;
    }

    @Override
    public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {

    }

}
