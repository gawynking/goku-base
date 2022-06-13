package com.pgman.goku.pojo;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

public class TableInfo {

    private String tableName = null;
    private TableSchema schema = null;
    private DataStream<Tuple2<Boolean, Row>> dataStream = null;

    public TableInfo(String tableName, TableSchema schema, DataStream<Tuple2<Boolean, Row>> dataStream) {
        this.tableName = tableName;
        this.schema = schema;
        this.dataStream = dataStream;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public TableSchema getSchema() {
        return schema;
    }

    public void setSchema(TableSchema schema) {
        this.schema = schema;
    }

    public DataStream<Tuple2<Boolean, Row>> getDataStream() {
        return dataStream;
    }

    public void setDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        this.dataStream = dataStream;
    }
}
