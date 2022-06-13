package com.pgman.goku.util;

import com.pgman.goku.config.ConfigurationManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class HBaseUtils {

    public static Logger logger = LoggerFactory.getLogger(HBaseUtils.class);

    private static HBaseUtils instance = null;

    public static HBaseUtils getInstance() {
        if (instance == null) {
            synchronized (HBaseUtils.class) {
                if (instance == null) {
                    instance = new HBaseUtils();
                }
            }
        }
        return instance;
    }

    private LinkedList<Connection> dataSource = new LinkedList<Connection>();

    private HBaseUtils(){

        int dataSourceSize = ConfigurationManager.getInteger("hbase.datasource.size");

        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum",ConfigurationManager.getString("hbase.zookeeper.quorum"));

        for(int i = 0 ;i < dataSourceSize;i++){

            try {

                Connection connection = ConnectionFactory.createConnection(config);
                dataSource.push(connection);

            }catch (Exception e ){
                e.printStackTrace();
            }

        }

    }

    public synchronized Connection getConnection() {
        while (dataSource.size() == 0) {
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return dataSource.poll();
    }

    /**
     * 关闭连接
     */
    public void close(Connection connection){
        try{
            if(null != connection){
                dataSource.push(connection);
            }
        }catch (Exception e){
            e.printStackTrace();
        }

    }


    /**
     * 判断给定rowkey是否在hbase 指定表中存在
     *
     * @param tableName
     * @param rowKey
     * @return
     */
    public boolean isDataExists(Connection connection, String tableName, String rowKey){

        try {
            Table table = connection.getTable(TableName.valueOf(tableName));

            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);

            if(result.size() == 0){
                return false;
            }else{
                return true;
            }

        }catch (Exception e){
            e.printStackTrace();
            logger.info("LogOutput : 查询hbase数据失败." + tableName + "." + rowKey);
        }

        return false;
    }


    /**
     * 执行hbase rowKey 数据写入
     *
     * @param tableName
     * @param rows
     * @param cf
     * @param cn
     */

    public boolean executePut(Connection connection,String tableName, List<String> rows, String cf, String cn){

        try{
            Table table = connection.getTable(TableName.valueOf(tableName));

            List<Put> puts = new ArrayList<>();
            for(String rowKey :rows){
                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn),Bytes.toBytes(rowKey));
                puts.add(put);
            }

            table.put(puts);

            return true;

        }catch (Exception e){
            e.printStackTrace();
            logger.info("LogOutput : 插入hbase数据失败."  + tableName + rows);
        }
        return false;
    }

}
