package com.pgman.goku.table.udf;

import com.pgman.goku.tool.FlinkSQLJDBCTools;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * 自定义 表函数
 *
 */
public class GetCustomerInfo extends TableFunction{

    Connection connection = null;
    String sql = "select \n" +
            "\n" +
            "customer_id,\n" +
            "customer_name,\n" +
            "sex,\n" +
            "address\n" +
            "\n" +
            "from tbl_customer \n" +
            "where customer_id = ? " +
            "";

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);

        try{
            Class.forName("com.mysql.jdbc.Driver");
            connection = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/test", "root", "mysql");
        }catch (Exception e){
            e.printStackTrace();
        }

    }


    @Override
    public void close() throws Exception {
        super.close();
        connection.close();
    }


    /**
     * 执行函数逻辑
     * @param customerId
     */
    public void eval(String customerId){

        FlinkSQLJDBCTools.executeQuery(connection, sql, new String[]{customerId}, new FlinkSQLJDBCTools.QueryCallback() {
            @Override
            public void process(ResultSet rs) throws Exception {
                while(rs.next()){
                    Row row = new Row(3);
                    String customerName = rs.getString("customer_name");
                    String sex = rs.getString("sex");
                    String address = rs.getString("address");

                    row.setField(0,customerName);
                    row.setField(1,sex);
                    row.setField(2,address);
                    collector.collect(row);
                }
            }
        });

    }

    @Override
    public TypeInformation getResultType() {
        return Types.ROW(Types.STRING,Types.STRING,Types.STRING);
    }

}
