package com.pgman.goku;

import com.pgman.goku.util.SQLUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestPatten {


    @Test
    public void test00(){

        String sql = "insert into tbl_order_sink on mode upsert(customer_id)\n" +
                "select\n" +
                "\n" +
                "get_json_object(t1.json,'$.customer_id') as custoemr_id,\n" +
                "sum(cast(get_json_object(t1.json,'$.order_amt') as int)) as order_amt\n" +
                "\n" +
                "from tbl_order_source t1\n" +
                "where get_json_object(t1.json,'$.order_status') = '1'\n" +
                "group by get_json_object(t1.json,'$.customer_id')\n" +
                ";";


        System.out.println(SQLUtils.getPrimaryKeys(sql));

    }


    @Test
    public void test01(){

        List<String> list = new ArrayList<>();

        String sql = "create table tbl_order_sink(\n" +
                "customer_id         string    comment '客户id',\n" +
                "order_amt           int       comment '客户下单总金额'\n" +
                ") with (\n" +
                "'connector.type' = 'jdbc',\n" +
                "'connector.url' = 'jdbc:mysql://127.0.0.1:3306/test',\n" +
                "'connector.table' = 'tbl_order_sink',\n" +
                "'connector.driver' = 'com.mysql.jdbc.Driver',\n" +
                "'connector.username' = 'root',\n" +
                "'connector.password' = 'mysql',\n" +
                "'connector.read.fetch-size' = '1',\n" +
                "'connector.write.flush.max-rows' = '1',\n" +
                "'connector.write.flush.interval' = '1s'\n" +
                ")\n" +
                ";";

        list.add(sql);

        System.out.println(SQLUtils.getSinkCreateStatementFromTableName("tbl_order_sink",list));

    }


    @Test
    public void testView(){

        String sql = "create view my_view as\n" +
                "  select\n" +
                "\n" +
                "  get_json_object(t1.json,'$.customer_id') as customer_id,\n" +
                "  sum(cast(get_json_object(t1.json,'$.order_amt') as int)) as order_amt\n" +
                "\n" +
                "  from tbl_order_source t1\n" +
                "  where get_json_object(t1.json,'$.order_status') = '4'\n" +
                "  group by get_json_object(t1.json,'$.customer_id')\n" +
                "";

        String[] strings = SQLUtils.parseViewSQL(sql);

        System.out.println(strings[0]);

        System.out.println(strings[1]);

    }




}
