package com.pgman.goku.druid;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.hive.parser.HiveStatementParser;
import com.alibaba.druid.sql.parser.SQLParserFeature;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.pgman.goku.sql.GokuSQLUtils;
import com.pgman.goku.util.SQLParserUtils;

import java.util.List;

public class Test {

    String sql = "-- 测试代码 \n" +
            "-- @chavinking \n" +
            "with tmp1 as (\n" +
            "\tselect \n" +
            "\t\tid, \n" +
            "\t\torder_id, \n" +
            "\t\tdiscount_amt\n" +
            "\tfrom mydb.act_order\n" +
            "\twhere dt >= '20220501' -- 开始日期 \n" +
            "\tand dt <= '20220531' -- 结束日期\n" +
            "), \n" +
            "tmp2 as (\n" +
            "\tselect \n" +
            "\t\tid, \n" +
            "\t\torder_id, \n" +
            "\t\tstatus as ctime\n" +
            "\tfrom mydb.peisong\n" +
            "\twhere dt >= '20220501' -- 带分号注释;带分号注释\n" +
            "\tand dt <= '20220531'\n" +
            ")\n" +
            "\n" +
            "select distinct \n" +
            "\tt1.dt, \n" +
            "\tt1.poi_id, \n" +
            "\tt2.poi_name, \n" +
            "\tt1.order_amt, \n" +
            "\tt3.product_id, \n" +
            "\tt3.product_name, \n" +
            "\tt3.product_price, \n" +
            "\tt3.product_cnt, \n" +
            "\tt4.pv, \n" +
            "\tt4.uv, \n" +
            "\tif(t1.id = t2.id, t4.user_id, null) as is_use, \n" +
            "\tcase \n" +
            "\t\twhen a = b and c = d and d = e and get_json_object(abc, '$.aaaaaaaa') is not null and f = c and f = c and f = c and f = c and f = c and f = c and f = c and f = c and f = c and f = c and f = c and f = c and f = c and f = c and f = c and f = c and f = c and f = c and f = c and b = e then 'a'\n" +
            "\tend as is_test\n" +
            "from mydb.tmp_order t1\n" +
            "join mydb.tmp_poi t2 on t1.poi_id = t2.poi_id\n" +
            "left join mydb.tmp_order_detail t3 on t1.order_id = t3.order_id\n" +
            "left join (\n" +
            "\tselect distinct \n" +
            "\t\tt1.dt, \n" +
            "\t\tt1.poi_id, \n" +
            "\t\tt1.order_id, \n" +
            "\t\tt1.product_id, \n" +
            "\t\tcount(1) as pv, \n" +
            "\t\tcount(DISTINCT t1.user_id) as uv, \n" +
            "\t\tcase \n" +
            "\t\t\twhen t1.poi_type = 1 then 'b2c'\n" +
            "\t\t\twhen t1.poi_type = 2 then 'o2o'\n" +
            "\t\t\twhen t1.poi_type = 3 then 'wm'\n" +
            "\t\t\telse 'other'\n" +
            "\t\tend as poi_type, \n" +
            "\t\trow_number() over (partition by t1.dt, t1.poi_id, t1.order_id order by t1.dt desc) as rn\n" +
            "\tfrom mydb.tmp_flow t1\n" +
            "\twhere t1.dt >= '20220501'\n" +
            "\tand t1.dt <= '20220531'\n" +
            "\tand t1.status = 1\n" +
            "\tgroup by \n" +
            "\t\tt1.dt, \n" +
            "\t\tt1.poi_id, \n" +
            "\t\tt1.order_id, \n" +
            "\t\tcase \n" +
            "\t\t\twhen t1.poi_type = 1 and t1.city_id = 2 then 'b2c'\n" +
            "\t\t\twhen t1.poi_type = 2 then 'o2o'\n" +
            "\t\t\twhen t1.poi_type = 3 then 'wm'\n" +
            "\t\t\telse 'other'\n" +
            "\t\tend\n" +
            "\thaving count(1) > 1\n" +
            "\tand count(DISTINCT t1.user_id) > 1\n" +
            "\torder by \n" +
            "\t\tt1.dt, \n" +
            "\t\tt1.poi_id, \n" +
            "\t\tt1.order_id, \n" +
            "\t\tt1.product_id\n" +
            ") t4\n" +
            "\t on t1.dt = t4.dt\n" +
            "\tand t1.poi_id = t4.poi_id\n" +
            "\tand t1.order_id = t4.order_id\n" +
            "\tand t3.product_id = t4.product_id\n" +
            "left join tmp1 t5 on t1.order_id = t5.order_id\n" +
            "left join tmp2 t6 on t1.order_id = t6.order_id\n" +
            "order by \n" +
            "\tt1.dt, \n" +
            "\tt1.poi_id, \n" +
            "\tt2.poi_name, \n" +
            "\tt1.order_amt, \n" +
            "\tt3.product_id, \n" +
            "\tt3.product_name\n" +
            ";";


    @org.junit.Test
    public void sqlFormat(){

        String sqlStr = SQLParserUtils.sqlFormat(sql);
        System.out.println(sqlStr);

    }

    @org.junit.Test
    public void sqlTrace(){

        SQLStatementParser parser = new HiveStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();

        System.out.println(statementList);

    }

    @org.junit.Test
    public void test02(){

        String sql = "-- 1 开头注释\n" +
                "insert into table tmp.abc \n" +
                "-- 1.1 test \n" +
                "select -- 2 select注释 \n" +
                "-- 3 空白注释 \n" +
                "a,b,-- 4 空白注释 \n" +
                "c, -- 5 空白注释 \n" +
                "d,e,f -- 6 空白注释 \n" +
                "-- 7 空白注释 \n" +
                "from tmp t1 -- 8 空白注释 \n" +
                "-- 9 空白注释 \n" +
                "group by -- 10 空白注释 \n" +
                "a,b,c,\n" +
                "d,-- 11 空白注释 \n" +
                "-- 12 空白注释 \n" +
                "e,f \n" +
                "-- 13 空白注释 \n" +
                "grouping sets( -- 14 空白注释 \n" +
                "-- 15 空白注释 \n" +
                "    (a,b,c),(b,c,d),(d,e,f))\n" +
                "-- 16 空白注释 ";

        SQLStatement sqlStatement = SQLUtils.parseSingleStatement(sql, DbType.hive);


        String format = GokuSQLUtils.sqlFormat(sql);
        System.out.println(format);

    }

}
