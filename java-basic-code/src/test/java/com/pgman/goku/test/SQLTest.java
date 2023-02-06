package com.pgman.goku.test;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.pgman.goku.sql.GokuSQLUtils;
import org.junit.Test;

import java.util.List;

public class SQLTest {

    @Test
    public void testSqlFormat(){

        String sql = "-- -1 \n" +
                "select \n" +
                "\n" +
                "-- 0 \n" +
                "t1.id as id, -- 1 \n" +
                "t1.name as name, -- 2 \n" +
                "t2.city_name -- 3 \n" +
                "\n" +
                "from tbl_a t1 \n" +
                "left join tbl_b t2 on t1.id = t2.id and t1.city_id = t2.city_id \n" +
                "\n" +
                "where t1.local = 'yz'\n" +
                ";";

        SQLUtils.parseStatements(sql, "hive")
        String sqlFormat = GokuSQLUtils.sqlFormat(sql);
        System.out.println(sqlFormat);

    }


}
