package com.pgman.goku.test;

import com.pgman.goku.sql.GokuSQLUtils;
import org.junit.Test;

public class SQLTest {

    @Test
    public void testSqlFormat(){

        String sql = "SELECT\n" +
                "\tany_value ( sc.SId ) SId,\n" +
                "\tany_value ( student.Sname ) Sname,\n" +
                "\tavg( sc.score ) score \n" +
                "FROM\n" +
                "\tsc\n" +
                "\tJOIN student ON sc.SId = student.SId and 1=1\n" +
                "GROUP BY\n" +
                "\tsc.SId \n" +
                "HAVING\n" +
                "\tavg( sc.score ) > 60 and 2=2;";

        String sqlFormat = GokuSQLUtils.sqlFormat(sql);
        System.out.println(sqlFormat);

    }


}
