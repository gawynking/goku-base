package com.goku.sql;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.GokuSqlParserImpl;

public class Test {
    public static void main(String[] args) throws Exception{

        String sql1 = "submit job as 'select id,name,age from emp'";
        String sql2 = "select id,name,age from emp where city_id = 1";

        SqlParser parser = SqlParser.create(sql2, SqlParser.config().withParserFactory(GokuSqlParserImpl.FACTORY));
        SqlNode sqlNode = parser.parseStmt();
        System.out.println(sqlNode.toString());

    }
}
