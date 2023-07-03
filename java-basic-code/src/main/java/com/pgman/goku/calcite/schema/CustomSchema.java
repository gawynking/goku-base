package com.pgman.goku.calcite.schema;

import org.apache.calcite.config.CalciteConnectionProperty;

import org.apache.calcite.server.ServerDdlExecutor;

import java.sql.*;
import java.util.Properties;


public class CustomSchema {

    public static void main(String[] args) throws Exception{
        customSchema();
    }

    public static void customSchema() throws SQLException {

        final Properties p = new Properties();
        p.put(CalciteConnectionProperty.PARSER_FACTORY.camelName(), ServerDdlExecutor.class.getName() + "#PARSER_FACTORY");
        try (final Connection conn = DriverManager.getConnection("jdbc:calcite:", p)) {

            final Statement s = conn.createStatement();
            // 创建schema
            s.execute("CREATE SCHEMA s");
            // 创建表
            s.executeUpdate("CREATE TABLE s.t(age int, name varchar(10))");
            s.executeUpdate("INSERT INTO s.t values(18,'jimo'),(20,'hehe')");
            ResultSet rs = s.executeQuery("SELECT count(*) FROM s.t");

            final int columnCount = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                for (int i = 0; i < columnCount; i++) {
                    System.out.println(rs.getObject(i + 1));
                }
            }

            // 创建视图
            s.executeUpdate("CREATE VIEW v1 AS select name from s.t");
            rs = s.executeQuery("SELECT * FROM v1");

            final int columnCount1 = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                for (int i = 0; i < columnCount1; i++) {
                    System.out.println(rs.getObject(i + 1));
                }
            }

            // 创建数据类型
            s.executeUpdate("CREATE TYPE vc10 as varchar(10)");
            s.executeUpdate("CREATE TABLE t1(age int, name vc10)");

            // 删除
            s.executeUpdate("DROP VIEW v1");
            s.executeUpdate("DROP TYPE vc10");
        }
    }
}