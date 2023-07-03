package com.pgman.goku.calcite.parser;

import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.server.CalciteServerStatement;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.OracleSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.kafka.common.protocol.types.Field;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class CalciteSQLParser {

    public static void main(String[] args) throws Exception{
        String modelPath = "/Users/chavinking/github/goku-base/java-basic-code/src/main/resources/model.json";
        sqlParserSimpleDemo();
        System.out.println("----------------");
        sqlParserPlanDemo(modelPath);
    }


    public static void sqlParserSimpleDemo() throws SqlParseException {

        // Sql语句
        String sql = "select * from t_user where id = 1";
        // 解析配置
        SqlParser.Config mysqlConfig = SqlParser.Config.DEFAULT;
        // 创建解析器
        SqlParser parser = SqlParser.create(sql, mysqlConfig);
        // 解析sql
        SqlNode sqlNode = parser.parseQuery();
        System.out.println(sqlNode.toString());

        // 还原某个方言的SQL
        System.out.println(sqlNode.toSqlString(OracleSqlDialect.DEFAULT));
    }


    public static void sqlParserPlanDemo(String modelPath) throws SQLException, SqlParseException {
        String sql = "select * from CSV.sys_role";

        String model = modelPath;
        Properties info = new Properties();
        info.put("model", model);
        Connection connection = DriverManager.getConnection("jdbc:calcite:", info);

        CalciteServerStatement statement = connection.createStatement().unwrap(CalciteServerStatement.class);
        CalcitePrepare.Context prepareContext = statement.createPrepareContext();

        final FrameworkConfig config = Frameworks.newConfigBuilder()
                .parserConfig(SqlParser.configBuilder().setLex(Lex.MYSQL).build())
                .defaultSchema(prepareContext.getRootSchema().plus())
                .build();

        final Planner planner = Frameworks.getPlanner(config);
        final SqlNode sqlNode = planner.parse(sql);
        System.out.println(sqlNode);

    }

}
