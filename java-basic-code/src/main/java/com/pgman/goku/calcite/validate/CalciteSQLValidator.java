package com.pgman.goku.calcite.validate;

import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.server.CalciteServerStatement;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.ValidationException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class CalciteSQLValidator {

    public static void main(String[] args) throws Exception{
        String modelPath = "/Users/chavinking/github/goku-base/java-basic-code/src/main/resources/model.json";
        String sql = "select * from (select * from CSV.sys_role order by role)";
        sqlValidatorPlanDemo(sql,modelPath);
        System.out.println("----------------------");
        validateSql(sql,modelPath);
    }



    public static void sqlValidatorPlanDemo(String sql,String model) throws SQLException, SqlParseException, ValidationException {

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

        SqlNode validate = planner.validate(sqlNode);

        System.out.println(validate);
    }


    public static void validateSql(String sql,String modelPath) throws Exception {

        SqlParser.Config mysqlConfig = SqlParser.configBuilder().setLex(Lex.MYSQL).build();
        SqlParser parser = SqlParser.create(sql, mysqlConfig);
        SqlNode sqlNode = parser.parseQuery();
        System.out.println("校验前SqlNode：");
        System.out.println(sqlNode.toString());


        // 构造SqlValidator实例
        Connection connection = DriverManager.getConnection("jdbc:calcite:model="+modelPath);
        CalciteServerStatement statement = connection.createStatement().unwrap(CalciteServerStatement.class);
        CalcitePrepare.Context prepareContext = statement.createPrepareContext();

        SqlTypeFactoryImpl factory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        CalciteCatalogReader calciteCatalogReader = new CalciteCatalogReader(
                prepareContext.getRootSchema(),
                prepareContext.getDefaultSchemaPath(),
                factory,
                new CalciteConnectionConfigImpl(new Properties())
        );

        final SqlStdOperatorTable instance = SqlStdOperatorTable.instance();



        SqlValidator validator = SqlValidatorUtil.newValidator(
                SqlOperatorTables.chain(instance, calciteCatalogReader),
                calciteCatalogReader,
                factory,
                SqlValidator.Config.DEFAULT.withIdentifierExpansion(true)
        );


        // 校验
        final SqlNode validatedSqlNode = validator.validate(sqlNode);
        System.out.println("校验后的SqlNode：");
        System.out.println(validatedSqlNode);
    }
}
