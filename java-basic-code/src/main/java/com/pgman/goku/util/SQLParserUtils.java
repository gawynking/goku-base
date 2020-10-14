package com.pgman.goku.util;


import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLUseStatement;
import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.sql.visitor.SchemaStatVisitor;
import com.alibaba.druid.stat.TableStat;
import com.alibaba.druid.util.JdbcConstants;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class SQLParserUtils {

    /**
     * hive SQL解析，返回SQL 目标表 及依赖原始表信息
     *
     * @param sql
     * @return
     * @throws ParserException
     */
    public static List<Map<String, Integer>> getTableFromTo(String sql) throws Exception {

        List<SQLStatement> stmts = SQLUtils.parseStatements(sql, JdbcConstants.HIVE);
        if (stmts == null) {
            return null;
        }

        List<Map<String, Integer>> tableInfo = new ArrayList<>();
        Map<String, Integer> fromMap = new HashMap<>();
        Map<String, Integer> toMap = new HashMap<>();

        String database = "default";
        for (SQLStatement stmt : stmts) {

            SchemaStatVisitor statVisitor = SQLUtils.createSchemaStatVisitor(JdbcConstants.HIVE);
            if (stmt instanceof SQLUseStatement) {
                database = ((SQLUseStatement) stmt).getDatabase().getSimpleName().toLowerCase();
            }

            stmt.accept(statVisitor);

            Map<TableStat.Name, TableStat> tables = statVisitor.getTables();
            if (tables != null) {
                final String dbName = database;
                tables.forEach((tableName, stat) -> {

                    if (stat.getCreateCount() > 0 || stat.getInsertCount() > 0) {
                        String toTable = tableName.getName().toLowerCase();
                        if (!toTable.contains("."))
                            toTable = dbName + "." + toTable;
                        toMap.put(toTable, stat.getCreateCount() + stat.getInsertCount());
                    } else if (stat.getSelectCount() > 0) {
                        String fromTable = tableName.getName().toLowerCase();
                        if (!fromTable.contains("."))
                            fromTable = dbName + "." + fromTable;
                        fromMap.put(fromTable, stat.getSelectCount());
                    }

                });
            } else {
                return null;
            }

        }

        tableInfo.add(toMap);
        tableInfo.add(fromMap);

        return tableInfo;

    }


}
