package com.pgman.goku.druid;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.druid.util.JdbcConstants;

import com.alibaba.druid.sql.SQLTransformUtils;

import java.util.List;

public class SQLParseTest {

    public static void main(String[] args) {

        String sql = "" +
                "select " +
                "   case when pdate = 1 then 1 when pdate = 2 then 2 end as pdate," +
                "   count(1) as pv," +
                "   count(distinct name) as uv," +
                "   distinct_udf(name) as dau " +
                "from (" +
                "   select " +
                "       name," +
                "       age," +
                "       pdage " +
                "   from test " +
                "   where city = 2 " +
                "   group by " +
                "       name," +
                "       age," +
                "       pdage" +
                "   ) t1 " +
                "where t1.name = 'goku' " +
                "and t1.age >= 20 " +
                "group by " +
                "t1.pdate " +
                "having count(1) > 2 ";

        String dbType = JdbcConstants.MYSQL;

        //格式化输出
        String result = SQLUtils.format(sql, dbType);

//        System.out.println(result); // 缺省大写格式

        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);

        //解析出的独立语句的个数
        System.out.println("size is:" + stmtList.size());

        for (int i = 0; i < stmtList.size(); i++) {

            SQLStatement stmt = stmtList.get(i);
            MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
            stmt.accept(visitor);




            //获取操作方法名称,依赖于表名称
            System.out.println("Manipulation          : " + visitor.getTables());

            //获取字段名称
            System.out.println("fields                : " + visitor.getColumns());

            System.out.println("getAggregateFunctions : " + visitor.getAggregateFunctions());

            System.out.println("getGroupByColumns     : " + visitor.getGroupByColumns());

            System.out.println("getFunctions          : " + visitor.getFunctions());

            System.out.println("getConditions         : " + visitor.getConditions());

            System.out.println("getOrderByColumns     : " + visitor.getOrderByColumns());

            System.out.println("getParameters         : " + visitor.getParameters());

            System.out.println("getDbType             : " + visitor.getDbType());

            System.out.println("getFeatures           : " + visitor.getFeatures());

            System.out.println("getRelationships      : " + visitor.getRelationships());

            System.out.println("getRepository         : " + visitor.getRepository());

            System.out.println("getTableStat          : " + visitor.getTableStat("test"));

        }
    }

}

