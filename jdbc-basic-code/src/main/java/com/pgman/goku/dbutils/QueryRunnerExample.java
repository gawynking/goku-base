package com.pgman.goku.dbutils;

import com.pgman.goku.util.C3P0JDBCUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.junit.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.List;
import java.util.Map;

public class QueryRunnerExample {

    @Test
    public void update() throws Exception{

        Connection connection = C3P0JDBCUtils.getInstance().getConnection();
        QueryRunner qr = new QueryRunner();

        String sql = "insert into test.fact_clue_indi(clue_id,user_id,is_distribute,from_source) values(?,?,?,?)";

        qr.update(connection,sql,"2","101","1","1");

        qr.update(connection,sql,"3","101","1","1");

        qr.update(connection,"delete from test.fact_clue_indi where id = 3");

    }


    @Test
    public void query() throws Exception{

        Connection connection = C3P0JDBCUtils.getInstance().getConnection();
        QueryRunner qr = new QueryRunner();

        String sql = "select clue_id,user_id,is_distribute,from_source from test.fact_clue_indi where id >= ?";

        List<Map<String, Object>> result = qr.query(connection, sql, new MapListHandler(), 1);

        for(Map row :result)
            System.out.println(row);

    }


}
