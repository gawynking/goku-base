package com.pgman.goku.jdbctemplate;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.junit.Test;
import org.springframework.jdbc.core.ColumnMapRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.util.List;
import java.util.Map;

public class JDBCTemplateExample {

    @Test
    public void update(){

        JdbcTemplate template = new JdbcTemplate(new ComboPooledDataSource("namedPool"));
        String sql = "insert into test.fact_clue_indi(clue_id,user_id,is_distribute,from_source) values(?,?,?,?)";

        template.update(sql,"5","101","1","1");
        template.update(sql,"6","101","1","1");
        template.update(sql,"7","101","1","1");

        template.update("delete from test.fact_clue_indi where id = ?",7);

    }

    @Test
    public void query(){

        JdbcTemplate template = new JdbcTemplate(new ComboPooledDataSource("namedPool"));
        String sql = "select * from test.fact_clue_indi where id >= ?";

        List<Map<String, Object>> result = template.query(sql, new ColumnMapRowMapper(), 5);

        for(Map row :result)
            System.out.println(row);

    }


}
