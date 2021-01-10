package com.pgman.travel.dao.impl;


import com.pgman.travel.dao.CategoryDao;
import com.pgman.travel.domain.Category;
import com.pgman.travel.util.JDBCUtils;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;

public class CategoryDaoImpl implements CategoryDao {

    private JdbcTemplate template = new JdbcTemplate(JDBCUtils.getDataSource());

    @Override
    public List<Category> findAll() {

        String sql = "select * from tab_category order by cid";

        List<Category> categories = template.query(sql, new BeanPropertyRowMapper<Category>(Category.class));

        return categories;

    }

}
