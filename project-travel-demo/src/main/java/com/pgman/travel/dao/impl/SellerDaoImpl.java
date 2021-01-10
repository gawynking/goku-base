package com.pgman.travel.dao.impl;

import com.pgman.travel.dao.SellerDao;
import com.pgman.travel.domain.Seller;
import com.pgman.travel.util.JDBCUtils;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;

public class SellerDaoImpl implements SellerDao {

    private JdbcTemplate template = new JdbcTemplate(JDBCUtils.getDataSource());


    /**
     * 根据ID 查询对象
     *
     * @param id
     * @return
     */
    @Override
    public Seller findById(int id) {

        String sql = "select * from tab_seller where sid = ? ";
        return template.queryForObject(sql, new BeanPropertyRowMapper<Seller>(Seller.class), id);
    }

}
