package com.pgman.travel.dao;


import com.pgman.travel.domain.Category;

import java.util.List;

public interface CategoryDao {

    /**
     * 查询所有
     *
     * @return
     */
    List<Category> findAll();

}
