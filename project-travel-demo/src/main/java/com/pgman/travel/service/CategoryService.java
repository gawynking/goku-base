package com.pgman.travel.service;

import com.pgman.travel.domain.Category;

import java.util.List;

public interface CategoryService {

    /**
     * 查询所有
     *
     * @return
     */
    List<Category> findAll();


}
