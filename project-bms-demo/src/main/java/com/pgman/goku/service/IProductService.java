package com.pgman.goku.service;

import com.pgman.goku.domain.Product;

import java.util.List;

public interface IProductService {

    public List<Product> findAll() throws Exception;

    void save(Product product) throws Exception;

}
