package com.pgman.goku.service.impl;

import com.pgman.goku.dao.IProductDao;
import com.pgman.goku.domain.Product;
import com.pgman.goku.service.IProductService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service // 启用spring管理
@Transactional // 启用事务
public class ProductServiceImpl implements IProductService {

    @Autowired // 自动注入
    private IProductDao productDao;

    @Override
    public void save(Product product) {
        productDao.save(product);
    }

    @Override
    public List<Product> findAll() throws Exception {
        return productDao.findAll();
    }

}
