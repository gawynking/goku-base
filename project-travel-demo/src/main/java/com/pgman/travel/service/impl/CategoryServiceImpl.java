package com.pgman.travel.service.impl;

import com.pgman.travel.dao.CategoryDao;
import com.pgman.travel.dao.impl.CategoryDaoImpl;
import com.pgman.travel.domain.Category;
import com.pgman.travel.service.CategoryService;
import com.pgman.travel.util.JedisUtil;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class CategoryServiceImpl implements CategoryService {

    private CategoryDao categoryDao = new CategoryDaoImpl();
    private Jedis jedis = JedisUtil.getJedis();

    /**
     * 查询所有
     *
     * @return
     */
    @Override
    public List<Category> findAll() {

        // 可使用sortedset排序查询
        Set<Tuple> categorys = jedis.zrangeWithScores("category", 0, -1);

        // 2 查询数据
        List<Category> cs = null;
        //2.2 判断查询的集合是否为空
        if (categorys == null || categorys.size() == 0) {

//            System.out.println("查询mysql");

            cs = categoryDao.findAll();

            for (int i = 0; i < cs.size(); i++) {
                jedis.zadd("category", cs.get(i).getCid(), cs.get(i).getCname());
            }

        } else {

//            System.out.println("查询redis");

            cs = new ArrayList<Category>();
            int i = 1;
            for (Tuple tuple : categorys) {
                Category category = new Category();
                category.setCid((int)tuple.getScore());
                category.setCname(tuple.getElement());
                cs.add(category);
                i++;
            }

        }

//        System.out.println(cs);

        return cs;

    }

}
