package com.pgman.goku.test;

import com.pgman.goku.util.JpaUtils;
import org.junit.Test;

import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;
import javax.persistence.Query;
import java.util.List;

public class TestJpql {

    /**
     * 查询全部
     *      jqpl：from com.pgman.goku.domain.Customer
     *      sql：SELECT * FROM cst_customer
     */
    @Test
    public void testFindAll() {
        //1.获取entityManager对象
        EntityManager entityManager = JpaUtils.getEntityManager();
        //2.开启事务
        EntityTransaction transaction = entityManager.getTransaction();
        transaction.begin();
        //3.查询全部
        String jpql = "from com.pgman.goku.domain.Customer";
        Query query = entityManager.createQuery(jpql);//创建Query查询对象，query对象才是执行jqpl的对象

        //发送查询，并封装结果集
        List list = query.getResultList();

        for (Object obj : list) {
            System.out.println(obj);
        }

        //4.提交事务
        transaction.commit();
        //5.释放资源
        entityManager.close();
    }


    /**
     * 排序查询
     *  sql:select * from cts_customer order by cust_id desc
     *  jpql:from Customer order by custId desc
     */
    @Test
    public void testOrder() {
        //1.获取entityManager对象
        EntityManager entityManager = JpaUtils.getEntityManager();
        //2.开启事务
        EntityTransaction transaction = entityManager.getTransaction();
        transaction.begin();
        //3.查询全部
        String jpql = "from Customer order by custId desc";
        Query query = entityManager.createQuery(jpql);//创建Query查询对象，query对象才是执行jqpl的对象

        //发送查询，并封装结果集
        List list = query.getResultList();

        for (Object obj : list) {
            System.out.println(obj);
        }

        //4.提交事务
        transaction.commit();
        //5.释放资源
        entityManager.close();
    }

    /**
     *
     * 进行jpql查询
     *      1.创建query查询对象
     *      2.对参数进行赋值
     *      3.查询，并得到返回结果
     *
     *
     * 查询总数
     *
     *
     * sql:select count(cust_id) from cts_customer
     * jpql:select count(custId) from Customer
     */
    @Test
    public void testCount() {
        //1.获取entityManager对象
        EntityManager entityManager = JpaUtils.getEntityManager();
        //2.开启事务
        EntityTransaction transaction = entityManager.getTransaction();
        transaction.begin();
        //3.查询全部
        //i.根据jpql语句创建Query查询对象
        String jpql = "select count(custId) from Customer";
        Query query = entityManager.createQuery(jpql);//创建Query查询对象，query对象才是执行jqpl的对象

        //ii.对参数赋值
        //iii.发送查询，并封装结果
        Object obj = query.getSingleResult();
        System.out.println(obj);

        //4.提交事务
        transaction.commit();
        //5.释放资源
        entityManager.close();
    }


    /**
     * 分页查询
     *
     * sql:select * from cts_customer limit ?,?
     * jpql:from Customer
     *      分页参数 传参
     */
    @Test
    public void testLimit() {
        //1.获取entityManager对象
        EntityManager entityManager = JpaUtils.getEntityManager();
        //2.开启事务
        EntityTransaction transaction = entityManager.getTransaction();
        transaction.begin();
        //3.查询全部
        //i.根据jpql语句创建Query查询对象
        String jpql = "from Customer";
        Query query = entityManager.createQuery(jpql);//创建Query查询对象，query对象才是执行jqpl的对象

        //ii.对参数赋值
        query.setFirstResult(0);
        query.setMaxResults(2);

        //iii.发送查询，并封装结果
        List lists = query.getResultList();

        for(Object list :lists){
            System.out.println(list);
        }

        //4.提交事务
        transaction.commit();
        //5.释放资源
        entityManager.close();
    }


    /**
     * 条件查询
     * sql : select * from cts_customer where cust_name = ?
     * jpql:from Customer where custName = ?
     */
    @Test
    public void testCondition() {
        //1.获取entityManager对象
        EntityManager entityManager = JpaUtils.getEntityManager();
        //2.开启事务
        EntityTransaction transaction = entityManager.getTransaction();
        transaction.begin();
        //3.查询全部
        //i.根据jpql语句创建Query查询对象
        String jpql = "from Customer where custName = ?";
        Query query = entityManager.createQuery(jpql);//创建Query查询对象，query对象才是执行jqpl的对象

        //ii 为参数赋值
        query.setParameter(1, "pgman");

        //iii.发送查询，并封装结果
        List lists = query.getResultList();

        for(Object list :lists){
            System.out.println(list);
        }

        //4.提交事务
        transaction.commit();
        //5.释放资源
        entityManager.close();
    }

}
