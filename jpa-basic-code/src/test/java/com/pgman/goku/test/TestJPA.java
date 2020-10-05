package com.pgman.goku.test;

import com.pgman.goku.domain.Customer;
import com.pgman.goku.util.JpaUtils;
import org.junit.Test;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.Persistence;

public class TestJPA {

    /**
     * 测试jpa的保存
     *
     * jpa操作步骤：
     *  1、加载配置文件创建实体管理工厂对象
     *  2、通过实体管理工厂获取实体管理器
     *  3、获取事务对象、开启事务
     *  4、完成增删改操作
     *  5、提交事务/回滚事务
     *  6、释放资源
     */
    @Test
    public void testSaveCustomer(){

        Customer customer = new Customer();
        customer.setCustName("pgman");
        customer.setCustAddress("美国");

        // 1、加载配置文件创建实体管理工厂对象
        EntityManagerFactory factory = Persistence.createEntityManagerFactory("myJpa");
        // 2、通过实体管理工厂获取实体管理器
        EntityManager entityManager = factory.createEntityManager();
        // 3、获取事务对象、开启事务
        EntityTransaction transaction = entityManager.getTransaction();
        transaction.begin();
        // 4、完成增删改操作
        entityManager.persist(customer); // persist : 保存
        // 5、提交事务/回滚事务
        transaction.commit();
        // 6、释放资源
        entityManager.close();
        factory.close();
    }


    /**
     * 测试JpaUtils工厂类
     */
    @Test
    public void testJpaUtils(){

        Customer customer = new Customer();
        customer.setCustName("goku");
        customer.setCustAddress("美国");

        EntityManager entityManager = JpaUtils.getEntityManager();
        // 3、获取事务对象、开启事务
        EntityTransaction transaction = entityManager.getTransaction();
        transaction.begin();
        // 4、完成增删改操作
        entityManager.persist(customer); // persist : 保存
        // 5、提交事务/回滚事务
        transaction.commit();
        // 6、释放资源
        entityManager.close();
    }

    /**
     * 查询1行数据
     */
    @Test
    public void testFindOne(){
        EntityManager entityManager = JpaUtils.getEntityManager();
        EntityTransaction transaction = entityManager.getTransaction();
        transaction.begin();

        // 立即加载
//        Customer customer = entityManager.find(Customer.class, 1);

        // 延迟加载
        Customer customer = entityManager.getReference(Customer.class, 1);
        System.out.println(customer);

        transaction.commit();
        entityManager.close();

    }

    /**
     * 删除数据
     */
    @Test
    public void testRemove(){
        EntityManager entityManager = JpaUtils.getEntityManager();
        EntityTransaction transaction = entityManager.getTransaction();
        transaction.begin();

        Customer customer = entityManager.find(Customer.class, 1);
        entityManager.remove(customer);

        transaction.commit();
        entityManager.close();
    }

    /**
     * 更新操作
     */
    @Test
    public void testUpdate(){
        EntityManager entityManager = JpaUtils.getEntityManager();
        EntityTransaction transaction = entityManager.getTransaction();
        transaction.begin();

        Customer customer = entityManager.find(Customer.class, 2);
        customer.setCustAddress("UA");
        entityManager.merge(customer);

        transaction.commit();
        entityManager.close();
    }

}
