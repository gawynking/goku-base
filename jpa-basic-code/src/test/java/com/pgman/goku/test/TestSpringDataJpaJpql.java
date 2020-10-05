package com.pgman.goku.test;

import com.pgman.goku.springdatajpa.dao.CustomerDao;
import com.pgman.goku.springdatajpa.domain.Customer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.jpa.repository.Query;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.annotation.Transactional;

import java.util.Arrays;
import java.util.List;

@RunWith(SpringJUnit4ClassRunner.class) //声明spring提供的单元测试环境
@ContextConfiguration(locations = "classpath:applicationContext.xml")//指定spring容器的配置信息
public class TestSpringDataJpaJpql {
    @Autowired
    private CustomerDao customerDao;

    @Test
    public void  testFindJPQL() {
        Customer customer = customerDao.findJpql("goku");
        System.out.println(customer);
    }

    @Test
    public void  testFindCustNameAndId() {
        Customer customer = customerDao.findCustNameAndId(2,"goku");
        System.out.println(customer);
    }

    /**
     * 测试jpql的更新操作
     *  * springDataJpa中使用jpql完成 更新/删除操作
     *         * 需要手动添加事务的支持
     *         * 默认会执行结束之后，回滚事务
     *   @Rollback : 设置是否自动回滚
     *          false | true
     */
    @Test
    @Transactional //添加事务的支持
    @Rollback(value = false)
    public void testUpdateCustomer() {
        customerDao.updateCustomer(4,"pgman");
    }

    //测试sql查询
    @Test
    public void testFindSql() {
        List<Object[]> list = customerDao.findSql();
        for(Object [] obj : list) {
            System.out.println(Arrays.toString(obj));
        }
    }

    @Test
    public void testFindSqlCondition() {
        List<Object[]> list = customerDao.findSqlCondition("pgman");
        for(Object [] obj : list) {
            System.out.println(Arrays.toString(obj));
        }
    }

    //测试方法命名规则的查询
    @Test
    public void testNaming() {
        Customer customer = customerDao.findByCustName("pgman");
        System.out.println(customer);
    }

    //测试方法命名规则的查询
    @Test
    public void testFindByCustNameLike() {
        List<Customer> list = customerDao.findByCustNameLike("pgman%");
        for (Customer customer : list) {
            System.out.println(customer);
        }
    }

    //测试方法命名规则的查询
    @Test
    public void testFindByCustNameLikeAndCustIndustry() {
        Customer customer = customerDao.findByCustNameLikeAndCustIndustry("pgman%", "it");
        System.out.println(customer);
    }
}
