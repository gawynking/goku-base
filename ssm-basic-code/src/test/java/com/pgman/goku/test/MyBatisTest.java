package com.pgman.goku.test;

import com.pgman.goku.dao.IAccountDao;
import com.pgman.goku.domain.Account;

import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.junit.Test;

import java.io.InputStream;
import java.util.List;

public class MyBatisTest {

    @Test
    public void findAll() throws Exception{
        // 加载配置文件
        InputStream in = Resources.getResourceAsStream("SqlMapConfig.xml");
        // 创建SqlSessionFactory对象
        SqlSessionFactory factory = new SqlSessionFactoryBuilder().build(in);
        // 创建SqlSession对象
        SqlSession session = factory.openSession();
        // 获取到代理对象
        IAccountDao dao = session.getMapper(IAccountDao.class);
        // 查询所有数据
        List<Account> list = dao.findAll();
        for(Account account : list){
            System.out.println(account);
        }
        // 关闭资源
        session.close();
        in.close();
    }

    @Test
    public void saveAccount() throws Exception{

        Account account = new Account();
        account.setName("chavinking");
        account.setMoney(999999f);

        // 加载配置文件
        InputStream in = Resources.getResourceAsStream("SqlMapConfig.xml");
        // 创建SqlSessionFactory对象
        SqlSessionFactory factory = new SqlSessionFactoryBuilder().build(in);
        // 创建SqlSession对象
        SqlSession session = factory.openSession();
        // 获取到代理对象
        IAccountDao dao = session.getMapper(IAccountDao.class);
        // 查询所有数据
        dao.saveAccount(account);
        session.commit();
        // 关闭资源
        session.close();
        in.close();
    }

}
