package com.pgman.goku.test;

import com.pgman.goku.dao.IDeptDao;
import com.pgman.goku.domain.Dept;

import com.pgman.goku.mybatis.io.Resources;
import com.pgman.goku.mybatis.session.SqlSession;
import com.pgman.goku.mybatis.session.SqlSessionFactory;
import com.pgman.goku.mybatis.session.SqlSessionFactoryBuilder;

import java.io.InputStream;
import java.util.List;

public class MybatisTest {

    public static void main(String[] args) throws Exception {

        //1.读取配置文件
        InputStream in = Resources.getResourceAsStream("mybatis.xml");
        //2.创建SqlSessionFactory工厂
        SqlSessionFactory factory = new SqlSessionFactoryBuilder().build(in);

        //3.使用工厂生产SqlSession对象
        SqlSession session = factory.openSession();

        //4.使用SqlSession创建Dao接口的代理对象
        IDeptDao deptDao = session.getMapper(IDeptDao.class);
        //5.使用代理对象执行方法
        List<Dept> depts = deptDao.findAll();
        for (Dept dept : depts) {
            System.out.println(dept);
        }
        //6.释放资源
        session.close();
        in.close();

    }

}
