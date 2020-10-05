package com.pgman.goku.demofactory;

import com.pgman.goku.dao.IEmpDao;
import com.pgman.goku.domain.Emp;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

import java.io.InputStream;
import java.util.List;

public class MybatisBasicDemo {

    public static void main(String[] args) {

        String resource = "SqlMapConfig.xml";
        InputStream inputStream = null;
        SqlSessionFactory sqlSessionFactory = null;
        SqlSession session = null;

        try {
            // 从 XML 中构建 SqlSessionFactory
            inputStream = Resources.getResourceAsStream(resource);
            sqlSessionFactory = new SqlSessionFactoryBuilder().build(inputStream);

            // 从 SqlSessionFactory 中获取 SqlSession
            session = sqlSessionFactory.openSession();

            // SqlSession 执行SQL语句
            IEmpDao empDao = session.getMapper(IEmpDao.class);
            List<Emp> emps = empDao.findAll();

            // 打印结果
            for (Emp emp : emps) {
                System.out.println(emp);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {

            try {
                // 释放资源
                session.close();
                inputStream.close();
            } catch (Exception e) {
                e.printStackTrace();
            }

        }


    }
}
