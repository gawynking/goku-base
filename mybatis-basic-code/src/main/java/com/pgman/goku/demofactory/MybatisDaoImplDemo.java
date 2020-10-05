package com.pgman.goku.demofactory;

import com.pgman.goku.dao.IEmpDao;
import com.pgman.goku.dao.impl.EmpDaoImpl;
import com.pgman.goku.domain.Emp;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

import java.io.InputStream;
import java.util.List;

/**
 * Mybatis 手写Dao实现类方式
 */
public class MybatisDaoImplDemo {

    public static void main(String args[]) {

        String resource = "SqlMapConfig.xml";
        InputStream inputStream = null;
        SqlSessionFactory sqlSessionFactory = null;

        try {
            // 从 XML 中构建 SqlSessionFactory
            inputStream = Resources.getResourceAsStream(resource);
            sqlSessionFactory = new SqlSessionFactoryBuilder().build(inputStream);

            IEmpDao empDao = new EmpDaoImpl(sqlSessionFactory);

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
                inputStream.close();
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }


}
