package com.pgman.goku.test;

import com.pgman.goku.dao.IDeptAnnoDao;
import com.pgman.goku.dao.IDeptDao;
import com.pgman.goku.domain.Dept;
import com.pgman.goku.domain.QueryVo;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class DeptAnnoTest {

    // 从 XML 中构建 SqlSessionFactory
    private String resource = "SqlMapConfig.xml";
    private InputStream inputStream;
    private SqlSessionFactory sqlSessionFactory;

    // 从 SqlSessionFactory 中获取 SqlSession
    private SqlSession session;

    /**
     * 初始化方法
     */
    @Before
    public void init() throws Exception {
        inputStream = Resources.getResourceAsStream(resource);
        sqlSessionFactory = new SqlSessionFactoryBuilder().build(inputStream);

        // 从 SqlSessionFactory 中获取 SqlSession
        session = sqlSessionFactory.openSession();
    }

    /**
     * 销毁方法
     *
     * @throws Exception
     */
    @After
    public void destroy() throws Exception {
        session.commit();
        session.close();
        inputStream.close();
    }


    /**
     * 1 查询所有
     */
    @Test
    public void findAll() {
        IDeptAnnoDao deptDao = session.getMapper(IDeptAnnoDao.class);
        List<Dept> depts = deptDao.findAll();

        for (Dept dept : depts) {
            System.out.println("-------------------------");
            System.out.println(dept);
            System.out.println(dept.getEmps());
        }
    }


}
