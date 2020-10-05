package com.pgman.goku.test;

import com.pgman.goku.dao.IEmpAnnoDao;
import com.pgman.goku.domain.Emp;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.util.Date;
import java.util.List;

public class EmpAnnoTest {

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
        IEmpAnnoDao empDao = session.getMapper(IEmpAnnoDao.class);
        List<Emp> emps = empDao.findAll();

        for (Emp emp : emps) {
            System.out.println(emp);
        }
    }

    /**
     * 2 保存数据
     */
    @Test
    public void saveUser(){
        IEmpAnnoDao empDao = session.getMapper(IEmpAnnoDao.class);
        Emp emp = new Emp();

        emp.setEmpno(9999);
        emp.setEname("chavinking");
        emp.setHiredate(new Date());
        emp.setJob("DBA");
        emp.setMgr(7839);
        emp.setSal(1000000);
        emp.setComm(99999999);
        emp.setDeptno(10);

        empDao.saveEmp(emp);
    }

    /**
     * 3 更新数据
     */
    @Test
    public void updateEmp(){
        IEmpAnnoDao empDao = session.getMapper(IEmpAnnoDao.class);
        Emp emp = new Emp();

        emp.setEmpno(9999);
        emp.setJob("big data");

        empDao.updateEmp(emp);
    }

    /**
     * 4 删除数据
     */
    @Test
    public void deleteEmp(){
        IEmpAnnoDao empDao = session.getMapper(IEmpAnnoDao.class);
        empDao.deleteEmp(9999);
    }

    /**
     * 5 查询单个数据
     */
    @Test
    public void findById(){
        IEmpAnnoDao empDao = session.getMapper(IEmpAnnoDao.class);
        Emp emp = empDao.findById(7369);
        System.out.println(emp);
    }

    /**
     * 6 模糊查询
     */
    @Test
    public void findByName(){
        IEmpAnnoDao empDao = session.getMapper(IEmpAnnoDao.class);
//        List<Emp> emps = empDao.findByName("%LA%");
        List<Emp> emps = empDao.findByName("LA");
        for(Emp emp :emps){
            System.out.println(emp);
        }
    }

    /**
     * 7 一对多查询
     */
    @Test
    public void findEmpContainsDept(){
        IEmpAnnoDao empDao = session.getMapper(IEmpAnnoDao.class);
        List<Emp> emps = empDao.findEmpContainsDept();
        for(Emp emp :emps){
            System.out.println("------------------------------------------");
            System.out.println(emp);
            System.out.println(emp.getDept());
        }
    }
}
