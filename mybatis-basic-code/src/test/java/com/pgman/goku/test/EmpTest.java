package com.pgman.goku.test;

import com.pgman.goku.dao.IDeptDao;
import com.pgman.goku.dao.IEmpDao;
import com.pgman.goku.domain.Dept;
import com.pgman.goku.domain.Emp;
import com.pgman.goku.domain.EmpContainDept;
import com.pgman.goku.domain.QueryVo;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.util.List;

public class EmpTest {

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
        IEmpDao empDao = session.getMapper(IEmpDao.class);
        List<Emp> emps = empDao.findAll();

        for (Emp emp : emps) {
            System.out.println(emp);
        }
    }

    /**
     * 2 查询用户信息 包含部门信息
     */
    @Test
    public void findEmpContainDept(){
        IEmpDao empDao = session.getMapper(IEmpDao.class);
        List<EmpContainDept> emps = empDao.findEmpContainDept();
        for(EmpContainDept emp :emps){
            System.out.println(emp);
        }
    }

    /**
     * 3 Mybatis方式查询员工信息包含部门信息
     */
    @Test
    public void findEmpContainDeptMybatis() {
        IEmpDao empDao = session.getMapper(IEmpDao.class);
        List<Emp> emps = empDao.findEmpContainDeptMybatis();

        for (Emp emp : emps) {
            System.out.println("-------------------------------");
            System.out.print(emp + "       ");
            System.out.println(emp.getDept());
        }
    }

    /**
     * 4 延迟加载
     */
    @Test
    public void findEmpDeptLazy(){
        IEmpDao empDao = session.getMapper(IEmpDao.class);
        List<Emp> emps = empDao.findEmpDeptLazy();
        for(Emp emp :emps){
            System.out.println("---------------------------------------------");
//            System.out.println(emp);
//            System.out.println(emp.getDept());
        }
    }

    /**
     * 5 按部门id查询数据
     */
    @Test
    public void findEmpByDeptno(){
        IEmpDao empDao = session.getMapper(IEmpDao.class);
        List<Emp> emps = empDao.findEmpByDeptno(20);
        for(Emp emp :emps){
            System.out.println(emp);
        }
    }


}
