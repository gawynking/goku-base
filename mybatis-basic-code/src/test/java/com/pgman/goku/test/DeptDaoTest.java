package com.pgman.goku.test;

import com.pgman.goku.dao.IDeptDao;
import com.pgman.goku.dao.impl.DeptDaoImpl;
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
import java.util.List;

public class DeptDaoTest {

    // 从 XML 中构建 SqlSessionFactory
    private String resource = "SqlMapConfig.xml";
    private InputStream inputStream;
    private IDeptDao deptDao;

    /**
     * 初始化方法
     */
    @Before
    public void init() throws Exception {
        inputStream = Resources.getResourceAsStream(resource);
        SqlSessionFactory sqlSessionFactory = new SqlSessionFactoryBuilder().build(inputStream);
        deptDao = new DeptDaoImpl(sqlSessionFactory);
    }

    /**
     * 销毁方法
     *
     * @throws Exception
     */
    @After
    public void destroy() throws Exception {
        inputStream.close();
    }


    /**
     * 查询所有
     */
    @Test
    public void findAll() {
        List<Dept> depts = deptDao.findAll();
        for (Dept dept : depts) {
            System.out.println(dept);
        }
    }

    /**
     * 插入数据
     */
    @Test
    public void saveDept(){

        Dept dept = new Dept();
        dept.setDname("data center");
        dept.setLoc("beijing");

        System.out.println("插入之前："+dept);
        deptDao.saveDept(dept);
        System.out.println("插入之后："+dept);
    }


    /**
     * 更新数据
     */
    @Test
    public void updateDept(){
        Dept dept = new Dept();
        dept.setDeptno(100);
        dept.setDname("data warehouse");
        dept.setLoc("shanghai");
        deptDao.updateDept(dept);
    }

    /**
     * 删除数据
     */
    @Test
    public void deleteDept(){
        deptDao.deleteDept(100);
    }

    /**
     * 根据id查询数据
     */
    @Test
    public void findById(){

        Dept dept = deptDao.findById(20);
        System.out.println(dept);
    }

    /**
     * 模糊查询
     */
    @Test
    public void findByName(){
        List<Dept> depts = deptDao.findByName("%data%");
//        List<Dept> depts = deptDao.findByName("data");
        for(Dept dept :depts ){
            System.out.println(dept);
        }

    }

    /**
     * 查询总数
     */
    @Test
    public void findTotal(){
        Integer num = deptDao.findTotal();
        System.out.println(num);
    }

}
