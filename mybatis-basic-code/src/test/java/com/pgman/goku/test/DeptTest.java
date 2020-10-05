package com.pgman.goku.test;

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

import javax.xml.bind.annotation.XmlType;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class DeptTest {

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
        IDeptDao deptDao = session.getMapper(IDeptDao.class);
        List<Dept> depts = deptDao.findAll();

        for (Dept dept : depts) {
            System.out.println(dept);
        }
    }

    /**
     * 2 插入数据
     */
    @Test
    public void saveDept(){

        Dept dept = new Dept();
        dept.setDname("data center");
        dept.setLoc("beijing");

        IDeptDao deptDao = session.getMapper(IDeptDao.class);
        System.out.println("插入之前："+dept);
        deptDao.saveDept(dept);
        System.out.println("插入之后："+dept);
    }

    /**
     * 3 更新数据
     */
    @Test
    public void updateDept(){
        Dept dept = new Dept();
        dept.setDeptno(100);
        dept.setDname("bigdata");
        dept.setLoc("shanghai");

        IDeptDao deptDao = session.getMapper(IDeptDao.class);
        deptDao.updateDept(dept);
    }

    /**
     * 4 删除数据
     */
    @Test
    public void deleteDept(){
        IDeptDao deptDao = session.getMapper(IDeptDao.class);
        deptDao.deleteDept(100);
    }

    /**
     * 5 根据id查询数据
     */
    @Test
    public void findById(){
        IDeptDao deptDao = session.getMapper(IDeptDao.class);
        Dept dept = deptDao.findById(20);
        System.out.println(dept);
    }

    /**
     * 6 模糊查询
     */
    @Test
    public void findByName(){
        IDeptDao deptDao = session.getMapper(IDeptDao.class);
        List<Dept> depts = deptDao.findByName("%data%");
//        List<Dept> depts = deptDao.findByName("data");
        for(Dept dept :depts ){
            System.out.println(dept);
        }

    }

    /**
     * 7 查询总数
     */
    @Test
    public void findTotal(){
        IDeptDao deptDao = session.getMapper(IDeptDao.class);
        Integer num = deptDao.findTotal();
        System.out.println(num);
    }

    /**
     * 8 根据实体类封装条件查询数据
     */
    @Test
    public void findDeptByVo(){

        QueryVo vo = new QueryVo();
        Dept dept = new Dept();
        dept.setDname("%data%");
        vo.setDept(dept);

        IDeptDao deptDao = session.getMapper(IDeptDao.class);
        List<Dept> depts = deptDao.findDeptByVo(vo);
        for(Dept dpet :depts){
            System.out.println(dpet);
        }

    }

    /**
   /**
     * 9 根据传入条件动态返回数据
     */
    @Test
    public void findDeptByCondition(){
        Dept dept = new Dept();
        dept.setDname("big data");
        dept.setLoc("beijing");
        IDeptDao deptDao = session.getMapper(IDeptDao.class);
        List<Dept> depts = deptDao.findDeptByCondition(dept);
        for(Dept d :depts){
            System.out.println(d);
        }

    }

    /**
     * 10 根据queryvo中提供的id集合，查询数据
     */
    @Test
    public void findDepts(){
        QueryVo vo = new QueryVo();
        List<Integer> deptnos = new ArrayList<Integer>();
        deptnos.add(100);
        deptnos.add(200);
        deptnos.add(300);

        vo.setDeptnos(deptnos);

        IDeptDao deptDao = session.getMapper(IDeptDao.class);
        List<Dept> depts = deptDao.findDepts(vo);
        for(Dept dept :depts){
            System.out.println(dept);
        }

    }

     /**
     * 11 查询用户下所有员工信息
     */
    @Test
    public void findDeptContainEmps(){
        IDeptDao deptDao = session.getMapper(IDeptDao.class);
        List<Dept> depts = deptDao.findDeptContainEmps();
        for(Dept dept :depts){
            System.out.println("---------------------------------------------------");
            System.out.println(dept + "       ");
            System.out.println(dept.getEmps());
        }

    }

    /**
     * 12 延迟加载
     */
    @Test
    public void findDeptEmpLazy(){
        IDeptDao deptDao = session.getMapper(IDeptDao.class);
        List<Dept> depts = deptDao.findDeptEmpLazy();
        for(Dept dept : depts){
//            System.out.println("---------------------------------------------");
//            System.out.println(dept);
//            System.out.println(dept.getEmps());
        }
    }

    /**
     * 13 缓存Mybatis一级缓存
     */
    @Test
    public void testFirstLevelCacha(){

        IDeptDao deptDao = session.getMapper(IDeptDao.class);
        Dept dept1 = deptDao.findById(20);
        System.out.println(dept1);


        Dept dept2 = deptDao.findById(20);
        System.out.println(dept2);

        System.out.println(dept1 == dept2);

    }

    /**
     * 14 测试二级缓存
     */
    @Test
    public void testSecondLevelCache(){

        SqlSession session1 = sqlSessionFactory.openSession();
        IDeptDao deptDao1 = session1.getMapper(IDeptDao.class);
        Dept dept1 = deptDao1.findById(20);
        System.out.println(dept1);
        session1.close();

        SqlSession session2 = sqlSessionFactory.openSession();
        IDeptDao deptDao2 = session2.getMapper(IDeptDao.class);
        Dept dept2 = deptDao2.findById(20);
        System.out.println(dept2);
        session2.close();

        System.out.println(dept1 == dept2);
    }

}
