package com.pgman.goku.example.demojdbc;

import com.pgman.goku.example.demojdbc.domain.Emp;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import com.pgman.goku.example.demojdbc.service.IEmpService;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.lang.Nullable;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "classpath:bean_jdbc.xml")
public class JdbcTest {

    @Autowired
    @Qualifier("empService")
    private IEmpService empService;

    @Test
    public void testJdbcTemplate(){
        //准备数据源：spring的内置数据源
        DriverManagerDataSource ds = new DriverManagerDataSource();

        ds.setDriverClassName("com.mysql.jdbc.Driver");
        ds.setUrl("jdbc:mysql://localhost:3306/mybatis");
        ds.setUsername("root");
        ds.setPassword("mysql");

        //1.创建JdbcTemplate对象
        JdbcTemplate jt = new JdbcTemplate();
        //给jt设置数据源
        jt.setDataSource(ds);
        //2.执行操作
        jt.execute("insert into emp(empno,ename,job,mgr,hiredate,sal,comm,deptno)values(9865,'pgman','DBA',7369,'1990-03-15',100000,500000,10)");
    }

    @Test
    public void testJdbcTemplateXml(){
        //1.获取容器
        ApplicationContext ac = new ClassPathXmlApplicationContext("bean_jdbc.xml");
        //2.获取对象
        JdbcTemplate jt = ac.getBean("jdbcTemplate",JdbcTemplate.class);
        //3.执行操作
        jt.execute("insert into emp(empno,ename,job,mgr,hiredate,sal,comm,deptno)values(9966,'pgman','DBA',7369,'1990-03-15',100000,500000,10)");
    }

    @Test
    public void testJdbcTemplateCrud(){
        //1.获取容器
        ApplicationContext ac = new ClassPathXmlApplicationContext("bean_jdbc.xml");
        //2.获取对象
        JdbcTemplate jt = ac.getBean("jdbcTemplate",JdbcTemplate.class);
        //3.执行操作
        //保存
//        jt.update("insert into emp(empno,ename,job,mgr,hiredate,sal,comm,deptno)values(?,?,?,?,?,?,?,?)",7888,"chavin","DBA",7369,"1990-03-15",100000,500000,10);
        //更新
//        jt.update("update emp set ename=?,sal=? where empno=?","chavinking",888888888,7888);
        //删除
//        jt.update("delete from emp where empno=?",7888);

        //查询所有
//        List<Emp> emps = jt.query("select * from emp where sal > ?",new EmpRowMapper(),1000);
//        List<Emp> emps = jt.query("select * from emp where sal > ?",new BeanPropertyRowMapper<Emp>(Emp.class),1000); // 原始表中数据存在null值报错
//        for(Emp emp : emps){
//            System.out.println(emp);
//        }

        //查询一个
//        List<Emp> accounts = jt.query("select * from emp where empno = ?",new BeanPropertyRowMapper<Emp>(Emp.class),7369);
//        System.out.println(accounts.isEmpty()?"没有内容":accounts.get(0));

        //查询返回一行一列（使用聚合函数，但不加group by子句）
//        Integer count = jt.queryForObject("select count(*) from emp where sal > ?",Integer.class,1000);
//        System.out.println(count);
    }

    @Test
    public void testJdbcTemplateFindAllEmpDao(){

        List<Emp> emps = empService.findAllEmp();
        for(Emp emp :emps){
            System.out.println(emp);
        }
    }


    @Test
    public void testJdbcTemplateFindByIdDao(){

        Emp emp = empService.findById(7369);
        System.out.println(emp);
    }

    @Test
    public void testJdbcTemplatesaveEmpDao(){

        Emp emp = new Emp();
        emp.setEmpno(9999);
        emp.setEname("pg");
        emp.setJob("dba");
        emp.setMgr(7369);
        emp.setSal(99999999);

        empService.saveEmp(emp);
    }

}


/**
 * 定义Emp的封装策略
 */
class EmpRowMapper implements RowMapper<Emp> {

    @Nullable
    public Emp mapRow(ResultSet resultSet, int i) throws SQLException {
        Emp emp = new Emp();

        emp.setEmpno(resultSet.getInt("empno"));
        emp.setEname(resultSet.getString("ename"));
        emp.setJob(resultSet.getString("job"));
        emp.setMgr(resultSet.getInt("mgr"));
        emp.setHiredate(resultSet.getDate("hiredate"));
        emp.setSal(resultSet.getInt("sal"));
        emp.setComm(resultSet.getInt("comm"));
        emp.setDeptno(resultSet.getInt("deptno"));

        return emp;
    }
}
