package com.pgman.goku.example.demoanno;

import com.pgman.goku.example.demoanno.domain.Emp;
import com.pgman.goku.example.demoanno.service.IEmpService;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.Date;
import java.util.List;

public class AnnoTest {

    private ApplicationContext applicationContext;
    private IEmpService empService;

    @Before
    public void init(){
        applicationContext = new ClassPathXmlApplicationContext("example_anno_bean.xml");
        empService = applicationContext.getBean("empService",IEmpService.class);
    }

    @Test
    public void testFindAllEmp(){

        List<Emp> emps = empService.findAllEmp();
        for(Emp emp :emps){
            System.out.println(emp);
        }
    }

    @Test
    public void testFindOne(){
        Emp emp = empService.findById(7369);
        System.out.println(emp);
    }

    @Test
    public void testSaveEmp(){
        Emp emp = new Emp();
        emp.setEmpno(5688);
        emp.setEname("pgman");
        emp.setMgr(7369);
        emp.setJob("DBA");
        emp.setSal(12598753);
        emp.setComm(456987);
        emp.setHiredate(new Date());
        emp.setDeptno(20);

        empService.saveEmp(emp);
    }

    @Test
    public void testUpdateEmp(){
        Emp emp = new Emp();
        emp.setEmpno(5689);
        emp.setEname("chavinking");
        emp.setMgr(7839);
        emp.setJob("DBA");
        emp.setSal(12598753);
        emp.setComm(456987);
        emp.setHiredate(new Date());
        emp.setDeptno(20);

        empService.updateEmp(emp);
    }

    @Test
    public void testDeleteEmp(){
        empService.deleteEmp(5689);
    }


}
