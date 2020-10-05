package com.pgman.goku.example.demoanno;

import com.pgman.goku.config.SpringConfiguration;
import com.pgman.goku.example.demoanno.domain.Emp;
import com.pgman.goku.example.demoanno.service.IEmpService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Date;
import java.util.List;

/**
 * 使用Junit单元测试：测试我们的配置
 * Spring整合junit的配置
 *      1、导入spring整合junit的jar(坐标)
 *      2、使用Junit提供的一个注解把原有的main方法替换了，替换成spring提供的
 *             @Runwith
 *      3、告知spring的运行器，spring和ioc创建是基于xml还是注解的，并且说明位置
 *          @ContextConfiguration
 *                  locations：指定xml文件的位置，加上classpath关键字，表示在类路径下
 *                  classes：指定注解类所在地位置
 *
 *   当我们使用spring 5.x版本的时候，要求junit的jar必须是4.12及以上
 */

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = SpringConfiguration.class)
public class AnnoWithoutXmlTest {

    @Autowired
    IEmpService empService;

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
        emp.setEmpno(5689);
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
