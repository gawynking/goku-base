package com.pgman.goku.ui;

import com.pgman.goku.dao.IDeptDao;
import com.pgman.goku.demodi.DiDemo1;
import com.pgman.goku.demodi.DiDemo2;
import com.pgman.goku.demodi.DiDemo3;
import com.pgman.goku.service.IDeptService;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.xml.XmlBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

/**
 * Spring Iod实现对象的创建
 */
public class XmlClient {

    public static void main(String[] args) {

//        testApplicationContext();

//        testBeanFactory();


//        testSingleInstance();

//        testLifecycle();

//        diDemo1();
        diDemo2();
//        diDemo3();
    }


    /**
     * ApplicationContext的三个常用实现类：
     * ClassPathXmlApplicationContext：它可以加载类路径下的配置文件，要求配置文件必须在类路径下,不在的话，加载不了。(更常用)
     * FileSystemXmlApplicationContext：它可以加载磁盘任意路径下的配置文件(必须有访问权限）
     * <p>
     * AnnotationConfigApplicationContext：它是用于读取注解创建容器的。
     */
    public static void testApplicationContext() {


        // 通过类路径下的配置文件创建ApplicationContext对象
        ApplicationContext applicationContext = new ClassPathXmlApplicationContext("bean.xml");

        // 通过磁盘路径加载配置文件
//        ApplicationContext applicationContext = new FileSystemXmlApplicationContext("E:\\goku\\spring-basic-code\\src\\main\\resources\\bean.xml");

//        IDeptDao deptDao = (IDeptDao)applicationContext.getBean("deptDao");
        IDeptService deptService = applicationContext.getBean("deptService", IDeptService.class);

        deptService.saveDept();

    }


    /**
     * 核心容器的两个接口引发出的问题：
     * ApplicationContext:     单例对象适用              采用此接口
     * 它在构建核心容器时，创建对象采取的策略是采用立即加载的方式。也就是说，只要一读取完配置文件马上就创建配置文件中配置的对象。
     * <p>
     * BeanFactory:            多例对象使用
     * 它在构建核心容器时，创建对象采取的策略是采用延迟加载的方式。也就是说，什么时候根据id获取对象了，什么时候才真正的创建对象。
     */
    public static void testBeanFactory() {

        Resource resource = new ClassPathResource("bean.xml");
        BeanFactory beanFactory = new XmlBeanFactory(resource);
        IDeptService deptService = (IDeptService) beanFactory.getBean("deptService");

        System.out.println(deptService);

    }

    /**
     * Spring创建对象默认采用单例模式
     */
    public static void testSingleInstance() {
        ApplicationContext applicationContext = new ClassPathXmlApplicationContext("bean.xml");
        IDeptService deptService1 = applicationContext.getBean("deptService", IDeptService.class);
        IDeptService deptService2 = applicationContext.getBean("deptService", IDeptService.class);
        System.out.println(deptService1 == deptService2);
    }

    /**
     * Bean生命周期
     */
    public static void testLifecycle() {
        ClassPathXmlApplicationContext applicationContext = new ClassPathXmlApplicationContext("bean.xml");
        IDeptService deptService = applicationContext.getBean("deptService", IDeptService.class);
        deptService.saveDept();
        applicationContext.close();
    }

    /**
     * 构造函数依赖注入
     */
    public static void diDemo1() {
        ApplicationContext applicationContext = new ClassPathXmlApplicationContext("bean.xml");
        DiDemo1 diDemo1 = applicationContext.getBean("diDemo1", DiDemo1.class);
        diDemo1.print();
    }

    /**
     * set方法注入
     */
    public static void diDemo2() {
        ApplicationContext applicationContext = new ClassPathXmlApplicationContext("bean.xml");
        DiDemo2 diDemo2 = applicationContext.getBean("diDemo2", DiDemo2.class);
        diDemo2.print();
    }

    /**
     * 复杂类型注入
     */
    public static void diDemo3() {
        ApplicationContext applicationContext = new ClassPathXmlApplicationContext("bean.xml");
        DiDemo3 diDemo3 = (DiDemo3) applicationContext.getBean("diDemo3");
        diDemo3.print();
    }

}
