package com.pgman.goku.example.demoxmlaop;

import com.pgman.goku.example.demoxmlaop.service.IAccountService;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class XmlAopTest {

    public static void main(String[] args) {
        //1.获取容器
        ApplicationContext ac = new ClassPathXmlApplicationContext("bean_xml_aop.xml");
        //2.获取对象
        IAccountService as = (IAccountService)ac.getBean("accountService");
        //3.执行方法
        as.saveAccount();
//        as.updateAccount(1);
//        as.deleteAccount();
    }

}
