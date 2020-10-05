package com.pgman.goku.test;

import com.pgman.goku.service.IAccountService;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class SpringTest {

    public static void main(String[] args) {

        ApplicationContext applicationContext = new ClassPathXmlApplicationContext("classpath:applicationContext.xml");

        IAccountService accountService = applicationContext.getBean("accountService", IAccountService.class);

        accountService.findAll();

    }
}
