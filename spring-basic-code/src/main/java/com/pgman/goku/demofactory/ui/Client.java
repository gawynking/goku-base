package com.pgman.goku.demofactory.ui;

import com.pgman.goku.demofactory.factory.BeanFactory;
import com.pgman.goku.demofactory.service.IDeptService;
import com.pgman.goku.demofactory.service.impl.DeptServiceImpl;

/**
 * 工厂模式解耦new关键词创建对象demo
 */
public class Client {

    public static void main(String[] args) {
//        IDeptService deptService = new DeptServiceImpl();
        // 工厂模式结构对象的创建(配置文件+反射)
        IDeptService deptService = (IDeptService)BeanFactory.getBean("deptService");
        deptService.saveDept();
    }

}
