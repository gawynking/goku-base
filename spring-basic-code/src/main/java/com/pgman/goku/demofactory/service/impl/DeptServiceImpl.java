package com.pgman.goku.demofactory.service.impl;

import com.pgman.goku.demofactory.dao.IDeptDao;
import com.pgman.goku.demofactory.dao.impl.DeptDaoImpl;
import com.pgman.goku.demofactory.factory.BeanFactory;
import com.pgman.goku.demofactory.service.IDeptService;


public class DeptServiceImpl implements IDeptService {

//    private IDeptDao deptDao = new DeptDaoImpl();

    // 工厂模式解耦对象的创建过程
    // 为什么这块用成员变量就报NullPointerException？
//    private IDeptDao deptDao = (IDeptDao) BeanFactory.getBean("deptDao");

    public void saveDept() {

        IDeptDao deptDao = (IDeptDao) BeanFactory.getBean("deptDao");
        deptDao.saveDept();

    }
}
