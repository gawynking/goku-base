package com.pgman.goku.service.impl;

import com.pgman.goku.dao.IDeptDao;
import com.pgman.goku.service.IDeptService;

public class DeptServiceXmlImpl implements IDeptService {

//    private IDeptDao deptDao = new DeptDaoImpl();

    private IDeptDao deptDao;

    public DeptServiceXmlImpl(){
        System.out.println("Service对象创建成功了.");
    }

    public void saveDept() {

//        deptDao.saveDept();

        System.out.println("Service执行成功了.");
    }

    public void init(){
        System.out.println("对象创建成功了.");
    }

    public void destroy(){
        System.out.println("对象销毁了.");
    }


}
