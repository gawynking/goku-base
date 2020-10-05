package com.pgman.goku.service.impl;

import com.pgman.goku.dao.IDeptDao;
import com.pgman.goku.service.IDeptService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Controller;
import org.springframework.stereotype.Repository;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;

//@Component("deptServiceAnnoImpl")
//@Repository("deptServiceAnnoImpl")
//@Controller("deptServiceAnnoImpl")
@Service("deptServiceAnnoImpl")

@Scope("singleton")
public class DeptServiceAnnoImpl implements IDeptService {

//    @Autowired
//    @Qualifier("deptDaoImpl")

    @Resource(name="deptDaoImpl")
    private IDeptDao deptDao;

    public DeptServiceAnnoImpl(){
        System.out.println("Service对象创建成功了.");
    }

    public void saveDept() {
//        System.out.println("Service执行成功了.");
        deptDao.saveDept();
    }

    @PostConstruct
    public void init(){
        System.out.println("对象创建成功了.");
    }

    @PreDestroy
    public void destroy(){
        System.out.println("对象销毁了.");
    }

}
