package com.pgman.goku.dao.impl;

import com.pgman.goku.dao.IDeptDao;
import org.springframework.stereotype.Repository;

@Repository(value = "deptDaoImpl")
public class DeptDaoAnnoImpl implements IDeptDao {

    public void saveDept() {
        System.out.println("保存了部门信息.");
    }
}
