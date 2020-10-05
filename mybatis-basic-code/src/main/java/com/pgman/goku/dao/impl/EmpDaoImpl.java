package com.pgman.goku.dao.impl;

import com.pgman.goku.dao.IEmpDao;
import com.pgman.goku.domain.Emp;
import com.pgman.goku.domain.EmpContainDept;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;

import java.util.List;

/**
 * 手写Dao实现类方法
 */
public class EmpDaoImpl implements IEmpDao{

    private SqlSessionFactory sqlSessionFactory;

    public EmpDaoImpl(SqlSessionFactory sqlSessionFactory){
        this.sqlSessionFactory = sqlSessionFactory;
    }

    public List<Emp> findAll() {
        SqlSession session = sqlSessionFactory.openSession();
        List<Emp> emps = session.selectList("com.pgman.goku.dao.IEmpDao.findAll");
        session.close();
        return emps;
    }

    public List<EmpContainDept> findEmpContainDept() {
        return null;
    }

    public List<Emp> findEmpContainDeptMybatis() {
        return null;
    }

    public List<Emp> findEmpDeptLazy() {
        return null;
    }

    public List<Emp> findEmpByDeptno(Integer deptno) {
        return null;
    }
}
