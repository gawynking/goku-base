package com.pgman.goku.dao.impl;

import com.pgman.goku.dao.IDeptDao;
import com.pgman.goku.domain.Dept;
import com.pgman.goku.domain.QueryVo;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;

import java.util.List;

public class DeptDaoImpl implements IDeptDao {

    private SqlSessionFactory sqlSessionFactory;

    public DeptDaoImpl(SqlSessionFactory sqlSessionFactory) {
        this.sqlSessionFactory = sqlSessionFactory;
    }


    public List<Dept> findAll() {
        SqlSession session = sqlSessionFactory.openSession();
        List<Dept> depts = session.selectList("com.pgman.goku.dao.IDeptDao.findAll");
        session.close();
        return depts;
    }


    public void saveDept(Dept dept) {
        SqlSession session = sqlSessionFactory.openSession();
        session.insert("com.pgman.goku.dao.IDeptDao.saveDept", dept);
        session.commit();
        session.close();
    }


    public void updateDept(Dept dept) {
        SqlSession session = sqlSessionFactory.openSession();
        session.insert("com.pgman.goku.dao.IDeptDao.updateDept", dept);
        session.commit();
        session.close();

    }

    public void deleteDept(Integer deptno) {

        SqlSession session = sqlSessionFactory.openSession();
        session.insert("com.pgman.goku.dao.IDeptDao.deleteDept", deptno);
        session.commit();
        session.close();
    }

    public Dept findById(Integer deptno) {
        SqlSession session = sqlSessionFactory.openSession();
        Dept dept = session.selectOne("com.pgman.goku.dao.IDeptDao.findById", deptno);
        session.close();
        return dept;
    }

    public List<Dept> findByName(String dname) {
        SqlSession session = sqlSessionFactory.openSession();
        List<Dept> depts = session.selectList("com.pgman.goku.dao.IDeptDao.findByName", dname);
        session.close();
        return depts;
    }

    public int findTotal() {
        SqlSession session = sqlSessionFactory.openSession();
        int num = session.selectOne("com.pgman.goku.dao.IDeptDao.findTotal");
        session.close();
        return num;
    }

    public List<Dept> findDeptByVo(QueryVo vo) {
        return null;
    }

    public List<Dept> findDeptByCondition(Dept dept) {
        return null;
    }

    public List<Dept> findDepts(QueryVo vo) {
        return null;
    }

    public List<Dept> findDeptContainEmps() {
        return null;
    }

    public List<Dept> findDeptEmpLazy() {
        return null;
    }
}
