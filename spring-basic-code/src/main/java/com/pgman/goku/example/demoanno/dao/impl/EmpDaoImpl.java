package com.pgman.goku.example.demoanno.dao.impl;

import com.pgman.goku.example.demoanno.dao.IEmpDao;
import com.pgman.goku.example.demoanno.domain.Emp;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository("empDao")
public class EmpDaoImpl implements IEmpDao {

    @Autowired
    private QueryRunner runner;

    public List<Emp> findAllEmp() {
        try{
            return runner.query("select * from emp",new BeanListHandler<Emp>(Emp.class));
        }catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Emp findById(Integer empno) {
        try{
            return runner.query("select * from emp where empno = ? ",new BeanHandler<Emp>(Emp.class),empno);
        }catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void saveEmp(Emp emp) {
        try{
            runner.update("insert into emp(empno,ename,job,mgr,hiredate,sal,comm,deptno)values(?,?,?,?,?,?,?,?)",
                    emp.getEmpno(),
                    emp.getEname(),
                    emp.getJob(),
                    emp.getMgr(),
                    emp.getHiredate(),
                    emp.getSal(),
                    emp.getComm(),
                    emp.getDeptno()
            );
        }catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void updateEmp(Emp emp) {
        try{
            runner.update("update emp set ename = ? ,job = ? ,mgr = ? ,hiredate = ? ,sal = ? ,comm = ? ,deptno = ? where empno = ?",
                    emp.getEname(),
                    emp.getJob(),
                    emp.getMgr(),
                    emp.getHiredate(),
                    emp.getSal(),
                    emp.getComm(),
                    emp.getDeptno(),
                    emp.getEmpno()
            );
        }catch (Exception e){
            throw new RuntimeException(e);
        }

    }

    public void deleteEmp(Integer empno) {
        try{
            runner.update("delete from emp where empno = ?",empno);
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }
}
