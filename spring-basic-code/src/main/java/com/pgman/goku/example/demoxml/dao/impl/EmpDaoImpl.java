package com.pgman.goku.example.demoxml.dao.impl;

import com.pgman.goku.example.demoxml.dao.IEmpDao;
import com.pgman.goku.example.demoxml.domain.Emp;
import com.pgman.goku.example.demoxml.utils.ConnectionUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;

import java.util.List;

public class EmpDaoImpl implements IEmpDao{

    private QueryRunner runner;

    private ConnectionUtils connectionUtils;

    public ConnectionUtils getConnectionUtils() {
        return connectionUtils;
    }

    public void setConnectionUtils(ConnectionUtils connectionUtils) {
        this.connectionUtils = connectionUtils;
    }

    public QueryRunner getRunner() {
        return runner;
    }

    public void setRunner(QueryRunner runner) {
        this.runner = runner;
    }


    public List<Emp> findAllEmp() {
        try{
            return runner.query(connectionUtils.getThreadConnection(),"select * from emp",new BeanListHandler<Emp>(Emp.class));
        }catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Emp findById(Integer empno) {
        try{
            return runner.query(connectionUtils.getThreadConnection(),"select * from emp where empno = ? ",new BeanHandler<Emp>(Emp.class),empno);
        }catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void saveEmp(Emp emp) {
        try{
            runner.update(connectionUtils.getThreadConnection(),"insert into emp(empno,ename,job,mgr,hiredate,sal,comm,deptno)values(?,?,?,?,?,?,?,?)",
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
            runner.update(connectionUtils.getThreadConnection(),"update emp set ename = ? ,job = ? ,mgr = ? ,hiredate = ? ,sal = ? ,comm = ? ,deptno = ? where empno = ?",
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
            runner.update(connectionUtils.getThreadConnection(),"delete from emp where empno = ?",empno);
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }
}
