package com.pgman.goku.example.demojdbc.dao.impl;

import com.pgman.goku.example.demojdbc.dao.IEmpDao;
import com.pgman.goku.example.demojdbc.domain.Emp;
import com.pgman.goku.example.demoxml.utils.ConnectionUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

import java.util.List;

public class EmpDaoImpl extends JdbcDaoSupport implements IEmpDao {


    public List<Emp> findAllEmp() {

        List<Emp> emps = super.getJdbcTemplate().query("select * from emp",new BeanPropertyRowMapper<Emp>(Emp.class));
        return emps.isEmpty()?null:emps;

    }

    public Emp findById(Integer empno) {
        List<Emp> emps = super.getJdbcTemplate().query("select * from emp where empno = ?",new BeanPropertyRowMapper<Emp>(Emp.class),empno);
        return emps.isEmpty()?null:emps.get(0);
    }

    public void saveEmp(Emp emp) {
        super.getJdbcTemplate().update("update emp set sal = ?,comm = ? where empno = ?",emp.getSal(),emp.getComm(),emp.getEmpno());
    }

    public void updateEmp(Emp emp) {
        super.getJdbcTemplate().update("insert into emp(empno,ename.job,mgr,hiredate,sal,comm,deptno)values(?,?,?,?,?,?,?,?)",
                emp.getEmpno(),
                emp.getEname(),
                emp.getJob(),
                emp.getMgr(),
                emp.getHiredate(),
                emp.getSal(),
                emp.getComm(),
                emp.getDeptno()
                );
    }

    public void deleteEmp(Integer empno) {
        super.getJdbcTemplate().update("delete from emp where empno = ?",empno);
    }
}
