package com.pgman.goku.example.demoxml.service.impl;

import com.pgman.goku.example.demoxml.dao.IEmpDao;
import com.pgman.goku.example.demoxml.domain.Emp;
import com.pgman.goku.example.demoxml.service.IEmpService;
import com.pgman.goku.example.demoxml.utils.TransactionManager;

import java.util.List;

public class EmpServiceImplNonProxy implements IEmpService{

    private IEmpDao empDao;

    private TransactionManager transactionManager;

    public TransactionManager getTransactionManager() {
        return transactionManager;
    }

    public void setTransactionManager(TransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    public IEmpDao getEmpDao() {
        return empDao;
    }

    public void setEmpDao(IEmpDao empDao) {
        this.empDao = empDao;
    }

    public List<Emp> findAllEmp() {

        try {
            //1.开启事务
            transactionManager.beginTransaction();
            //2.执行操作
            List<Emp> emps = empDao.findAllEmp();
            //3.提交事务
            transactionManager.commit();
            //4.返回结果
            return emps;
        }catch (Exception e){
            //5.回滚操作
            transactionManager.rollback();
            throw new RuntimeException(e);
        }finally {
            //6.释放连接
            transactionManager.release();
        }

    }

    public Emp findById(Integer empno) {

        try {
            //1.开启事务
            transactionManager.beginTransaction();
            //2.执行操作
            Emp emp = empDao.findById(empno);
            //3.提交事务
            transactionManager.commit();
            //4.返回结果
            return emp;
        }catch (Exception e){
            //5.回滚操作
            transactionManager.rollback();
            throw new RuntimeException(e);
        }finally {
            //6.释放连接
            transactionManager.release();
        }

    }

    public void saveEmp(Emp emp) {
        try {
            //1.开启事务
            transactionManager.beginTransaction();
            //2.执行操作
            empDao.saveEmp(emp);
            //3.提交事务
            transactionManager.commit();
        }catch (Exception e){
            //4.回滚操作
            transactionManager.rollback();
            throw new RuntimeException(e);
        }finally {
            //5.释放连接
            transactionManager.release();
        }

    }

    public void updateEmp(Emp emp) {

        try {
            //1.开启事务
            transactionManager.beginTransaction();
            //2.执行操作
            empDao.updateEmp(emp);
            //3.提交事务
            transactionManager.commit();
        }catch (Exception e){
            //4.回滚操作
            transactionManager.rollback();
            throw new RuntimeException(e);
        }finally {
            //5.释放连接
            transactionManager.release();
        }

    }

    public void deleteEmp(Integer empno) {
        try {
            //1.开启事务
            transactionManager.beginTransaction();
            //2.执行操作
            empDao.deleteEmp(empno);
            //3.提交事务
            transactionManager.commit();
        }catch (Exception e){
            //4.回滚操作
            transactionManager.rollback();
            throw new RuntimeException(e);
        }finally {
            //5.释放连接
            transactionManager.release();
        }

    }

}
