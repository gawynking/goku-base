package com.pgman.goku.example.demoxml.dao;

import com.pgman.goku.example.demoxml.domain.Emp;

import java.util.List;

public interface IEmpDao {

    /**
     * 1 查询所有
     * @return
     */
    List<Emp> findAllEmp();

    /**
     * 2 根据ID 查询用户
     * @return
     */
    Emp findById(Integer empno);

    /**
     * 3 保存数据
     * @param emp
     */
    void saveEmp(Emp emp);

    /**
     * 4 更新用户
     * @param emp
     */
    void updateEmp(Emp emp);

    /**
     * 5 删除用户
     * @param empno
     */
    void deleteEmp(Integer empno);



}
