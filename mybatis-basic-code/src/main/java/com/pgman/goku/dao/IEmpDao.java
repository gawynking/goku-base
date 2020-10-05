package com.pgman.goku.dao;

import com.pgman.goku.domain.Emp;
import com.pgman.goku.domain.EmpContainDept;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface IEmpDao {

    /**
     *  1 查询所有
     * @return
     */
//    使用注解方式需要去掉注释
//    @Select("select * from emp")
    List<Emp> findAll();

    /**
     * 2 查询员工信息同时带有部门名称和地址
     */
    List<EmpContainDept> findEmpContainDept();

    /**
     * 3 Mybatis方式查询员工信息包含部门信息
     */
    List<Emp> findEmpContainDeptMybatis();

    /**
     * 4 延迟加载
     */
    List<Emp> findEmpDeptLazy();

    /**
     * 5 根据部门ID 查询数据
     */
    List<Emp> findEmpByDeptno(Integer deptno);

}
