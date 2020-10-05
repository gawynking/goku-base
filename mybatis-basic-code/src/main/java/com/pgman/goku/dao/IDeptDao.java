package com.pgman.goku.dao;

import com.pgman.goku.domain.Dept;
import com.pgman.goku.domain.QueryVo;
import com.pgman.goku.mybatis.annotations.Select;

import java.util.List;

public interface IDeptDao {

    /**
     * 1 查询所有
     */
//    @Select("select * from dept")
    List<Dept> findAll();

    /**
     * 2 保存数据
     */
    void saveDept(Dept dept);

    /**
     * 3 更新数据
     */
    void updateDept(Dept dept);

    /**
     * 4 更新数据
     */
    void deleteDept(Integer deptno);

    /**
     * 5 根据id查询数据
     */
    Dept findById(Integer deptno);

    /**
     * 6 模糊查询
     */
    List<Dept> findByName(String dname);

    /**
     * 7 查询总条数
     */
    int findTotal();

    /**
     * 8 根据实体类包装条件查询数据
     */
    List<Dept> findDeptByVo(QueryVo vo);


    /**
     * 9 根据传入条件查询数据
     */
    List<Dept> findDeptByCondition(Dept dept);

    /**
     * 10 根据queryvo中提供的id集合，查询数据
     */
    List<Dept> findDepts(QueryVo vo);
	
    /**
     * 11 查询部门及下所有员工信息
     */
    List<Dept> findDeptContainEmps();

    /**
     * 12 延迟加载
     */
    List<Dept> findDeptEmpLazy();

}
