package com.pgman.goku.dao;

import com.pgman.goku.domain.Emp;
import org.apache.ibatis.annotations.*;
import org.apache.ibatis.mapping.FetchType;

import java.util.List;

@CacheNamespace(blocking = true)
public interface IEmpAnnoDao {

    /**
     * 1 查询所有
     */
    @Select("select " +
            "empno as emp_no," +
            "ename as emp_name," +
            "hiredate as hiredate," +
            "job as job," +
            "sal as sal," +
            "comm as comm," +
            "deptno as deptno," +
            "mgr as emp_mgr" +
            " from emp")
    @Results(id = "empMap",value = {
            @Result(id = true, property = "empno", column = "emp_no"),

            @Result(id = false, property = "ename", column = "emp_name"),
            @Result(id = false, property = "hiredate", column = "hiredate"),
            @Result(id = false, property = "mgr", column = "emp_mgr"),
            @Result(id = false, property = "sal", column = "sal"),
            @Result(id = false, property = "comm", column = "comm"),
            @Result(id = false, property = "job", column = "job"),
            @Result(id = false, property = "deptno", column = "deptno")
    })
    List<Emp> findAll();

    /**
     * 2 保存数据
     */
    @Insert("insert into emp(empno,ename,job,mgr,hiredate,sal,comm,deptno)values(#{empno},#{ename},#{job},#{mgr},#{hiredate},#{sal},#{comm},#{deptno})")
    void saveEmp(Emp emp);

    /**
     * 3 更新数据
     */
    @Update("update emp set job = #{job} where empno = #{empno}")
    void updateEmp(Emp emp);

    /**
     * 4 删除数据
     */
    @Delete("delete from emp where empno = #{empno}")
    void deleteEmp(Integer empno);

    /**
     * 5 查询单个用户
     */
    @Select("select " +
            "empno as emp_no," +
            "ename as emp_name," +
            "hiredate as hiredate," +
            "job as job," +
            "mgr as emp_mgr," +
            "sal as sal," +
            "comm as comm," +
            "deptno as deptno " +
            "from emp " +
            "where empno = #{empno}")
    @ResultMap(value = {"empMap"})
    Emp findById(Integer empno);

    /**
     * 6 模糊查询
     */
//    @Select("select * from emp where ename like #{ename}")
    @Select("select * from emp where ename like '%${value}%'")
    List<Emp> findByName(String ename);

    /**
     * 7 一对一查询
     */
    @Select("select * from emp")
    @Results(id="empDeptMap",value = {
            @Result(id = true,property = "empno",column = "empno"),
            @Result(property = "ename",column = "ename"),
            @Result(property = "job",column = "job"),
            @Result(property = "hiredate",column = "hiredate"),
            @Result(property = "sal",column = "sal"),
            @Result(property = "comm",column = "comm"),
            @Result(property = "mgr",column = "mgr"),
            @Result(property = "deptno",column = "deptno"),

            @Result(property = "dept",column = "deptno",one = @One(select = "com.pgman.goku.dao.IDeptDao.findById",fetchType = FetchType.EAGER))
    })
    List<Emp> findEmpContainsDept();

    /**
     * 8 查询部门下所属员工信息
     */
    @Select("select * from emp where deptno = #{deptno}")
    List<Emp> findByDeptno();

}
