package com.pgman.goku.dao;

import com.pgman.goku.domain.Dept;
import com.pgman.goku.domain.QueryVo;
import org.apache.ibatis.annotations.*;
import org.apache.ibatis.mapping.FetchType;

import java.util.List;

@CacheNamespace(blocking = true)
public interface IDeptAnnoDao {

    /**
     * 1 查询所有
     */
    @Select("select * from dept")
    @Results(id="deptEmpMap",value = {
            @Result(id = true,property = "deptno",column = "deptno"),

            @Result(property = "dname",column = "dname"),
            @Result(property = "loc",column = "loc"),

            @Result(property = "emps",column = "deptno",many = @Many(
                    select = "com.pgman.goku.dao.IEmpAnnoDao.findByDeptno",
                    fetchType = FetchType.LAZY
            ))
    })
    List<Dept> findAll();

}
