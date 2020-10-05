package com.pgman.goku.domain;

import com.pgman.goku.domain.Dept;

import java.util.ArrayList;
import java.util.List;

public class QueryVo {

    private Dept dept;
    private List<Integer> deptnos;

    public List<Integer> getDeptnos() {
        return deptnos;
    }

    public void setDeptnos(List<Integer> deptnos) {
        this.deptnos = deptnos;
    }

    public Dept getDept() {
        return dept;
    }

    public void setDept(Dept dept) {
        this.dept = dept;
    }

    @Override
    public String toString() {
        return "QueryVo{" +
                "dept=" + dept +
                '}';
    }
}
