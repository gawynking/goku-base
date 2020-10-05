package com.pgman.goku.domain;

import java.io.Serializable;
import java.util.List;

public class Dept implements Serializable{
    /**
     * CREATE TABLE `dept` (
     `deptno` int(11) NOT NULL AUTO_INCREMENT,
     `dname` varchar(14) DEFAULT NULL,
     `loc` varchar(13) DEFAULT NULL,
     PRIMARY KEY (`deptno`)
     ) ENGINE=InnoDB AUTO_INCREMENT=41 DEFAULT CHARSET=utf8mb4;
     */

    private int deptno;
    private String dname;
    private String loc;

    private List<Emp> emps;

    public List<Emp> getEmps() {
        return emps;
    }

    public void setEmps(List<Emp> emps) {
        this.emps = emps;
    }

    public int getDeptno() {
        return deptno;
    }

    public void setDeptno(int deptno) {
        this.deptno = deptno;
    }

    public String getDname() {
        return dname;
    }

    public void setDname(String dname) {
        this.dname = dname;
    }

    public String getLoc() {
        return loc;
    }

    public void setLoc(String loc) {
        this.loc = loc;
    }

    @Override
    public String toString() {
        return "Dept{" +
                "deptno=" + deptno +
                ", dname='" + dname + '\'' +
                ", loc='" + loc + '\'' +
                '}';
    }
}
