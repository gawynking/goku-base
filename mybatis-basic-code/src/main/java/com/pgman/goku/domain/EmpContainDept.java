package com.pgman.goku.domain;

public class EmpContainDept extends Emp{

    private String dname;
    private String loc;

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
        return super.toString() + "     EmpContainDept{" +
                "dname='" + dname + '\'' +
                ", loc='" + loc + '\'' +
                '}';
    }
}
