package com.pgman.goku.generic.mapper;

public class Manager extends Employee{

    private int mgrNumbers;

    public Manager() {
        super();
    }

    public Manager(String name, Integer age, int mgrNumbers) {
        super(name, age);
        this.mgrNumbers = mgrNumbers;
    }

    public int getMgrNumbers() {
        return mgrNumbers;
    }

    public void setMgrNumbers(int mgrNumbers) {
        this.mgrNumbers = mgrNumbers;
    }

}
