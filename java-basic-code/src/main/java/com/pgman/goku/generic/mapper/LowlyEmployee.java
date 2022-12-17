package com.pgman.goku.generic.mapper;

public class LowlyEmployee extends Employee{

    private String level;

    public LowlyEmployee() {
        super();
    }

    public LowlyEmployee(String name, Integer age, String level) {
        super(name, age);
        this.level = level;
    }

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }

}
