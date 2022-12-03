package com.pgman.goku.javadp.model;

/**
 * 交通工具，车辆
 */
public abstract class Vehicle {

    abstract public Vehicle newInstance();
    private String name;
    private String color;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getColor() {
        return color;
    }

    public void setColor(String color) {
        this.color = color;
    }
}
