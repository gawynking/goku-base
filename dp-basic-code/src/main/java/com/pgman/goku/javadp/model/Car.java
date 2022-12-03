package com.pgman.goku.javadp.model;

public class Car extends Vehicle{

    @Override
    public Vehicle newInstance() {
        return new Car();
    }
}
