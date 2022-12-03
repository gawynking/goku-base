package com.pgman.goku.javadp.model;

public class Bike extends Vehicle{

    @Override
    public Vehicle newInstance() {
        return new Bike();
    }
}
