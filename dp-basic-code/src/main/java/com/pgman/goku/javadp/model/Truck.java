package com.pgman.goku.javadp.model;

public class Truck extends Vehicle{

    @Override
    public Vehicle newInstance() {
        return new Truck();
    }
}
