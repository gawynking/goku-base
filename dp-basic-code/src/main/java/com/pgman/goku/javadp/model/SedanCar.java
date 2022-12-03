package com.pgman.goku.javadp.model;

public class SedanCar extends Vehicle{
    @Override
    public Vehicle newInstance() {
        return new SedanCar();
    }
}
