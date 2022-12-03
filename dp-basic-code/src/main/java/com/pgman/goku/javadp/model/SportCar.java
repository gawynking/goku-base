package com.pgman.goku.javadp.model;

public class SportCar extends Vehicle{
    @Override
    public Vehicle newInstance() {
        return new SportCar();
    }
}
