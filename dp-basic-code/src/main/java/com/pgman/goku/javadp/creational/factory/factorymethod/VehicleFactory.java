package com.pgman.goku.javadp.creational.factory.factorymethod;

import com.pgman.goku.javadp.model.Vehicle;

public abstract class VehicleFactory {
    protected abstract Vehicle createVehicle(String item);

    public Vehicle orderVehicle(String size,String color){
        Vehicle vehicle = createVehicle(size);
        vehicle.setColor(color);
        return vehicle;
    }
}
