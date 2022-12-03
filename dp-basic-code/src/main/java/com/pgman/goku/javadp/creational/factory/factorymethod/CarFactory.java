package com.pgman.goku.javadp.creational.factory.factorymethod;

import com.pgman.goku.javadp.model.SedanCar;
import com.pgman.goku.javadp.model.SportCar;
import com.pgman.goku.javadp.model.Vehicle;

public class CarFactory extends VehicleFactory{
    @Override
    protected Vehicle createVehicle(String item) {
        if(item.equals("small")){
            return new SportCar();
        }
        if(item.equals("large")){
            return new SedanCar();
        }
        return null;
    }
}
