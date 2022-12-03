package com.pgman.goku.javadp.creational.factory.factorymethod;

import com.pgman.goku.javadp.model.SedanCar;
import com.pgman.goku.javadp.model.SportCar;
import com.pgman.goku.javadp.model.Truck;
import com.pgman.goku.javadp.model.Vehicle;

public class TruckFactory extends VehicleFactory{
    @Override
    protected Vehicle createVehicle(String item) {
        if(item.equals("small")){
            return new Truck();
        }
        if(item.equals("large")){
            return new Truck();
        }
        return null;
    }
}
