package com.pgman.goku.test;

import com.pgman.goku.javadp.creational.factory.factorymethod.CarFactory;
import com.pgman.goku.javadp.creational.factory.simplefactory.BaseMethodVehicleFactory;
import com.pgman.goku.javadp.creational.factory.simplefactory.RefectVehicleFactory;
import com.pgman.goku.javadp.model.Vehicle;
import org.junit.Test;

public class VehicleTest {

    @Test
    public void refectVeicheTest() throws InstantiationException, IllegalAccessException {

        Vehicle car1 = RefectVehicleFactory.create("car");
        Vehicle car2 = RefectVehicleFactory.create("car");
        Vehicle car3 = RefectVehicleFactory.create("car");

        System.out.println(car1);
        System.out.println(car2);
        System.out.println(car3);
    }


    @Test
    public void baseMethodVeichelTest() throws InstantiationException, IllegalAccessException {
        Vehicle car1 = BaseMethodVehicleFactory.create("car");
        Vehicle car2 = BaseMethodVehicleFactory.create("car");
        Vehicle car3 = BaseMethodVehicleFactory.create("car");

        System.out.println(car1);
        System.out.println(car2);
        System.out.println(car3);
    }

    @Test
    public void factoryMethodTest(){
        CarFactory carFactory = new CarFactory();
        Vehicle vehicle = carFactory.orderVehicle("large", "blue");
        System.out.println(vehicle);
    }
}
