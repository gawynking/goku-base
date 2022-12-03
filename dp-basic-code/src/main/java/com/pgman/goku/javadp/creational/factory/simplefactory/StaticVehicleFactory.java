package com.pgman.goku.javadp.creational.factory.simplefactory;

import com.pgman.goku.javadp.model.Bike;
import com.pgman.goku.javadp.model.Car;
import com.pgman.goku.javadp.model.Truck;
import com.pgman.goku.javadp.model.Vehicle;

/**
 * 静态工厂模式
 * - 符合单一职责
 * - 符合依赖倒置
 * - 违反开闭原则 - 新增子类型需要同时更新工厂类，生产不建议使用类似方法
 */
public class StaticVehicleFactory {

    public enum VehicleType{
        Bike,Car,Truck;
    }

    public static Vehicle create(VehicleType type){
        if(type.equals(VehicleType.Bike)) return new Bike();
        if(type.equals(VehicleType.Car)) return new Car();
        if(type.equals(VehicleType.Truck)) return new Truck();
        return null;
    }

}
