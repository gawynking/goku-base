package com.pgman.goku.javadp.creational.factory.simplefactory;

import com.pgman.goku.javadp.model.Vehicle;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * 利用反射机制实现的工厂模式
 * - 实现
 *  可以依赖Reflections提供功能获取具体子类，并将子类型添加进容器
 * - 优点：
 *  当新增子类实现时，不需要更改工厂类代码既可以动态获取新增子类的实例
 * - 缺点：
 *  1.执行效率会有所降低
 *  2.newInstance()方法只能支持无参构造，对于有构造参数的类支持比较不友好
 */
public class RefectVehicleFactory {

    private static Map<String,Class<? extends Vehicle>> registeredProducts = new HashMap();
    static {
        Reflections reflections = new Reflections("com",new SubTypesScanner(true));
        Set<Class<? extends Vehicle>> types = reflections.getSubTypesOf(Vehicle.class);
        for(Class<? extends Vehicle> type:types){
            String[] splits = type.getName().split("\\.");
            String split = splits[splits.length - 1];
            String name = split.substring(0, 1).toLowerCase() + split.substring(1);
            registeredProducts.put(name,type);
        }
    }

    public static Vehicle create(String type) throws InstantiationException, IllegalAccessException {
        return (Vehicle)registeredProducts.get(type).newInstance();
    }

}
