package com.pgman.goku.dp23.factory.simple.pizzastore.order;

import com.pgman.goku.dp23.factory.simple.pizzastore.pizza.CheesePizza;
import com.pgman.goku.dp23.factory.simple.pizzastore.pizza.GreekPizza;
import com.pgman.goku.dp23.factory.simple.pizzastore.pizza.PepperPizza;
import com.pgman.goku.dp23.factory.simple.pizzastore.pizza.Pizza;


/**
 * 简单工厂模式-实现pisa店订购任务：
 *
 * 看一个披萨的项目：要便于披萨种类的扩展，要便于维护
 * 1) 披萨的种类很多(比如 GreekPizz、CheesePizz 等)
 * 2) 披萨的制作有 prepare，bake, cut, box
 * 3) 完成披萨店订购功能。
 *
 *
 * 简单工厂模式的设计方案: 定义一个可以实例化Pizaa对象的类，封装创建对象的代码。
 *
 */
public class SimpleFactory {

    public Pizza createPizza(String orderType) {

        Pizza pizza = null;

        // 封装了new关键字
        System.out.println("简单工厂模式");
        if (orderType.equals("greek")) {
            pizza = new GreekPizza();
            pizza.setName(" greek ");
        } else if (orderType.equals("cheese")) {
            pizza = new CheesePizza();
            pizza.setName(" cheese ");
        } else if (orderType.equals("pepper")) {
            pizza = new PepperPizza();
            pizza.setName(" pepper ");
        }

        return pizza;

    }


    public static Pizza createPizza2(String orderType) {

        Pizza pizza = null;

        System.out.println("简单工厂-静态方法模式");
        if (orderType.equals("greek")) {
            pizza = new GreekPizza();
            pizza.setName(" greek ");
        } else if (orderType.equals("cheese")) {
            pizza = new CheesePizza();
            pizza.setName(" cheese ");
        } else if (orderType.equals("pepper")) {
            pizza = new PepperPizza();
            pizza.setName(" pepper ");
        }

        return pizza;

    }

}
