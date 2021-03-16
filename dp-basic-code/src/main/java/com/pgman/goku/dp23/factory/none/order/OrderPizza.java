package com.pgman.goku.dp23.factory.none.order;

import com.pgman.goku.dp23.factory.none.pizza.CheesePizza;
import com.pgman.goku.dp23.factory.none.pizza.GreekPizza;
import com.pgman.goku.dp23.factory.none.pizza.PepperPizza;
import com.pgman.goku.dp23.factory.none.pizza.Pizza;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;


public class OrderPizza {

    public OrderPizza() {

        Pizza pizza = null;
        String orderType;
        do {
            orderType = getType();
            if (orderType.equals("greek")) {
                pizza = new GreekPizza();
                pizza.setName(" GreekPizza ");
            } else if (orderType.equals("cheese")) {
                pizza = new CheesePizza();
                pizza.setName(" CheesePizza ");
            } else if (orderType.equals("pepper")) {
                pizza = new PepperPizza();
                pizza.setName(" PepperPizza ");
            } else {
                break;
            }

            pizza.prepare();
            pizza.bake();
            pizza.cut();
            pizza.box();

        } while (true);
    }


    private String getType() {
        try {
            BufferedReader strin = new BufferedReader(new InputStreamReader(System.in));
            System.out.println("input pizza 类型:");
            String str = strin.readLine();
            return str;
        } catch (IOException e) {
            e.printStackTrace();
            return "";
        }
    }

}
