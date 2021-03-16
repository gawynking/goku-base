package com.pgman.goku.dp23.factory.abs.pizzastore.pizza;

public class LDPepperPizza extends Pizza {
    @Override
    public void prepare() {
        setName("LDPepperPizza");
        System.out.println(" 伦敦胡椒披萨 ");
    }
}
