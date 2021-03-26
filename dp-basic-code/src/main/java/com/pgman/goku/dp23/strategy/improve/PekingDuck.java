package com.pgman.goku.dp23.strategy.improve;

public class PekingDuck extends Duck {

    public PekingDuck() {
        flyBehavior = new BadFlyBehavior();

    }

    @Override
    public void display() {
        System.out.println("PekingDuck");
    }

}
