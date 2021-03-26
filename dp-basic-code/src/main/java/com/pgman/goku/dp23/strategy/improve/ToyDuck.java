package com.pgman.goku.dp23.strategy.improve;

public class ToyDuck extends Duck {


    public ToyDuck() {
        flyBehavior = new NoFlyBehavior();
    }

    @Override
    public void display() {
        System.out.println(" ToyDuck ");
    }

    public void quack() {
        System.out.println("ToyDuck quack");
    }

    public void swim() {
        System.out.println("ToyDuck swim");
    }

}
