package com.pgman.goku.dp23.strategy.none;

public abstract class Duck {

    public Duck() {}

    public abstract void display();

    public void quack() {
        System.out.println("Duck quack");
    }

    public void swim() {
        System.out.println("Duck swim");
    }

    public void fly() {
        System.out.println("Duck fly");
    }

}
