package com.pgman.goku.dp23.strategy.improve;

public abstract class Duck {

    // 聚合接口
    FlyBehavior flyBehavior;
    QuackBehavior quackBehavior;

    public Duck() {
    }

    public abstract void display();

    public void quack() {
        System.out.println(" Duck quack ");
    }

    public void swim() {
        System.out.println(" Duck swim ");
    }

    public void fly() {
        if (flyBehavior != null) {
            flyBehavior.fly();
        }
    }

    public void setFlyBehavior(FlyBehavior flyBehavior) {
        this.flyBehavior = flyBehavior;
    }

    public void setQuackBehavior(QuackBehavior quackBehavior) {
        this.quackBehavior = quackBehavior;
    }

}
