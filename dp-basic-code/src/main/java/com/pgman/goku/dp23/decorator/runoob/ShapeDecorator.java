package com.pgman.goku.dp23.decorator.runoob;

// 装饰者需要实现被装饰接口，同时需要持有被装饰对象
public abstract class ShapeDecorator implements Shape {

    protected Shape decoratedShape;

    public ShapeDecorator(Shape decoratedShape){
        this.decoratedShape = decoratedShape;
    }

    public void draw(){
        decoratedShape.draw();
    }
}
