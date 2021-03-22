package com.pgman.goku.dp23.decorator.runoob;

public class RedShapeDecorator extends ShapeDecorator {

    public RedShapeDecorator(Shape decoratedShape) {
        super(decoratedShape);
    }

    @Override
    public void draw() {
        decoratedShape.draw();
        setRedBorder(decoratedShape);
    }

    // 增加的新功能
    private void setRedBorder(Shape decoratedShape){
        System.out.println("Border Color: Red");
    }
}
