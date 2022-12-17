package com.pgman.goku.generic.tuple;

/**
 * 定义泛型类
 *
 * @param <T>
 * @param <U>
 */
public class Tuple2<T,U> {

    private T name;
    private U value;

    public Tuple2(T name, U value) {
        this.name = name;
        this.value = value;
    }

    public T getName() {
        return name;
    }

    public void setName(T name) {
        this.name = name;
    }

    public U getValue() {
        return value;
    }

    public void setValue(U value) {
        this.value = value;
    }

}
