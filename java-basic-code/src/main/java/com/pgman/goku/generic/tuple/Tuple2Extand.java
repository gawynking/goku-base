package com.pgman.goku.generic.tuple;

import com.pgman.goku.generic.mapper.Employee;

/**
 * 定义泛型类
 *
 * @param <T>
 * @param <U>
 */
public class Tuple2Extand<T extends Employee,U extends Employee> {

    private T name;
    private U value;

    public Tuple2Extand(T name, U value) {
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
