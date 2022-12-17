package com.pgman.goku.generic;

import com.pgman.goku.generic.tuple.Tuple2;
import com.pgman.goku.reflection.mapper.Person;

/**
 * 泛型方法
 */
public class GenericMethod {

    /**
     * 泛型方法
     *  如果想在方法内使用泛型变量，需要在public static 后使用<T,...>方法声明泛型类型变量
     *  泛型方法可以在普通类中定义，也可以在泛型类中定义
     *
     * @param numberStr
     * @return
     * @param <T>
     */
    public static <T> Tuple2<T,T> get(String numberStr){
        T t = (T) numberStr;
        return new Tuple2<>(t,t);
    }

    public static <T extends Person> Tuple2<T,T> get2(Object obj){
        T t = (T) obj;
        return new Tuple2<>(t,t);
    }

}
