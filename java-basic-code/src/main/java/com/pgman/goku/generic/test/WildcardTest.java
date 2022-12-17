package com.pgman.goku.generic.test;

import com.pgman.goku.generic.mapper.Employee;
import com.pgman.goku.generic.mapper.Manager;
import com.pgman.goku.generic.tuple.Tuple2;
import org.junit.Test;

/**
 * 通配符
 * - 子类型限定通配符
 * - 超类型限定通配符
 *
 * java应用通配符主要用来限制获取或者修改对应的规则问题，并且不会限制死泛型类型
 */
public class WildcardTest {


    @Test
    public static void testExtand() {

        Manager ceo = new Manager("唐僧",18,10);
        Manager cto = new Manager("沙僧",20,5);

        Employee employee = new Employee("白龙马", 20);

        Tuple2<Manager, Manager> managerTuple2 = new Tuple2<>(ceo, cto);

        // 不能直接将Tuple2<Manager, Manager>类型对象赋值给Tuple2<Employee,Employee>类型
//        Tuple2<Employee,Employee> employeeTuple2 = managerTuple2;

        // 可以将Tuple2<Manager, Manager>类型对象赋值给Tuple2<? extends Employee,? extends Employee>类型
        // 被通配符接受的对象，不支持进行set操作，或者说<? extends Employee>对get操作安全
        Tuple2<? extends Employee,? extends Employee> employeeTuple2 = managerTuple2;

        // 下边操作会报编译错误
//        employeeTuple2.setName(employee);
//        employeeTuple2.setName(cto);

        Employee name = employeeTuple2.getName();
        System.out.println(name);


    }

    @Test
    public void testSuper() {

        Manager ceo = new Manager("唐僧",18,10);
        Manager cto = new Manager("沙僧",20,5);
        Employee employee = new Employee("白龙马", 20);

        Tuple2<Manager, Manager> managerTuple2 = new Tuple2<>(ceo, cto);
        Tuple2<Employee, Employee> employeeTuple = new Tuple2<>(employee, employee);

        // <? super Manager>声明可以接收Manager的父类型对象
        Tuple2<? super Manager,? super Manager> employeeTuple2 = employeeTuple;

        // set方法只能设置Manager类型对象，不能设置Employee类型对象
//        employeeTuple2.setName(employee); // error
        employeeTuple2.setName(cto);

        Object ceoObj = employeeTuple2.getName();
        System.out.println((Manager)ceoObj);

    }

    public static <T extends Comparable<? super T>> T testExtandSuper(T t){
        return t;
    }

    public static void printBuddies(Tuple2<Employee,Employee> tuple2){
        Employee first = tuple2.getName();
        Employee second = tuple2.getValue();
        System.out.println(first.getName() + " and "+ second.getName() + " are buddies.");
    }


    public static void main(String[] args){
        Manager ceo = new Manager("唐僧",18,10);
        Manager cto = new Manager("沙僧",20,5);
        Employee employee = new Employee("白龙马", 20);

        Tuple2<Manager, Manager> managerTuple2 = new Tuple2<>(ceo, cto);
        Tuple2<?, ?> employeeTuple = managerTuple2;

        Manager name = managerTuple2.getName();
        System.out.println(name);

        managerTuple2.setName(cto);
        name = managerTuple2.getName();
        System.out.println(name);

//        managerTuple2.setName(employee); // error

        Tuple2<?, ?> managerTuple21 = new Tuple2<>(ceo, cto);
//        managerTuple21.setName(cto);
//        managerTuple21.setName(employee);
        Object name1 = managerTuple21.getName();
        System.out.println(name1);

        managerTuple21.setName(null);
        name1 = managerTuple21.getName();
        System.out.println(name1);
    }

}
