//: generics/MultipleInterfaceVariants.java
package generics; /* Added by Eclipse.py */
// {CompileTimeError} (Won't compile)

interface Payable<T> {}

class Employee1 implements Payable<Employee1> {}
//class Hourly extends Employee1 implements Payable<Hourly> {} ///:~
//class A implements Payable<String>,Payable<Integer>{}

class Test{
    public static void main(String[] args) {
        System.out.println("chavin king");
    }
}
