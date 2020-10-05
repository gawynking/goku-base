//: generics/MultipleInterfaceVariants.java
package generics; /* Added by Eclipse.py */
// {CompileTimeError} (Won't compile)

interface Payable1<T> {}

class Employee2 implements Payable1 {}

class Hourly extends Employee2 implements Payable1 {} ///:~

class Test1{
    public static void main(String[] args) {
        System.out.println("chavin king");
    }
}
