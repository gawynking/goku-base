//: generics/Holder.java
package generics; /* Added by Eclipse.py */

public class Holder<T> {

    private T value;

    public Holder() {
    }

    public Holder(T val) {
        value = val;
    }

    public void set(T val) {
        value = val;
    }

    public T get() {
        return value;
    }

    public boolean equals(Object obj) {
        return value.equals(obj);
    }

    public static void main(String[] args) {

        Holder<Apple> Apple = new Holder<Apple>(new Apple());
        Apple d = Apple.get();
        Apple.set(d);

        // Holder<Fruit> Fruit = Apple; // Cannot upcast
        Holder<? extends Fruit> fruit = Apple; // OK
        Fruit p = fruit.get();
        d = (Apple) fruit.get(); // Returns 'Object'
        try {
            Orange c = (Orange) fruit.get(); // No warning
        } catch (Exception e) {
            System.out.println(e);
        }

        /**
         * The set( ) method won’t work with either an Apple or a Fruit, because the set( ) argument is also "? Extends
         * Fruit," which means it can be anything and the compiler can’t verify type safety for "anything."
         *
         * The compiler is only paying attention to the types of objects that are passed
         * and returned. It is not analyzing the code to see if you perform any actual writes or reads.
         */
        // fruit.set(new Apple()); // Cannot call set()
        // fruit.set(new Fruit()); // Cannot call set()
        System.out.println(fruit.equals(d)); // OK

    }

} /* Output: (Sample)
java.lang.ClassCastException: Apple cannot be cast to Orange
true
*///:~
