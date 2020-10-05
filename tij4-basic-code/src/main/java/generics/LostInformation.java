//: generics/LostInformation.java
package generics; /* Added by Eclipse.py */

import java.util.*;

class Frob {}

class Fnorkle {}

class Quark<Q> {}

class Particle<POSITION, MOMENTUM> {}

public class LostInformation {
    public static void main(String[] args) {

        List<Frob> list = new ArrayList<Frob>();
        Map<Frob, Fnorkle> map = new HashMap<Frob, Fnorkle>();
        Quark<Fnorkle> quark = new Quark<Fnorkle>();
        Particle<Long, Double> p = new Particle<Long, Double>();

        String str = "ChavinKing";

        System.out.println(Arrays.toString(list.getClass().getTypeParameters()));
        System.out.println(Arrays.toString(map.getClass().getTypeParameters()));
        System.out.println(Arrays.toString(quark.getClass().getTypeParameters()));
        System.out.println(Arrays.toString(p.getClass().getTypeParameters()));

        System.out.println(Arrays.toString(str.getClass().getTypeParameters()));

        System.out.println("-------------------------------------------------------------------");

        System.out.println(list.getClass().getName());
        System.out.println(map.getClass().getName());
        System.out.println(quark.getClass().getName());
        System.out.println(p.getClass().getName());

        System.out.println(str.getClass().getName());

    }
} /* Output:
[E]
[K, V]
[Q]
[POSITION, MOMENTUM]
*///:~
