//: arrays/ArrayOfGenerics.java
package arrays; /* Added by Eclipse.py */
// It is possible to create arrays of generics.

import java.util.*;

public class ArrayOfGenerics {
    @SuppressWarnings("unchecked")
    public static void main(String[] args) {

        List<String>[] ls;
        List[] la = new List[10];
        System.out.println("la:" + Arrays.asList(la));

        ls = (List<String>[]) la; // "Unchecked" warning
        System.out.println("ls:" + Arrays.asList(ls));

        ls[0] = new ArrayList<String>();
        ls[1] = new ArrayList<String>();
        ls[0].add("chavin");
        ls[0].add("king");
        System.out.println("la:" + Arrays.asList(la));

        // Compile-time checking produces an error:
        //! ls[1] = new ArrayList<Integer>();

        // The problem: List<String> is a subtype of Object
        Object[] objects = ls; // So assignment is OK
        // Compiles and runs without complaint:
        objects[1] = new ArrayList<Integer>();

        // However, if your needs are straightforward it is
        // possible to create an array of generics, albeit
        // with an "unchecked" warning:
        List<BerylliumSphere>[] spheres = (List<BerylliumSphere>[]) new List[10];
        for (int i = 0; i < spheres.length; i++)
            spheres[i] = new ArrayList<BerylliumSphere>();

        System.out.println("spheres:" + Arrays.asList(spheres));
    }

} ///:~
