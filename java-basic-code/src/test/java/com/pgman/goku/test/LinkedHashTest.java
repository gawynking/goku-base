package com.pgman.goku.test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

public class LinkedHashTest {
    public static void main(String[] args) {

        ArrayList<String> remaining = new ArrayList<>();
        remaining.add("a");
        remaining.add("b");
        remaining.add("c");
        Iterator<String> iter = remaining.iterator();

        while (iter.hasNext()) {
            if(iter.next()!="b"){
                iter.remove();
            }
        }

        remaining.remove("a");

        System.out.println(remaining);

    }
}
