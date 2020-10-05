package com.pgman.goku.util;

import java.util.HashSet;
import java.util.Set;

public class SetUtils {


    /**
     * 求2个集合的交集
     *
     * @param set1
     * @param set2
     * @return
     */
    public static Set<String> intersection(Set<String> set1,Set<String> set2){

        Set<String> set1Local = new HashSet<>();
        set1Local.addAll(set1);

        Set<String> set2Local = new HashSet<>();
        set2Local.addAll(set2);

        set1Local.retainAll(set2Local);

        return set1Local;

    }


    /**
     * 求2个集合的差集
     *
     * @param set1
     * @param set2
     * @return
     */
    public static Set<String> difference(Set<String> set1,Set<String> set2){

        Set<String> set1Local = new HashSet<>();
        set1Local.addAll(set1);

        Set<String> set2Local = new HashSet<>();
        set2Local.addAll(set2);

        set1Local.removeAll(set2Local);

        return set1Local;

    }

    /**
     * 求2个集合的并集
     *
     * @param set1
     * @param set2
     * @return
     */
    public static Set<String> union(Set<String> set1,Set<String> set2){

        Set<String> set1Local = new HashSet<>();
        set1Local.addAll(set1);

        Set<String> set2Local = new HashSet<>();
        set2Local.addAll(set2);

        set1Local.addAll(set2Local);

        return set1Local;

    }



}
