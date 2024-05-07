package com.pgman.goku.test;

import org.roaringbitmap.RoaringBitmap;

public class RoaringbitmapDemo {

    public static void main(String[] args) {

        RoaringBitmap bitmap = new RoaringBitmap();
        bitmap.add(10);
        bitmap.add(3);

        RoaringBitmap integers = RoaringBitmap.bitmapOf(1, 2, 3, 3);

        System.out.println(integers.getCardinality());

        bitmap.or(integers);

        System.out.println(bitmap.getCardinality());

    }
}
