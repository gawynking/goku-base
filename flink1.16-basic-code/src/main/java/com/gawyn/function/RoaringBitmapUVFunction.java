package com.gawyn.function;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.AggregateFunction;
import org.roaringbitmap.RoaringBitmap;


public class RoaringBitmapUVFunction extends AggregateFunction<Integer, RoaringBitmapUVFunction.MyAcc>{

    /**
     * 创建基于RoaringBitmap的累加器
     *
     * @return
     */
    @Override
    public RoaringBitmapUVFunction.MyAcc createAccumulator() {
        MyAcc acc = new MyAcc();
        acc.roaringBitmap = new RoaringBitmap();
        return acc;
    }

    /**
     * 向累加器添加元素
     *
     * @param acc
     * @param uvField
     */
    public void accumulate(RoaringBitmapUVFunction.MyAcc acc, Integer uvField) {
        acc.roaringBitmap.add(uvField);
    }

    /**
     * 撤回元素
     *
     * @param acc
     * @param uvField
     */
    public void retract(RoaringBitmapUVFunction.MyAcc  acc, Integer uvField) {
        acc.roaringBitmap.remove(uvField);
    }

    /**
     * 合并多个累加器
     *
     * @param acc
     * @param it
     */
    public void merge(RoaringBitmapUVFunction.MyAcc acc, Iterable<RoaringBitmap> it) {
        for(RoaringBitmap rb : it){
            acc.roaringBitmap.or(rb);
        }
    }

    /**
     * 重置累加器
     *
     * @param acc
     */
    public void resetAccumulator(RoaringBitmapUVFunction.MyAcc acc) {
        acc.roaringBitmap = new RoaringBitmap();
    }

    /**
     * 获取UV值
     *
     * @param acc
     * @return
     */
    @Override
    public Integer getValue(RoaringBitmapUVFunction.MyAcc acc) {
        return acc.roaringBitmap.getCardinality();
    }


    public static class MyAcc{

        /**
         * 这里属性必须被@DataTypeHint("RAW")声明，否则会触发编译错误：
         *  Field 'highLowContainer' of class 'org.roaringbitmap.RoaringBitmap' is neither publicly accessible nor does it have a corresponding getter method.
         */
        @DataTypeHint("RAW")
        public RoaringBitmap roaringBitmap;

    }

}
