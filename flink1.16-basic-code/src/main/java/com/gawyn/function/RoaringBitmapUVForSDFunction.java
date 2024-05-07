package com.gawyn.function;

import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.roaringbitmap.RoaringBitmap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;


/**
 * 这个方法有问题？
 *  - 一行数据序列化/反序列一次，CPU扛不住，可能要探索一下其他方案
 */
public class RoaringBitmapUVForSDFunction extends AggregateFunction<Integer, RoaringBitmapUVForSDFunction.MyAcc>{

    public RoaringBitmap bitmap;

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        bitmap = new RoaringBitmap();
    }

    @Override
    public RoaringBitmapUVForSDFunction.MyAcc createAccumulator() {
        MyAcc acc = new MyAcc();
        acc.roaringBitmap = acc.serialize(bitmap);
        return acc;
    }

    /**
     *
     * @param acc
     * @param uvField : Set集合元素
     */
    public void accumulate(RoaringBitmapUVForSDFunction.MyAcc acc, Integer uvField) {
        RoaringBitmap roaringBitmap = acc.deserialize(acc.roaringBitmap);
        roaringBitmap.add(uvField);
        acc.roaringBitmap = acc.serialize(roaringBitmap);
    }

    public void retract(RoaringBitmapUVForSDFunction.MyAcc  acc, Integer uvField) {
        RoaringBitmap roaringBitmap = acc.deserialize(acc.roaringBitmap);
        roaringBitmap.remove(uvField);
        acc.roaringBitmap = acc.serialize(roaringBitmap);
    }

    public void merge(RoaringBitmapUVForSDFunction.MyAcc acc, Iterable<RoaringBitmap> it) {

    }

    public void resetAccumulator(RoaringBitmapUVForSDFunction.MyAcc acc) {
        acc.roaringBitmap = acc.serialize(new RoaringBitmap());
    }

    @Override
    public Integer getValue(RoaringBitmapUVForSDFunction.MyAcc acc) {
        RoaringBitmap roaringBitmap = acc.deserialize(acc.roaringBitmap);
        return roaringBitmap.getCardinality();
    }


    public static class MyAcc{

        public byte[] roaringBitmap;

        public byte[] serialize(RoaringBitmap bitmap){
            try{
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(baos);
                bitmap.serialize(dos);
                return baos.toByteArray();
            }catch (Exception e){
                e.printStackTrace();
            }
            return null;
        }

        public RoaringBitmap deserialize(byte[] bitmapBytes) {
            try {
                RoaringBitmap bitmap = new RoaringBitmap();
                ByteArrayInputStream bais = new ByteArrayInputStream(bitmapBytes);
                DataInputStream dis = new DataInputStream(bais);
                bitmap.deserialize(dis);
                return bitmap;
            }catch (Exception e){
                e.printStackTrace();
            }
            return null;
        }
    }

}
