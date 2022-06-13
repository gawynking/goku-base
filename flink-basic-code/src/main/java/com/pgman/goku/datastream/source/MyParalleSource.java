package com.pgman.goku.datastream.source;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

/**
 * 自定义实现一个支持并行度的source
 *
 * 针对于 event time ，可以在源端定义生成时间戳逻辑 和 发出 watermark
 */
public class MyParalleSource implements ParallelSourceFunction<Long> {

    private long count = 1L;

    private boolean isRunning = true;

    /**
     * 主要的方法
     * 启动一个source
     * 大部分情况下，都需要在这个run方法中实现一个循环，这样就可以循环产生数据了
     *
     * @param ctx
     * @throws Exception
     */
    @Override
    public void run(SourceContext<Long> ctx) throws Exception {

        while(isRunning){
            ctx.collect(count);
            count++;
            //每秒产生一条数据
            Thread.sleep(1000);
        }

//        生成时间戳和水位线
//        ctx.collectWithTimestamp();
//        ctx.emitWatermark();
//        ctx.markAsTemporarilyIdle();

    }

//    官网例子
//    @Override
//    public void run(SourceContext<MyType> ctx) throws Exception {
//        while (/* condition */) {
//            MyType next = getNext();
//            ctx.collectWithTimestamp(next, next.getEventTimestamp());
//
//            if (next.hasWatermarkTime()) {
//                ctx.emitWatermark(new Watermark(next.getWatermarkTime()));
//            }
//        }
//    }



    /**
     * 取消一个cancel的时候会调用的方法
     *
     */
    @Override
    public void cancel() {
        isRunning = false;
    }
}
