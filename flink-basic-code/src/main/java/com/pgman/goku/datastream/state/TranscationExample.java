package com.pgman.goku.datastream.state;

import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.apache.flink.streaming.runtime.operators.GenericWriteAheadSink;

public class TranscationExample {

    public static void main(String[] args) {


    }


    /**
     *
     * WAL（预写日志实现）
     * 实现 模板类 GenericWriteAheadSink
     */
    public static void walExample(){


    }


    /**
     * 2PC (两阶段提交实现) ：一种内部实现 checkpoint 与 事务绑定 ；也可以自定义模拟事务实现
     * 实现 TwoPhaseCommitSinkFunction
     */
    public static void tpcExample(){


    }



}
