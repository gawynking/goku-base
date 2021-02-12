package com.pgman.goku.netty.inout;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;


public class MyServerInitializer extends ChannelInitializer<SocketChannel> {

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {

        ChannelPipeline pipeline = ch.pipeline();

        // 入站的handler进行解码
        pipeline.addLast(new MyByteToLongDecoder());

        // 出站handler
        pipeline.addLast(new MyLongToByteEncoder());

        // 自定义的handler 处理业务逻辑
        pipeline.addLast(new MyServerHandler());

    }
}
