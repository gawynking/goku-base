package com.pgman.goku.netty.http;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpServerCodec;

public class HttpServerInitializer extends ChannelInitializer<SocketChannel> {

    @Override
    protected void initChannel(SocketChannel socketChannel) throws Exception {

        // 获取管道对象
        ChannelPipeline pipeline = socketChannel.pipeline();

        // 添加http编解码器
        pipeline.addLast("myHttpServerCodec",new HttpServerCodec());

        // 添加自定义的处理器
        pipeline.addLast("",new HttpServerHandler());

    }
}
