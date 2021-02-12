package com.pgman.goku.netty.inout;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class MyClientHandler  extends SimpleChannelInboundHandler<Long> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Long msg) throws Exception {

        System.out.println("--> 服务器的ip=" + ctx.channel().remoteAddress() + ",收到服务器消息=" + msg);

    }

    // 重写channelActive 发送数据
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {

        System.out.println("MyClientHandler 发送数据");
        ctx.writeAndFlush(123456L); // 发送的是一个long

    }
}
