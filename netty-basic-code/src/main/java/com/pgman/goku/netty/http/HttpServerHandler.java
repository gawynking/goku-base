package com.pgman.goku.netty.http;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;

import java.net.URI;

/**
 * 1. SimpleChannelInboundHandler ： ChannelInboundHandlerAdapter 子类
 * 2. HttpObject ： 客户端和服务器端相互通讯的数据被封装成 HttpObject
 */
public class HttpServerHandler extends SimpleChannelInboundHandler<HttpObject> {

    // 读取客户端数据
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) throws Exception {

        if(msg instanceof HttpRequest){

            System.out.println("ctx 类型="+ctx.getClass());
            // 每次请求都是生成新的，浏览器都拥有自己的handler对象
            System.out.println("==> pipeline hashcode : " + ctx.pipeline().hashCode() + " ,TestHttpServerHandler hashcode : " + this.hashCode());


            System.out.println("==> msg类型 : " + msg.getClass());
            System.out.println("==> 客户端地址 : " + ctx.channel().remoteAddress());

            // 过滤指定资源
            HttpRequest httpRequest = (HttpRequest) msg;
            // 获取uri, 过滤指定的资源
            URI uri = new URI(httpRequest.uri());
            if("/favicon.ico".equals(uri.getPath())) {
                System.out.println("==> 不处理favicon.ico请求");
                return;
            }

            // 回复消息给客户端
            ByteBuf content = Unpooled.copiedBuffer("Hello Http Client,I am Netty Server.", CharsetUtil.UTF_8);

            // 构造HttpResponse
            FullHttpResponse httpResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,HttpResponseStatus.OK,content);

            httpResponse.headers().set(HttpHeaderNames.CONTENT_TYPE,"text/plain");
            httpResponse.headers().set(HttpHeaderNames.CONTENT_LENGTH,content.readableBytes());

            // 写回消息
            ctx.writeAndFlush(httpResponse);

        }



    }

}
