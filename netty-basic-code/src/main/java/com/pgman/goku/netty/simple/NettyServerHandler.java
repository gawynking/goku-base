package com.pgman.goku.netty.simple;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.util.CharsetUtil;

import java.util.concurrent.TimeUnit;


/**
 *
 */
public class NettyServerHandler extends ChannelInboundHandlerAdapter {

    /**
     * 读取数据方法
     *
     * 需要删除覆写方法的super调用 ，否则运行时会出异常
     *
     * @param ctx : 上线文环境
     * @param msg : 消息
     * @throws Exception
     */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

        // 模拟1：长操作-阻塞场景
//        Thread.sleep(10 * 1000);
//        ctx.writeAndFlush(Unpooled.copiedBuffer("这是一个阻塞测试\n",CharsetUtil.UTF_8));
//        System.out.println("==> 阻塞结束，可以执行其他操作.");

        // 解决方案1：用户程序自定义的普通任务，任务提交到 taskQueue 队列
        ctx.channel().eventLoop().execute(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(10 * 1000);
                    ctx.writeAndFlush(Unpooled.copiedBuffer("这是阻塞场景-方案1测试-1\n", CharsetUtil.UTF_8));
                    System.out.println("==> 继续");
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        });

        ctx.channel().eventLoop().execute(new Runnable() { // 启动30秒后执行
            @Override
            public void run() {
                try {
                    Thread.sleep(20 * 1000);
                    ctx.writeAndFlush(Unpooled.copiedBuffer("这是阻塞场景-方案1测试-2\n", CharsetUtil.UTF_8));
                    System.out.println("==> 继续");
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        });

        // 解决方案2：用户程序自定义的定时任务，任务提交到 scheduleTaskQueue 队列
        ctx.channel().eventLoop().schedule(new Runnable() {
            @Override
            public void run() {
                try {
                    ctx.writeAndFlush(Unpooled.copiedBuffer("这是阻塞场景-方案2测试-1\n", CharsetUtil.UTF_8));
                    System.out.println("==> 继续");
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        },5, TimeUnit.SECONDS);

        // 基础测试代码
//        System.out.println("==> 服务器读取线程 " + Thread.currentThread().getName() + " ,channle = " + ctx.channel());
//        System.out.println("==> server ctx = " + ctx);
//        System.out.println("==> channel 和 pipeline的关系: 你中有我，我中有你.");
//        Channel channel = ctx.channel(); // 获取Channel对象
//        ChannelPipeline pipeline = ctx.pipeline(); // 本质是一个双向链接, 出站入站
//
//        System.out.println("==> Channel 对象 " + channel + " ,Pipeline 对象 " + pipeline);
//
//        // 将 msg 转成一个 ByteBuf
//        // ByteBuf 是 Netty 提供的，不是 NIO 的 ByteBuffer.
//        ByteBuf buf = (ByteBuf) msg;
//        System.out.println("==> 客户端发送消息是:" + buf.toString(CharsetUtil.UTF_8));
//        System.out.println("==> 客户端地址:" + channel.remoteAddress());

    }

    /**
     * 数据读取完成
     *
     * @param ctx
     * @throws Exception
     */
    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        // 向客户端返回消息
        ctx.writeAndFlush(Unpooled.copiedBuffer("Hello Netty Client.", CharsetUtil.UTF_8));
    }

    /**
     * 异常处理
     *
     * @param ctx
     * @param cause
     * @throws Exception
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        ctx.channel().close();
    }

}
