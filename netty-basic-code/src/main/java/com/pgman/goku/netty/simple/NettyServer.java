package com.pgman.goku.netty.simple;


import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

/**
 * 核心组件：
 *  Bootstrap ：其实就是启动的意思，主要用来配置 Netty 的相关配置，串联各个组件，针对客户端。
 *  ServerBootstrap ：同上，只是它是针对服务端。
 *
 *
 */
public class NettyServer {

    public static void main(String[] args){

        /**
         *  创建BossGroup 和 WorkerGroup
         *         说明
         *         1. 创建两个线程组 bossGroup 和 workerGroup
         *         2. bossGroup 只是处理连接请求 , 真正的和客户端业务处理，会交给 workerGroup完成
         *         3. 两个都是无限循环
         *         4. bossGroup 和 workerGroup 含有的子线程(NioEventLoop)的个数
         *            默认实际 cpu核数 * 2
         */
        EventLoopGroup bossGroup = new NioEventLoopGroup(1); // bossGroup指定一个就够用了
        EventLoopGroup workGroup = new NioEventLoopGroup(4); // 指定8个 ，默认是主机cpu个数*2

        try {
            // 创建服务端启动对象
            ServerBootstrap serverBootstrap = new ServerBootstrap();

            // 配置参数
            serverBootstrap
                    .group(bossGroup, workGroup) // 设置两个线程组
                    .channel(NioServerSocketChannel.class) // 使用 NioServerSocketChannel 作为服务端通道实现
                    .option(ChannelOption.SO_BACKLOG, 128) // 设置队列连接个数
                    .childOption(ChannelOption.SO_KEEPALIVE, true) // 设置保持活动连接状态
                    .childHandler(new ChannelInitializer<SocketChannel>() { // 创建一个匿名通道对象
                        // 为pipeline设置处理器
                        @Override
                        protected void initChannel(SocketChannel socketChannel) throws Exception {
                            // 可以使用一个集合管理 SocketChannel， 再推送消息时，可以将业务加入到各个 channel 对应的 NIOEventLoop 的 taskQueue 或者 scheduleTaskQueue 中
                            System.out.println("==> Client haseCode = " + socketChannel.hashCode());
                            socketChannel.pipeline().addLast(new NettyServerHandler());
                        }
                    });

            System.out.println("==> Netty Server Running...");

            // 绑定端口，启动server端服务器
            ChannelFuture cf = serverBootstrap.bind(9955).sync();

            /**
             * 给ChannelFuture绑定监听器，用于监听我们关心的事件
             *
             * Future-listener 机制：通过返回的future对象获取操作结果
             */
            cf.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture channelFuture) throws Exception {
                    if(channelFuture.isSuccess()){
                        System.out.println("==> 绑定端口且启动成功.");
                    }
                }
            });

            // 通道关闭监听
            cf.channel().closeFuture().sync();
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            bossGroup.shutdownGracefully();
            workGroup.shutdownGracefully();
        }

    }

}
