package com.pgman.goku.netty.dubborpc.customer;

import com.pgman.goku.netty.dubborpc.netty.NettyClient;
import com.pgman.goku.netty.dubborpc.service.HelloService;

public class ClientBootstrap {

    // 这里定义协议头
    public static final String providerName = "HelloService#hello#";

    public static void main(String[] args) throws Exception {

        // 创建一个消费者
        NettyClient customer = new NettyClient();

        // 创建代理对象
        HelloService service = (HelloService) customer.getBean(HelloService.class, providerName);

        for (; ;) {
            Thread.sleep(2 * 1000);
            // 通过代理对象调用服务提供者的方法(服务)
            String res = service.hello("hello dubbo~~~");
            System.out.println("--> ClientBootstrap 调用的结果 res = " + res);
        }
    }
}
