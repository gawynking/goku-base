package com.pgman.goku.netty.dubborpc.provider;

import com.pgman.goku.netty.dubborpc.service.HelloService;

public class HelloServiceImpl implements HelloService {

    private static int count = 0;

    @Override
    public String hello(String mes) {
        System.out.println("==> HelloServiceImpl 收到客户端消息 = " + mes);

        if (mes != null) {
            return "你好客户端, 我已经收到你的消息 [" + mes + "] 第" + (++count) + " 次";
        } else {
            return "你好客户端, 我已经收到你的消息 ";
        }
    }
}
