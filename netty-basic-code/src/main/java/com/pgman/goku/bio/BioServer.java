package com.pgman.goku.bio;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 阻塞IO服务示例
 */
public class BioServer {

    private static void bioServer(int port) {

        // 注册线程池对象
        ExecutorService threadPools = Executors.newCachedThreadPool();

        // 创建特定端口的服务器socket对象
        ServerSocket serverSocket = null;

        try {
            serverSocket = new ServerSocket(port);
            System.out.println("服务器启动了");

            while (true) {

                final Socket socket = serverSocket.accept(); // 阻塞操作1,获取客户端连接
                System.out.println("连接到一个客户端 : " + "线程信息 id = " + Thread.currentThread().getId() + ";名字 = " + Thread.currentThread().getName());

                // 为监听到的socket对象 启动工作线程
                threadPools.execute(new Runnable() {
                    public void run() {
                        // 客户端通讯方法
                        handler(socket);
                    }
                });

            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                serverSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    // 通讯方法
    private static void handler(Socket socket) {

        try {

            InputStream inputStream = socket.getInputStream();
            byte[] bytes = new byte[1024];
            while (true) {

                int read = inputStream.read(bytes); // 阻塞操作2
                if (read != -1) {
                    //输出客户端发送的数据
                    System.out.println(new String(bytes, 0, read));
                } else {
                    break;
                }

            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }


    // 单元测试方法
    public static void main(String[] args) {
        bioServer(9999);
    }

}
