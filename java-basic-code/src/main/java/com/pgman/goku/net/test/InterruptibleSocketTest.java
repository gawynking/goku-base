package com.pgman.goku.net.test;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public class InterruptibleSocketTest {
    public static void main(String[] args) {
        EventQueue.invokeLater(new Runnable() {
            public void run() {
                JFrame frame = new InterruptibleSocketFrame();
                frame.setTitle("InterruptibleSocketTest");
                frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
                frame.setVisible(true);   //显示此窗体
            }
        });
    }
}

class InterruptibleSocketFrame extends JFrame {
    private Scanner in;
    private JButton interruptilButton;
    private JButton blockingButton;
    private JButton cancelButton;
    private JTextArea messages;
    private TestServer server;
    private Thread connectThread;

    public InterruptibleSocketFrame() {

        JPanel northpJPanel = new JPanel();
        add(northpJPanel, BorderLayout.NORTH);//对容器进行定位
        final int TEXT_ROWS = 20;
        final int TEXT_COLUMNS = 60;
        messages = new JTextArea(TEXT_ROWS, TEXT_COLUMNS); //JTextArea类,是一个显示纯文本的多行区域
        add(new JScrollPane(messages)); //JScrollPane类，为数据源提供一个窗口
        interruptilButton = new JButton("Interruptilbel");//JButton类，按钮
        blockingButton = new JButton("Blocking");
        northpJPanel.add(interruptilButton);//将button添加到窗口中
        northpJPanel.add(blockingButton);

        interruptilButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent event) {
                interruptilButton.setEnabled(false);
                blockingButton.setEnabled(false);//blocking按钮变成不可用
                cancelButton.setEnabled(true); //cancelButton按钮变成可用
                connectThread = new Thread(new Runnable() { //创建新线程
                    public void run() {
                        try {
                            connectInterruptibly();
                        } catch (Exception e) {
                            messages.append("\nInterruptibleSocketTest.connectInterruptibly: " + e);
                        }
                    }
                });
                connectThread.start();
            }
        });

        blockingButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent event) {
                interruptilButton.setEnabled(false);
                blockingButton.setEnabled(false);
                cancelButton.setEnabled(true);
                connectThread = new Thread(new Runnable() {
                    public void run() {
                        try {
                            connectBlocking();
                        } catch (IOException e) {
                            messages.append("\nInterruptibleSocketTest.connectblocking: " + e);
                        }
                    }
                });
                connectThread.start();
            }
        });

        cancelButton = new JButton("Cancel");
        cancelButton.setEnabled(false);
        northpJPanel.add(cancelButton);
        cancelButton.addActionListener(new ActionListener() { //点击cancel按钮来中断线程
            public void actionPerformed(ActionEvent e) {
                connectThread.interrupt();
                cancelButton.setEnabled(false);

            }
        });
        server = new TestServer(); //自定义的TestServer类
        new Thread(server).start();
        pack();
    }

    /**
     * 连接到测试服务器，使用可中断I/O
     */
    public void connectInterruptibly() throws IOException {
        messages.append("Interruptible:\n");
        SocketChannel channel = SocketChannel.open(new InetSocketAddress("localhost", 8189));//可中断套接字
        try {
            in = new Scanner(channel, String.valueOf(StandardCharsets.UTF_8));
            while (!Thread.currentThread().isInterrupted()) {
                messages.append("Reading ");
                if (in.hasNextLine()) {//获取服务器的输出
                    String line = in.nextLine();
                    messages.append(line);
                    messages.append("\n");
                }
            }
        } finally {
            EventQueue.invokeLater(new Runnable() {
                public void run() {
                    messages.append("Channel closed\n");
                    interruptilButton.setEnabled(true);
                    blockingButton.setEnabled(true);
                }
            });
        }
    }

    /**
     * 连接到测试服务器，使用阻塞I/O
     */
    public void connectBlocking() throws IOException {
        messages.append("Blocking:\n");
        Socket socket = new Socket("localhost", 8189); //不可中断套接字
        try {
            in = new Scanner(socket.getInputStream(), String.valueOf(StandardCharsets.UTF_8));
            while (!Thread.currentThread().isInterrupted()) {
                messages.append("Reading ");
                if (in.hasNextLine()) {
                    String line = in.nextLine();
                    messages.append(line);
                    messages.append("\n");
                }
            }
        } finally {
            EventQueue.invokeLater(new Runnable() {
                public void run() {
                    messages.append("Socket closed\n");
                    interruptilButton.setEnabled(true);
                    blockingButton.setEnabled(true);
                }
            });
        }
    }

    /**
     * 一个监听8189端口的多线程服务器，并向客户端连续发送数字，并在发送10个数字之后挂起
     */
    class TestServer implements Runnable {
        public void run() {
            try {
                ServerSocket s = new ServerSocket(8189);
                while (true) {
                    Socket incoming = s.accept();
                    Runnable r = new TestServerHandler(incoming);
                    Thread t = new Thread(r);
                    t.start();
                }
            } catch (Exception e) {
                messages.append("\nTestServer.run: " + e);
            }
        }

    }

    /**
     * 处理客户端用于服务器套接字链接的客户端输入
     */
    class TestServerHandler implements Runnable {
        private Socket incoming;
        private int counter;

        public TestServerHandler(Socket i) {
            incoming = i;
        }

        public void run() {
            try {
                OutputStream outputStream = incoming.getOutputStream();
                PrintWriter out = new PrintWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8), true);
                while (counter < 100) {
                    counter++;
                    if (counter <= 10)
                        out.println(counter);
                    Thread.sleep(100);
                }
                incoming.close();
                messages.append("Closing Server\n");
            } catch (Exception e) {
                messages.append("\nTestServerHandler.run: " + e);
            }

        }

    }
}
