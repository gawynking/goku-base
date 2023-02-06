package com.pgman.goku.net.server;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public class ThreacEchoServer {
    public static void main(String[] args) throws IOException {

        ServerSocket serverSocket = new ServerSocket(8888);
        int i = 1;
        while (true){
            Socket socket = serverSocket.accept();
            System.out.println("Spawning " + i);
            ThreadEchoHandler handler = new ThreadEchoHandler(socket);
            Thread thread = new Thread(handler);
            thread.start();
            i++;
        }

    }

    public static class ThreadEchoHandler implements Runnable{
        private Socket socket;

        public ThreadEchoHandler(Socket socket){
            this.socket=socket;
        }

        @Override
        public void run() {
            try {
                InputStream inputStream = socket.getInputStream();
                OutputStream outputStream = socket.getOutputStream();
                Scanner in = new Scanner(inputStream, String.valueOf(StandardCharsets.UTF_8));
                PrintWriter out = new PrintWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8), true);
                out.println("Hello,Enter BYE to exit.");
                boolean done = false;
                while (!done && in.hasNextLine()){
                    String line = in.nextLine();
                    System.out.println("Echo : " + line);
                    if(line.trim().equals("BYE")){
                        done = true;
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        }
    }
}
