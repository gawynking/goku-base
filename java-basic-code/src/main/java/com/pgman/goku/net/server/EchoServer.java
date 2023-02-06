package com.pgman.goku.net.server;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public class EchoServer {
    public static void main(String[] args) throws IOException {

        ServerSocket serverSocket = new ServerSocket(8189);
        Socket socket = serverSocket.accept();

        InputStream inputStream = socket.getInputStream();
        OutputStream outputStream = socket.getOutputStream();
        Scanner in = new Scanner(inputStream, String.valueOf(StandardCharsets.UTF_8));
        PrintWriter out = new PrintWriter(new OutputStreamWriter(outputStream,StandardCharsets.UTF_8), true);
        out.println("Hello,Enter BYE to exit.");

        boolean done = false;
        while (!done && in.hasNextLine()){
            String line = in.nextLine();
            out.println("Echo:"+line);
            if(line.trim().equals("BYE")){
                done = true;
            }
        }

    }
}
