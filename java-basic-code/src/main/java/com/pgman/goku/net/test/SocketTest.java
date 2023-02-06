package com.pgman.goku.net.test;

import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public class SocketTest {
    public static void main(String[] args) throws IOException {
        Socket s = new Socket("time-a.nist.gov", 13);
        s.setSoTimeout(10000);
        Scanner in = new Scanner(s.getInputStream(), String.valueOf(StandardCharsets.UTF_8));
        while (in.hasNext()){
            String line = in.nextLine();
            System.out.println(line);
        }

    }
}
