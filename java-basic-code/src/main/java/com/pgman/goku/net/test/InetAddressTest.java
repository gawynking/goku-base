package com.pgman.goku.net.test;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class InetAddressTest {
    public static void main(String[] args) throws UnknownHostException {

        InetAddress[] names = InetAddress.getAllByName("time-a.nist.gov");
        for (InetAddress name :names){
            System.out.println(name);
        }

        InetAddress name = InetAddress.getByName("time-a.nist.gov");
        System.out.println(name);
        byte[] address = name.getAddress();

        InetAddress localHost = InetAddress.getLocalHost();
        System.out.println(localHost);

    }
}
