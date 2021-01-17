package com.pgman.goku.util;

import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;

/**
 * 用于加密密码
 */
public class BCryptPasswordEncoderUtils {

    private static BCryptPasswordEncoder bCryptPasswordEncoder = new BCryptPasswordEncoder();

    public static String encodePassword(String password) {
        return bCryptPasswordEncoder.encode(password);
    }

    public static void main(String[] args) {

        String password = "admin";
        String pwd = encodePassword(password);
        System.out.println(pwd);

    }

}
