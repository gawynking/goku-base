package com.pgman.goku.test;

import com.pgman.goku.util.StringUtils;

public class StringUtilTest {

    public static void main(String[] args) {

        System.out.println(StringUtils.substr("chavinking",3,19));
        System.out.println(StringUtils.substr("chavinking",6));
        System.out.println(StringUtils.instr("chavinking","vin"));

        String nullStr = "   ";
        System.out.println(StringUtils.isEmpty(nullStr));

        System.out.println(StringUtils.encryptString("chavinking","oracle"));

        System.out.println(StringUtils.decryptString("7F7648A7C0CC01A604B8C1A14A5555FB","oracle"));

        System.out.println(StringUtils.getUUID());

        System.out.println(StringUtils.concat("aaa","bbb","ccc"));

        System.out.println(StringUtils.concatWS(",","aaa","bbb","ccc"));

        System.out.println(StringUtils.equals("aaa","aaa",false));

        System.out.println("aaa".concat("bbb"));

        System.out.println(StringUtils.trim("  chavin king    "));

        System.out.println(StringUtils.reverse("chavinking"));



        String str = "pgsqlpgsqlpgsqlasdfjkljfgsipgsqlpgsqlpgsql";
        System.out.println(StringUtils.trim(str,"pgsql"));

    }
}
