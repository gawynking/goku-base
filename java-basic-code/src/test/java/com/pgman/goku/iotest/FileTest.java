package com.pgman.goku.iotest;

import com.pgman.goku.util.FileUtils;
import com.pgman.goku.util.IOUtils;
import org.junit.Test;

import java.io.File;
import java.util.List;

public class FileTest {

    @Test
    public void testCopyFile(){

        String in = "E:\\test\\1.mkv";
        String out = "E:\\test\\5.mkv";

        long start = System.currentTimeMillis();
        IOUtils.copy(in,out);
        long end = System.currentTimeMillis();

        System.out.println(end - start);

    }



    @Test
    public void test01(){

        List<String> list = FileUtils.listFiles("E:\\学习资料\\Java教程\\01 语言基础+高级·");

        for(String f :list)
        System.out.println(f);

    }

    @Test
    public void test02(){

        File file = new File("E:\\学习资料\\Java教程\\01 语言基础+高级·");

        if(file.exists()){

            if(file.isDirectory()){
                System.out.println("ddddddd");
            }

            System.out.println(1);
        }

    }


    @Test
    public void test03(){

//        File file = new File("E:\\学习资料\\Java教程\\01 语言基础+高级·");
//        print(file,1);

        FileUtils.printFiles("E:\\学习资料\\Java教程\\01 语言基础+高级·");

    }


    public void print(File file,int level){

        if(file.exists()){
            if(file.isDirectory()){
                for (int i = 1; i <level; i++) {
                    System.out.print("\t");
                }
                System.out.println(file.getPath());

                File[] files = file.listFiles();
                for(File f :files){
                    print(f,level+1);
                }
            }else {
                for(int i=1; i<level; i++){
                    System.out.print("\t");
                }
                System.out.println(file.getName());
            }

        }

    }












}
