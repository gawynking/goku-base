package com.pgman.goku.util;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FileUtils {

    /**
     * 获取当前目录下的文件列表
     *
     * @param filePath
     * @return
     */
    public static List<String> listFiles(String filePath){

        List<String> list = new ArrayList<String>();
        File file = new File(filePath);

        if(file.exists() && file.isDirectory()){
            File[] files = file.listFiles();
            for(File f :files){
                String path = f.getPath();
                list.add(path);
            }
        }

        return list;
    }


    /**
     * 递归打印所有文件信息
     *
     * @param filePath
     */
    public static void printFiles(String filePath){
        File file = new File(filePath);
        print(file,1);
    }

    public static void print(File file,int level){

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

    /**
     * 创建目录
     *
     * @param filePath
     * @return
     */
    public static boolean mkdir(String filePath){

        File file = new File(filePath);
        return file.mkdirs();

    }


    /**
     * 创建文件
     *
     * @param filePath
     * @return
     */
    public static boolean createFile(String filePath){

        File file = new File(filePath);
        try {
            return file.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return false;
    }


    /**
     * 删除文件
     *
     * @param filePath
     */
    public static boolean deleteFile(String filePath){

        File file = new File(filePath);
        return file.delete();

    }


    /**
     * 重命名文件
     *
     * @param oldFile
     * @param newFile
     * @return
     */
    public static String renameFile(String oldFile,String newFile){

        File srcFile = new File(oldFile);
        File tgtFile = new File(newFile);

        srcFile.renameTo(tgtFile);

        return oldFile;

    }


}
