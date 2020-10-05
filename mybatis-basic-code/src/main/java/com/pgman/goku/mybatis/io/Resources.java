package com.pgman.goku.mybatis.io;

import java.io.InputStream;

public class Resources {

    /**
     * 根据传入的文件路径通过类加载器反射方式加载文件
     * @param filePath
     * @return
     */
    public static InputStream getResourceAsStream(String filePath){
        return Resources.class.getClassLoader().getResourceAsStream(filePath);
    }

}
