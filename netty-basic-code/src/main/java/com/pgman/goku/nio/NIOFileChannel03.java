package com.pgman.goku.nio;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * nio示例：应用一个buffer 实现文件复制
 */
public class NIOFileChannel03 {
    public static void main(String[] args) throws Exception {

        FileInputStream fileInputStream = new FileInputStream("d:\\file01.txt");
        FileChannel inputChannel = fileInputStream.getChannel();

        FileOutputStream fileOutputStream = new FileOutputStream("d:\\file02.txt");
        FileChannel outputChannel = fileOutputStream.getChannel();

        ByteBuffer byteBuffer = ByteBuffer.allocate(512);

        while (true) { //循环读取

            /** 这里有一个重要的操作，一定不要忘了
                 public final Buffer clear() {
                    position = 0;
                    limit = capacity;
                    mark = -1;
                    return this;
                }
             */
            byteBuffer.clear(); // 清空buffer，使得下一次可以从头开始同步
            int read = inputChannel.read(byteBuffer); // 将通道内容读入buffer，read放回读入数据大小

            System.out.println("read =" + read);
            if(read == -1) { // 表示读完
                break;
            }

            byteBuffer.flip();
            outputChannel.write(byteBuffer);
        }

        //关闭相关的流
        fileInputStream.close();
        fileOutputStream.close();

    }
}
