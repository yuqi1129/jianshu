package com.yuqi.jianshu.lucenetest.filetest;

import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Author yuqi
 * Time 19/7/19
 **/
public class FileChannelTest {


    //See lecene at NIOFSDirectory
    public static final AtomicInteger position = new AtomicInteger(0);

    public static void main(String[] args) {
        File file = new File("test.txt");


        try {
            FileWriter fileWriter = new FileWriter(file);
            IOUtils.write("Hello, this is a test", fileWriter);
            fileWriter.flush();

            File f1 = new File("test.txt");
            CountDownLatch c1 = new CountDownLatch(10);
            CountDownLatch c2 = new CountDownLatch(10);

            FileChannel fileChannel = new RandomAccessFile(f1, "rw").getChannel();


            //RandomAccessFile raf = new RandomAccessFile(file, "rw");
            //raf.s
            for (int i = 0; i < 10; i++) {


                FileChannelReader fileChannelReader =
                        new FileChannelReader(fileChannel, i, c1);
                new Thread(fileChannelReader).start();
            }

            c1.await();

            File f2 = new File("test.txt");
            for (int i = 0; i < 10; i++) {
                CommonFileReader commonFileReader = new CommonFileReader(f2, c2);
                new Thread(commonFileReader).start();
            }

            c2.await();


        } catch (Exception e) {
            e.printStackTrace();
        }





    }

    public static class FileChannelReader implements Runnable {

        private FileChannel fileChannel;
        private int position;
        private CountDownLatch countDownLatch;
        //


        public FileChannelReader(FileChannel fileChannel, int position, CountDownLatch countDownLatch) {
            this.fileChannel = fileChannel;
            this.position = position;
            this.countDownLatch = countDownLatch;
        }

        public void run() {
            try {
                byte[] bytes = new byte[1];
                ByteBuffer b = ByteBuffer.wrap(bytes);
                //fileChannel.read(new ByteBuffer[]{b}, 0, 1);

                fileChannel.read(b, 0);

                System.out.println(Thread.currentThread() + " read data:'" + new String(bytes) + "'");
                countDownLatch.countDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class CommonFileReader implements Runnable {

        private File file;
        private CountDownLatch countDownLatch;

        public CommonFileReader(File file, CountDownLatch countDownLatch) {
            this.file = file;
            this.countDownLatch = countDownLatch;
        }

        public void run() {
            try {
                byte[] f = new byte[1];
                IOUtils.read(new FileInputStream(file), f, 1, 1);
                System.out.println(Thread.currentThread() + " read data:'" + new String(f) + "'");
                countDownLatch.countDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
