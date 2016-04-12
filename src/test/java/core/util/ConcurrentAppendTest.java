package core.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;

/**
 * Created by ylu on 3/8/16.
 */

class HelloThread extends Thread {

    String content;
    int id;
    HelloThread(String c, int i){
        content = c;
        id = i;
    }
    public void run() {
        String uri = "hdfs://localhost:9000/test.txt";

        // instantiate a configuration class
        Configuration conf = new Configuration();
        //conf.setBoolean("dfs.support.append", true);
        // get a HDFS filesystem instance
        FileSystem fs = null;
        try {
            fs = FileSystem.get(URI.create(uri), conf);
        } catch (IOException e) {
            e.printStackTrace();
        }


        FSDataOutputStream fsout = null;
        try {
            fsout = fs.append(new Path(uri));
            for(int i = 0; i <1000; i ++){
                //System.out.println(id);
                fsout.writeChars(content);
            }

            // wrap the outputstream with a writer

            //fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}

public class ConcurrentAppendTest {


    public static void main(String[] args) {

        HelloThread t1 = new HelloThread("hello\n", 1);
        HelloThread t2 = new HelloThread("world\n", 2);
        t1.start();
        t2.start();

        try {
            t1.join();
            t2.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

}
