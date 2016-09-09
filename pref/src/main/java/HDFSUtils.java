import com.google.common.io.ByteStreams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;

import java.io.*;


public class HDFSUtils {

    public static String readFile(FileSystem hdfs, String filename) {
        try {
            FSDataInputStream in;

            in = hdfs.open(new Path(filename));
            byte[] bytes = ByteStreams.toByteArray(in);
            in.close();
            return new String(bytes);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Could not read from file:" + filename);
        }
    }

    private static FileSystem fs;

    private static FileSystem getFS(String coreSitePath) {
        if (fs == null) {
            try {
                Configuration conf = new Configuration();
                conf.addResource(new Path(coreSitePath));
                fs = FileSystem.get(conf);
            } catch (Exception e) {
                throw new RuntimeException(
                        "Failed to get the HDFS Filesystem! " + e.getMessage());
            }
        }
        return fs;
    }

    public static FileSystem getFSByHadoopHome(String hadoopHome) {
        return getFS(hadoopHome + "/etc/hadoop/core-site.xml");
    }
}
