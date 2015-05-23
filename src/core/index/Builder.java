package core.index;

import core.index.build.InputReader;
import core.index.key.CartilageIndexKey;
import core.index.robusttree.RobustTreeHs;
import core.utils.ConfUtils;
import core.utils.HDFSUtils;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Created by qui on 5/22/15.
 */
public class Builder {

    public static void main(String[] args) {
        double samplingRate = Double.parseDouble(args[args.length-1]);
        int bucketSize = 64; // 64 mb
        //int scaleFactor = 10000;
        int scaleFactor = 1;
        String hdfsDir = "/user/qui/dodo";
        //String hdfsDir = "/user/anil/singletest";
        String propertiesFile = "/Users/qui/Documents/mdindex/conf/cartilage.properties";
        //String propertiesFile = "/home/mdindex/cartilage.properties";

        int numBuckets = (scaleFactor * 759) / bucketSize + 1;
        RobustTreeHs index = new RobustTreeHs(samplingRate);
        index.initBuild(numBuckets);
        CartilageIndexKey key = new CartilageIndexKey('|');
        InputReader r = new InputReader(index, key);

        ConfUtils conf = new ConfUtils(propertiesFile);
        FileSystem fs = HDFSUtils.getFS(conf.getHADOOP_HOME() + "/etc/hadoop/core-site.xml");
        r.scanHDFSDirectory(fs, hdfsDir);
        index.initProbe();

        byte[] indexBytes = index.marshall();
        OutputStream out = HDFSUtils.getHDFSOutputStream(fs, hdfsDir+"/index", (short)1, 536870912);
        try {
            out.write(indexBytes);
            out.flush();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

