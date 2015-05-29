package core.index;

import core.index.build.InputReader;
import core.index.kdtree.KDMedianTree;
import core.index.key.CartilageIndexKey;
import core.index.key.CartilageIndexKeySet;
import core.index.robusttree.RobustTreeHs;
import core.utils.ConfUtils;
import core.utils.HDFSUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

/**
 * Created by qui on 5/22/15.
 */
public class Builder {

    public static void main(String[] args) {
        double samplingRate = Double.parseDouble(args[args.length-1]);
        int bucketSize = 64; // 64 mb
        int scaleFactor = 1000;
        //int scaleFactor = 1;
        //String hdfsDir = "/user/qui/dodo";
        String hdfsDir = "/user/anil/one";
        //String propertiesFile = "/Users/qui/Documents/mdindex/conf/cartilage.properties";
        String propertiesFile = "/home/mdindex/cartilage.properties";

        int numBuckets = (scaleFactor * 759) / bucketSize + 1;
        RobustTreeHs index = new RobustTreeHs(samplingRate);
        //KDMedianTree index = new KDMedianTree(samplingRate);
        index.initBuild(numBuckets);
        CartilageIndexKey key = new CartilageIndexKey('|');
        InputReader r = new InputReader(index, key);

        ConfUtils conf = new ConfUtils(propertiesFile);
        FileSystem fs = HDFSUtils.getFS(conf.getHADOOP_HOME() + "/etc/hadoop/core-site.xml");
        long startTime = System.nanoTime();

        // read all the sample files and put them into the sample key set
        CartilageIndexKeySet sample = new CartilageIndexKeySet();
        try {
            RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path(hdfsDir), false);
            while (files.hasNext()) {
                String path = files.next().getPath().toString();
                byte[] bytes = HDFSUtils.readFile(fs, path);
                sample.unmarshall(bytes);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        index.loadSample(sample);
        System.out.println("BUILD: scan sample time = " + ((System.nanoTime() - startTime) / 1E9));
        startTime = System.nanoTime();
        index.initProbe();
        System.out.println("BUILD: index building time = " + ((System.nanoTime() - startTime) / 1E9));
        System.out.println("Types: "+ Arrays.toString(index.dimensionTypes));

        startTime = System.nanoTime();
        byte[] indexBytes = index.marshall();
        OutputStream out = HDFSUtils.getHDFSOutputStream(fs, hdfsDir+"/index", (short)1, 536870912);
        try {
            out.write(indexBytes);
            out.flush();
            out.close();
            System.out.println("BUILD: index writing time = " + ((System.nanoTime() - startTime) / 1E9));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

