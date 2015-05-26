package core.index;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.fs.FileSystem;

import core.index.build.InputReader;
import core.index.key.CartilageIndexKey;
import core.index.robusttree.RobustTreeHs;
import core.utils.ConfUtils;
import core.utils.HDFSUtils;

/**
 * Created by qui on 5/21/15.
 */
public class Sampler {

    public static void main(String[] args) {
        String inputFilename = args[args.length-1];
        System.out.println(inputFilename); // make sure fabric can do this
        String partitionDir = "/user/qui/dodo";
        //String partitionDir = "/user/anil/singletest";
        String propertiesFile = "/Users/qui/Documents/mdindex/conf/cartilage.properties";
        //String propertiesFile = "/home/mdindex/cartilage.properties";

        String fileNum = inputFilename.substring(inputFilename.lastIndexOf("."));

        int bucketSize = 64; // 64 mb
        int scaleFactor = 10000;
        //int scaleFactor = 1;
        int numBuckets = (scaleFactor * 759) / bucketSize + 1;
        double samplingRate = ((double)numBuckets) / (60000 * scaleFactor);
        System.out.println("sampling rate: "+samplingRate);

        RobustTreeHs index = new RobustTreeHs(1);
        index.initBuild(numBuckets);
        CartilageIndexKey key = new CartilageIndexKey('|');
        InputReader r = new InputReader(index, key);
        r.scanWithBlockSampling(inputFilename, samplingRate);

        byte[] sampleBytes = index.serializeSample();
        ConfUtils conf = new ConfUtils(propertiesFile);
        FileSystem fs = HDFSUtils.getFS(conf.getHADOOP_HOME()+"/etc/hadoop/core-site.xml");
        OutputStream out = HDFSUtils.getHDFSOutputStream(fs, partitionDir+"/sample"+fileNum, (short)1, 536870912);
        try {
            out.write(sampleBytes);
            out.flush();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
