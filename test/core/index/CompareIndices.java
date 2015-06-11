package core.index;

import core.index.build.CountingPartitionWriter;
import core.index.build.InputReader;
import core.index.kdtree.KDDTree;
import core.index.kdtree.KDMedianTree;
import core.index.key.CartilageIndexKey;
import core.utils.TreeUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * Created by qui on 3/30/15.
 */
public class CompareIndices {

    //int bucketSize = 64*1024*1024;
    int bucketSize = 1024*1024;
    CountingPartitionWriter writer;

    String inputFilename;

    String localPartitionDir;
    int attributes;

    long fileSize;
    int maxBuckets;

    @Rule
    public TestName name = new TestName();

    @Before
    public void setUp(){
        inputFilename = Settings.tpchPath + "lineitem.tbl";

        localPartitionDir = Settings.localPartitionDir;

        attributes = 16;
        writer = new CountingPartitionWriter(localPartitionDir, Settings.cartilageConf);

        File f = new File(inputFilename);
        fileSize = f.length();
        maxBuckets = (int) (fileSize / bucketSize) + 1;
        System.out.println("Max buckets: "+maxBuckets);
    }

    @After
    public void calculateStats() {
        Map<String, Integer> counts = writer.getCounts();
        int n = counts.size();

        // calculate mean
        double sum = 0.0;
        for (Integer count : counts.values()) {
            sum += count;
        }
        double mean = sum / n;

        // calculate st dev
        double sqErr = 0.0;
        for (Integer count : counts.values()) {
            double err = count - mean;
            sqErr += err * err;
        }
        double stdev = Math.sqrt(sqErr / n);

        System.out.println("Number of buckets: "+n);
        System.out.println("Mean size: "+mean);
        System.out.println("Size st dev: "+stdev);
        try{
            TreeUtils.plot(counts, String.format("BarChart_%s.jpeg", name.getMethodName()));
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void testBasicKDTree() {
        MDIndex index = new KDDTree();
        CartilageIndexKey key = new CartilageIndexKey('|');
        InputReader r = new InputReader(index, key);

        long startTime = System.nanoTime();
        index.initBuild(maxBuckets);
        r.scan(inputFilename);
        index.initProbe();
        double time1 = (double)(System.nanoTime()-startTime)/1E9;
        System.out.println("Index Build Time = "+time1+" sec");

        r.scan(inputFilename, writer);
    }

    @Test
    public void testKDMedianTree() {
        MDIndex index = new KDMedianTree(1);
        CartilageIndexKey key = new CartilageIndexKey('|');
        InputReader r = new InputReader(index, key);

        Runtime runtime = Runtime.getRuntime();
        double samplingRate = runtime.freeMemory() / (2.0 * fileSize);
        System.out.println("Sampling rate: "+samplingRate);

        long startTime = System.nanoTime();
        index.initBuild(maxBuckets);
        r.scan(inputFilename);
        index.initProbe();
        double time1 = (double)(System.nanoTime()-startTime)/1E9;
        System.out.println("Index Build Time = "+time1+" sec");

        r.scan(inputFilename, writer);
    }
}
