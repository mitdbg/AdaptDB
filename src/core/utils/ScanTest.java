package core.utils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;

import core.index.MDIndex;
import core.index.kdtree.KDMedianTree;
import core.index.key.CartilageIndexKey;

public class ScanTest {

	int bufferSize = 256 * 1024;

	FileChannel ch;
	ByteBuffer bb;

	//int[][] offsets;

	private byte[] byteArray,brokenLine;
	boolean hasLeftover;
	char newLine;
	int nRead, byteArrayIdx, previous;


	public void FileChannelScan(String filename, MDIndex index, CartilageIndexKey key){

		byteArray = new byte[bufferSize];
		bb = ByteBuffer.wrap(byteArray);
		previous = 0;
		byteArrayIdx = 0;
		nRead = 0;
		newLine = '\n';

		//offsets = new int[6001215][];
		//int tupleId=0;

		int bucketSize = 1024*1024*1;	// **1mb**

		//CartilageIndexKey2 key = new CartilageIndexKey2('|');
		// SimpleRangeTree t = new SimpleRangeTree();
        // MDIndex t = new KDDTree();
		//t.initBuild(bucketSize); todo(qui): remember to undo this later!

		FileInputStream f;
		try {
			long startTime = System.nanoTime();

			f = new FileInputStream(filename);
			ch = f.getChannel();
            int numBuckets = (int) (ch.size() / bucketSize) + 1;
            System.out.println("num buckets: " + numBuckets);
            index.initBuild(numBuckets);

			//byte[] line = null;
			int totalLineSize=0, lineCount=0;
			while((nRead = ch.read(bb)) != -1){
				if(nRead==0)
					continue;

				byteArrayIdx = previous = 0;
				for ( ; byteArrayIdx<nRead; byteArrayIdx++ ){
			    	if(byteArray[byteArrayIdx]==newLine){

			    		totalLineSize += byteArrayIdx-previous;
			    		//line = BinaryUtils.getBytes(byteArray, previous, byteArrayIdx-previous);
			    		//key.setBytes(line);
			    		key.setBytes(byteArray, previous, byteArrayIdx-previous);
			    		if(hasLeftover){

			    			byte[] a = new byte[brokenLine.length + byteArrayIdx-previous];
			    			System.arraycopy(brokenLine, 0, a, 0, brokenLine.length);
			    			System.arraycopy(byteArray, previous, a, brokenLine.length, byteArrayIdx-previous);
			    			key.setBytes(a);

			    			//line = BinaryUtils.concatenate(brokenLine, (byte[])line);
			    			//key.setBytes(line);

			    			totalLineSize += brokenLine.length;
			    			hasLeftover = false;
			    		}
			    		//System.out.println(new String((byte[])line));
			    		previous = ++byteArrayIdx;

			    		lineCount++;
			    		//offsets[tupleId++] = key.getOffsets();
			    		index.insert(key);
			    	}
			    }
			    if(previous < nRead) {    // is there a broken line in the end?
                    brokenLine = BinaryUtils.getBytes(byteArray, previous, nRead - previous);
                    hasLeftover = true;
                }
			    bb.clear();

			}

			ch.close();

			System.out.println("Time taken = "+(System.nanoTime()-startTime)/1E9+" sec");
			System.out.println("Line count = "+lineCount);
			System.out.println("Average line size = "+(double)totalLineSize/lineCount);

		} catch (FileNotFoundException e) {
			throw new IllegalAccessError("Cannot parse file: "+filename);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

    public Map<String, Integer> countBuckets(String filename, MDIndex index, CartilageIndexKey key) {
        byteArray = new byte[bufferSize];
        bb = ByteBuffer.wrap(byteArray);
        previous = 0;
        byteArrayIdx = 0;
        nRead = 0;
        newLine = '\n';
        Map<String, Integer> bucketCounts = new HashMap<String, Integer>();

        index.initProbe();

        FileInputStream f;
        try {
            long startTime = System.nanoTime();

            f = new FileInputStream(filename);
            ch = f.getChannel();

            //byte[] line = null;
            //int lineCount=0;
            //int totalLineSize=0;

            while((nRead = ch.read(bb)) != -1){
                if(nRead==0)
                    continue;

                byteArrayIdx = previous = 0;
                for ( ; byteArrayIdx<nRead; byteArrayIdx++ ){
                    if(byteArray[byteArrayIdx]==newLine){

                        //totalLineSize += byteArrayIdx-previous;
                        //line = BinaryUtils.getBytes(byteArray, previous, byteArrayIdx-previous);
                        //key.setBytes(line);
                        key.setBytes(byteArray, previous, byteArrayIdx-previous);
                        if(hasLeftover){

                            byte[] a = new byte[brokenLine.length + byteArrayIdx-previous];
                            System.arraycopy(brokenLine, 0, a, 0, brokenLine.length);
                            System.arraycopy(byteArray, previous, a, brokenLine.length, byteArrayIdx-previous);
                            key.setBytes(a);

                            //line = BinaryUtils.concatenate(brokenLine, (byte[])line);
                            //key.setBytes(line);

                            //totalLineSize += brokenLine.length;
                            hasLeftover = false;
                        }
                        //System.out.println(new String((byte[])line));
                        previous = ++byteArrayIdx;

                        //lineCount++;
                        //offsets[tupleId++] = key.getOffsets();
                        String bucketId = (String) index.getBucketId(key);
                        if (!bucketCounts.containsKey(bucketId)) {
                            bucketCounts.put(bucketId, 1);
                        } else {
                            int oldCount = bucketCounts.get(bucketId);
                            bucketCounts.put(bucketId, oldCount + 1);
                        }
                    }
                }
                if(previous < nRead){	// is there a broken line in the end?
                    brokenLine = BinaryUtils.getBytes(byteArray, previous, nRead-previous);
                    hasLeftover = true;
                }
                bb.clear();

            }

            ch.close();
            f.close();

            System.out.println("Time taken = "+(System.nanoTime()-startTime)/1E9+" sec");
            System.out.println(bucketCounts.size());
            System.out.println(bucketCounts);
            return bucketCounts;

        } catch (FileNotFoundException e) {
            throw new IllegalAccessError("Cannot parse file: "+filename);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }



	public static void main(String[] args) {
		ScanTest t = new ScanTest();

		CartilageIndexKey key = new CartilageIndexKey('|');
		MDIndex index = new KDMedianTree(1);

		//t.FileChannelScan("/Users/alekh/Work/Cartilage/support/datasets/tpch_0.01/lineitem.tbl", index, key);
		//t.FileChannelScan("/Users/alekh/Work/Cartilage/support/datasets/scale_1/lineitem.tbl", index, key);
        t.FileChannelScan("test/lineitem.tbl", index, key);
        Map<String, Integer> buckets = t.countBuckets("test/lineitem.tbl", index, key);
        try{
        	TreeUtils.plot(buckets,  "BarChart.jpeg" );
        } catch (IOException ex) {
        	ex.printStackTrace();
        }

	}
}
