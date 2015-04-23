package core.access.spark;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hadoop.fs.Path;

import core.access.iterator.PartitionIterator;
import core.access.spark.SparkInputFormat.SparkFileSplit;

public class TestSparkInputFormat extends TestCase{

	
	public static class TestSparkFileSplit extends TestCase{
		
		SparkFileSplit sparkSplit;
		
		Path[] files;
		long[] start;
		long[] lengths;
		String[] locations;
		PartitionIterator iterator;
		
		public void setUp(){
			files = new Path[]{new Path("path0"), new Path("path1")};
			start = new long[]{0,0};
			lengths = new long[]{100,100};
			locations = new String[]{"location0", "location1"};
			iterator = new PartitionIterator();
		}
		
		public void testGetIterator(){
			sparkSplit = new SparkFileSplit(files, start, lengths, locations, iterator);
			assertEquals(iterator, sparkSplit.getIterator());
		}
		
		public void testWrite(){
			sparkSplit = new SparkFileSplit(files, start, lengths, locations, iterator);
			DataOutput out = new DataOutputStream(new ByteArrayOutputStream());
			try {
				sparkSplit.write(out);
				assert(true);
			} catch (IOException e) {
				e.printStackTrace();
				assert(false);
			}
		}
		
		public void testReadFields(){
			sparkSplit = new SparkFileSplit(files, start, lengths, locations, iterator);
			ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
			DataOutput out = new DataOutputStream(byteOutputStream);
			try {
				sparkSplit.write(out);				
				SparkFileSplit sparkSplit2 = new SparkFileSplit();				
				byte[] buf = byteOutputStream.toByteArray();
				DataInput in = new DataInputStream(new ByteArrayInputStream(buf));
				sparkSplit2.readFields(in);
				for(int i=0;i<sparkSplit2.getNumPaths();i++){
					assertEquals(files[i], sparkSplit2.getPath(i));
					assertEquals(start[i], sparkSplit2.getOffset(i));
					assertEquals(lengths[i], sparkSplit2.getLength(i));
				}
				assert(true);
				
			} catch (IOException e) {
				e.printStackTrace();
				assert(false);
			}
		}
		
//		public void testReadFields2(){
//			iterator = new RepartitionIterator(101, 102);
//			sparkSplit = new SparkFileSplit(files, start, lengths, locations, iterator);
//			ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
//			DataOutput out = new DataOutputStream(byteOutputStream);
//			try {
//				sparkSplit.write(out);				
//				SparkFileSplit sparkSplit2 = new SparkFileSplit();				
//				byte[] buf = byteOutputStream.toByteArray();
//				DataInput in = new DataInputStream(new ByteArrayInputStream(buf));
//				sparkSplit2.readFields(in);
//				for(int i=0;i<sparkSplit2.getNumPaths();i++){
//					assertEquals(files[i], sparkSplit2.getPath(i));
//					assertEquals(start[i], sparkSplit2.getOffset(i));
//					assertEquals(lengths[i], sparkSplit2.getLength(i));
//					assertEquals(iterator.getClass().getName(), sparkSplit2.getIterator().getClass().getName());
//				}
//				assert(true);
//				
//			} catch (IOException e) {
//				e.printStackTrace();
//				assert(false);
//			}
//		}
	}


	
	SparkInputFormat sparkInputFormat;
	
	public void setUp(){
		sparkInputFormat = new SparkInputFormat();
	}
	
	public void testGetSplits(){
		
	}
	
	public void testCreateRecordReader(){
	}
	
}
