package core.index.build;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import core.data.CartilageDatum.CartilageFile;
import core.index.MDIndex;
import core.index.key.CartilageIndexKey;
import core.utils.BinaryUtils;
import core.utils.IOUtils;

public class InputReader {

	int bufferSize = 256 * 1024;
	char newLine = '\n';

	byte[] byteArray, brokenLine;
	ByteBuffer bb;
	int nRead, byteArrayIdx, previous;
	boolean hasLeftover;

	int totalLineSize, lineCount;
	long arrayCopyTime, bucketIdTime;

	MDIndex index;
	CartilageIndexKey key;

	boolean firstPass;

	public InputReader(MDIndex index, CartilageIndexKey key){
		this.index = index;
		this.key = key;
		this.firstPass = true;
		arrayCopyTime = 0;
		bucketIdTime = 0;
	}


	private void initScan(){
		byteArray = new byte[bufferSize];
		brokenLine = null;
		bb = ByteBuffer.wrap(byteArray);
		nRead=0; byteArrayIdx=0; previous=0;
		hasLeftover = false;

		totalLineSize = 0;
		lineCount = 0;
		arrayCopyTime = 0;
		bucketIdTime = 0;
	}

	public void scan(String filename){
		scan(filename, null);
	}

	public void scan(String filename, PartitionWriter writer){
		initScan();

		//long startTime = System.nanoTime();

		FileChannel ch = IOUtils.openFileChannel(new CartilageFile(filename));
		//byte[] line = null;
		try {
			while((nRead = ch.read(bb)) != -1){
				if(nRead==0)
					continue;

				byteArrayIdx = previous = 0;
				processByteBuffer(writer);

			    if(previous < nRead){	// is there a broken line in the end?
			    	brokenLine = BinaryUtils.getBytes(byteArray, previous, nRead-previous);
			    	hasLeftover = true;
			    }
			    bb.clear();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		IOUtils.closeFileChannel(ch);
		firstPass = false;

		//System.out.println("Time taken = "+(double)(System.nanoTime()-startTime)/1E9+" sec");
		System.out.println("Line count = " + lineCount);
		System.out.println("Average line size = " + (double) totalLineSize / lineCount);
		System.out.println("Scan array copy time = "+arrayCopyTime/1E9);
		System.out.println("Scan bucket id time = "+bucketIdTime/1E9);
	}

	public void scanWithBlockSampling(String filename, double samplingRate) {
		initScan();
		FileChannel ch = IOUtils.openFileChannel(new CartilageFile(filename));
		try {
			long position = 0;
			while (Math.random() > samplingRate) {
				position += bufferSize;
			}
			ch.position(position);
			while ((nRead = ch.read(bb)) != -1) {
				if(nRead==0)
					continue;

				byteArrayIdx = previous = 0;
				while (byteArrayIdx < nRead && byteArray[byteArrayIdx] != newLine) {
					byteArrayIdx++;
				}
				previous = ++byteArrayIdx;
				processByteBuffer(null);

				bb.clear();

				while (Math.random() > samplingRate) {
					position += bufferSize;
				}
				ch.position(position);
			};
		} catch (IOException e) {
			e.printStackTrace();
		}
		IOUtils.closeFileChannel(ch);
		firstPass = false;
	}

	private void processByteBuffer(PartitionWriter writer){
		//System.out.println("processing buffer ..");
		for ( ; byteArrayIdx<nRead; byteArrayIdx++ ){
	    	if(byteArray[byteArrayIdx]==newLine){

	    		totalLineSize += byteArrayIdx-previous;
	    		//line = BinaryUtils.getBytes(byteArray, previous, byteArrayIdx-previous);
	    		//key.setBytes(line);
	    		//key.setBytes(byteArray, previous, byteArrayIdx-previous);
	    		if(hasLeftover){
				long startTime = System.nanoTime();
	    			byte[] a = new byte[brokenLine.length + byteArrayIdx-previous];
	    			System.arraycopy(brokenLine, 0, a, 0, brokenLine.length);
	    			System.arraycopy(byteArray, previous, a, brokenLine.length, byteArrayIdx-previous);
	    			key.setBytes(a);
				arrayCopyTime += System.nanoTime() - startTime;

	    			totalLineSize += brokenLine.length;
	    			hasLeftover = false;

	    			if(writer!=null){
					startTime = System.nanoTime();
					String bucketId = index.getBucketId(key).toString();
					bucketIdTime += System.nanoTime() - startTime;
	    				writer.writeToPartition(bucketId, a, 0, a.length);
	    			}
				} else {
					key.setBytes(byteArray, previous, byteArrayIdx-previous);
	    			if(writer!=null){
					long startTime = System.nanoTime();
					String bucketId = index.getBucketId(key).toString();
					bucketIdTime += System.nanoTime() - startTime;
	    				writer.writeToPartition(bucketId, byteArray, previous, byteArrayIdx-previous);
	    			}
	    		}

	    		previous = ++byteArrayIdx;

	    		lineCount++;

	    		if(firstPass)
	    			index.insert(key);
	    	}
	    }
	}

}
