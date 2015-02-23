package core.index.build;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import core.data.CartilageDatum.CartilageFile;
import core.index.MDIndex;
import core.index.key.CartilageIndexKey;
import core.utils.BinaryUtils;
import core.utils.IOUtils;

public class ReplicatedInputReader {

	int bufferSize = 256 * 1024;
	char newLine = '\n';
	
	byte[] byteArray, brokenLine;
	ByteBuffer bb;
	int nRead, byteArrayIdx, previous;	
	boolean hasLeftover;
	
	int totalLineSize, lineCount;
	
	MDIndex[] indexes;
	CartilageIndexKey[] keys;
	
	boolean firstPass;
	
	public ReplicatedInputReader(MDIndex[] indexes, CartilageIndexKey[] keys){
		this.indexes = indexes;
		this.keys = keys;
		this.firstPass = true;
	}
	
	
	private void initScan(){
		byteArray = new byte[bufferSize];
		brokenLine = null;
		bb = ByteBuffer.wrap(byteArray);
		nRead=0; byteArrayIdx=0; previous=0;	
		hasLeftover = false;
		
		totalLineSize = 0;
		lineCount = 0;
	}
	
	public void scan(String filename){
		scan(filename, null);
	}
	
	public void scan(String filename, PartitionWriter[] writers){
		initScan();
		
		//long startTime = System.nanoTime();
		
		FileChannel ch = IOUtils.openFileChannel(new CartilageFile(filename));
		//byte[] line = null;
		try {
			while((nRead = ch.read(bb)) != -1){
				if(nRead==0)
					continue;
				
				byteArrayIdx = previous = 0;
				processByteBuffer(writers);
				
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
		System.out.println("Line count = "+lineCount);
		System.out.println("Average line size = "+(double)totalLineSize/lineCount);
	}
	
	private void processByteBuffer(PartitionWriter[] writers){
		//System.out.println("processing buffer ..");
		for ( ; byteArrayIdx<nRead; byteArrayIdx++ ){
	    	if(byteArray[byteArrayIdx]==newLine){
	    		
	    		totalLineSize += byteArrayIdx-previous;
	    		//line = BinaryUtils.getBytes(byteArray, previous, byteArrayIdx-previous);
	    		//key.setBytes(line);
	    		//key.setBytes(byteArray, previous, byteArrayIdx-previous);
	    		if(hasLeftover){
	    			
	    			byte[] a = new byte[brokenLine.length + byteArrayIdx-previous];
	    			System.arraycopy(brokenLine, 0, a, 0, brokenLine.length);
	    			System.arraycopy(byteArray, previous, a, brokenLine.length, byteArrayIdx-previous);
	    			//key.setBytes(a);
	    			
	    			//line = BinaryUtils.concatenate(brokenLine, (byte[])line);
	    			//key.setBytes(line);
	    			
	    			totalLineSize += brokenLine.length;
	    			hasLeftover = false;
	    			
	    			for(int i=0; i<indexes.length; i++){
	    				keys[i].setBytes(a);
	    				if(writers!=null)
	    					writers[i].writeToPartition((String)indexes[i].getBucketId(keys[i]), a, 0, a.length);
	    			}
	    		}
	    		else{
	    			for(int i=0; i<indexes.length; i++){
		    			keys[i].setBytes(byteArray, previous, byteArrayIdx-previous);
		    			if(writers!=null)
		    				writers[i].writeToPartition((String)indexes[i].getBucketId(keys[i]), byteArray, previous, byteArrayIdx-previous);
	    			}
	    		}
	    		
	    		//System.out.println(new String((byte[])line));
	    		previous = ++byteArrayIdx;
	    		
	    		lineCount++;
	    		//if(writer!=null && lineCount%20 == 0)
	    		//	System.out.println("processed "+lineCount+" lines");
	    		//offsets[tupleId++] = key.getOffsets();
	    		
	    		if(firstPass)
	    			for(int i=0;i<indexes.length;i++)
	    				indexes[i].insert(keys[i]);
	    	}
	    }
	}
	
}
