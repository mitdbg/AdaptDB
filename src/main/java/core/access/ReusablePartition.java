package core.access;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

import core.utils.BufferManager;
import core.utils.IOUtils;

public class ReusablePartition implements Cloneable, Serializable {

	private static final long serialVersionUID = 1L;
	
	public static int V_MAX_INT = 1024*1024*50;

	public static enum State {ORIG,NEW,MODIFIED};
	State state;

	protected String path;
	protected BufferManager buffMgr;
	protected ByteBuffer bytes;
	protected int offset;
	protected int recordCount;

	protected int partitionId;
	
	protected boolean nextBytesReturned = false;

	/**
	 * Create an existing  partition object.
	 * @param pathAndPartitionId
	 */
	public ReusablePartition(String pathAndPartitionId){
		this(FilenameUtils.getPath(pathAndPartitionId),
				Integer.parseInt(FilenameUtils.getBaseName(pathAndPartitionId)));
	}

	public ReusablePartition(String path, int partitionId){
		this.path = path;
		this.partitionId = partitionId;
		this.state = State.ORIG;
		this.offset = 0;
		this.recordCount = 0;
	}

	/* hack: currently the clone object is the standard Partition object, i.e. we do not support reusing for writing.
	 */
	
	public Partition clone() {
		Partition p = new Partition(path, partitionId);
		p.bytes = new byte[1024];
		p.state = Partition.State.NEW;
        return p;
    }
	
	public void setPathAndPartitionId(String pathAndPartitionId){
		this.path = FilenameUtils.getPath(pathAndPartitionId);
		this.partitionId = Integer.parseInt(FilenameUtils.getBaseName(pathAndPartitionId));
		this.offset = 0;
	}

	public void setPartitionId(int partitionId){
		this.partitionId = partitionId;
	}

	public int getPartitionId(){
		return this.partitionId;
	}

	public String getPath() { return path; }


	public boolean load(){
		if(path==null || path.equals(""))
			return false;
		if(!path.startsWith("/"))
			path = "/"+path;
		InputStream in = IOUtils.getFileInputStream(path+"/"+partitionId);
		bytes = buffMgr.load(in);
		IOUtils.closeInputStream(in);
		offset = bytes.position();
		return true;	// load the physical block for this partition
	}

	public void write(byte[] source, int offset, int length){
		if (this.offset+length >= bytes.limit()) {
			store(true);
			this.offset = 0;
			bytes.clear();
		}
		
		bytes.put(source, offset, length);
		this.offset += length;
		
		bytes.put((byte)'\n');
		this.offset++;
		
		this.recordCount++;
	}

	public void store(boolean append){
		String storePath = path + "/" + partitionId;
		OutputStream os = IOUtils.getFileOutputStream(storePath);
		buffMgr.store(os, bytes);
		IOUtils.closeOutputStream(os);
	}

	public void drop(){
		FileUtils.deleteQuietly(new File(path));
	}

	public ByteBuffer getBytes(){
		if(bytes==null)
			load();
		return bytes;
	}
	
	public ByteBuffer getNextBytes(){
		ByteBuffer r = nextBytesReturned ? null : getBytes();
		nextBytesReturned = true;
		return r;
	}

	@Override
	public boolean equals(Object p){
		return ((ReusablePartition)p).path.equals(path) && ((ReusablePartition)p).partitionId==partitionId;
	}

	@Override
	public int hashCode(){
		return (path+partitionId).hashCode();
	}

	public int getRecordCount(){
		return this.recordCount;	// returns the number of records written to this partition
	}
}
