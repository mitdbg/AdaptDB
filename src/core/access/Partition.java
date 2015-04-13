package core.access;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

import core.utils.ArrayUtils;
import core.utils.IOUtils;


public class Partition {
	
	public static enum State {ORIG,NEW,MODIFIED};
	State state;
	
	protected String path;
	protected byte[] bytes;
	protected int offset;
	
	public int[] lineage;
	
	/**
	 * Create an existing  partition object.
	 * @param path
	 */
	public Partition(String path){
		this.path = path;
		this.state = State.ORIG;
		this.offset = 0;
		this.lineage = ArrayUtils.splitWithException("_", FilenameUtils.getBaseName(path));
	}
	

	/**
	 * Create a new partition
	 * 
	 * @param childId
	 * @return
	 */
	public Partition createChild(int childId){
		Partition p = new Partition(path+"_"+childId);
		p.bytes = new byte[bytes.length];	// child cannot have more bytes than parent
		//p.bytes = new byte[8*1024*1024];	// child cannot have more than 8m bytes
		p.state = State.NEW;
		return p;
	}
	
	public void setAsParent(Partition p){
		for(int i=0;i<p.lineage.length;i++)
			lineage[i] = p.lineage[i];
		this.offset = 0;
	}
	
	public void setPath(String path){
		this.path = path;
		this.lineage = ArrayUtils.splitWithException("_", FilenameUtils.getBaseName(path));
		this.offset = 0;
	}
	
	public boolean load(){
		if(path==null || path.equals(""))
			return false;
		bytes = IOUtils.readByteArray(path);
		offset = bytes.length;
		return true;	// load the physical block for this partition 
	}
	
	public void write(byte[] source, int offset, int length){
		if(offset+length > source.length || this.offset+length>bytes.length)
			System.out.println("somthing is wrong!");
		System.arraycopy(source, offset, bytes, this.offset, length);
		this.offset += length;
		bytes[this.offset] = '\n';
		this.offset++;
	}
	
	public void store(boolean append){
		String storePath = FilenameUtils.getFullPath(path) + ArrayUtils.join("_", lineage);
		IOUtils.writeByteArray(storePath, bytes, 0, offset, append);
	}
	
	public void drop(){
		FileUtils.deleteQuietly(new File(path));
	}
	
	public byte[] getBytes(){
		if(bytes==null)
			load();
		return bytes;
	}
	
	public int getSize(){
		return offset;
	}
	
	public boolean equals(Object p){
		return ((Partition)p).path.equals(path);
	}
	
	public int hashCode(){
		return path.hashCode();
	}
}
