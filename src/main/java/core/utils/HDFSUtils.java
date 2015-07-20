package core.utils;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.List;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;


public class HDFSUtils {

	public static final int WRITE_BUFFER_SIZE = 64 * 1024 * 1024;

	public static class HDFSData{
		protected static FileSystem fs;
		protected String hadoopConfDir;
		public HDFSData(FileSystem fs){
			if(HDFSData.fs == null)
				HDFSData.fs = fs;
		}
		public HDFSData(String hadoopConfDir){
			if(fs == null){
				Configuration conf = new Configuration();
		        conf.addResource(new Path(hadoopConfDir+"/core-site.xml"));
		        conf.addResource(new Path(hadoopConfDir+"/hdfs-site.xml"));
				try {
					fs =  FileSystem.get(conf);
				} catch (IOException e) {
					throw new RuntimeException("could not get the filesystem: "+e.getMessage());
				}
			}
			this.hadoopConfDir = hadoopConfDir;
		}
		protected void delete(Path dataPath){
			try {
				fs.delete(dataPath, true);
			} catch (IOException e) {
				throw new RuntimeException("could not delete the data path: "+dataPath+", "+e.getMessage());
			}
		}
	}

	public static class HDFSDir extends HDFSData{
		private Path dataPath;
		public HDFSDir(String hadoopConfDir, String dataPath) {
			super(hadoopConfDir);
			this.dataPath = new Path(dataPath);
		}
		public List<HDFSFile> getFiles(){
			try {
				List<HDFSFile> files = Lists.newArrayList();
				for(FileStatus fileStatus: fs.listStatus(dataPath))
					files.add(new HDFSFile(hadoopConfDir, fileStatus));
				return files;
			} catch (IOException e) {
				throw new RuntimeException("failed to list the files in directory "+dataPath+" ! "+e.getMessage());
			}
		}
		public void delete(){
			super.delete(dataPath);
		}
	}

	public static class HDFSFile extends HDFSData{
		private FileStatus status;
		public HDFSFile(String hadoopConfDir, FileStatus status) {
			super(hadoopConfDir);
			this.status = status;
		}
		public HDFSFile(){
			super(HDFSData.fs);
		}
		public boolean isCorrupted(){
			try {
				for(BlockLocation blk: fs.getFileBlockLocations(status, 0, status.getLen())){
					if(blk.isCorrupt())
						return true;
				}
				return false;
			} catch (IOException e) {
				throw new RuntimeException("failed to get the blocks locations of file "+status.getPath()+" ! "+e.getMessage());
			}
		}
		public void delete(){
			super.delete(status.getPath());
		}
		public List<HDFSFile> getSiblingFiles(String prefix){
			final String siblingPrefix = prefix;
			String parentDir = FilenameUtils.getPath(status.getPath().getName());
			try {
				FileStatus[] siblingStatuses = fs.listStatus(new Path(parentDir),
						new PathFilter() {
							@Override
							public boolean accept(Path path) {
								return path.getName().startsWith(siblingPrefix);
							}
						});
				List<HDFSFile> siblingFiles = Lists.newArrayList();
				for(FileStatus status: siblingStatuses)
					siblingFiles.add(new HDFSFile(hadoopConfDir, status));
				return siblingFiles;
			} catch (IOException e) {
				throw new RuntimeException("Failed to get the siblings of "+status.getPath()+", "+e.getMessage());
			}
		}
		public String getPath(){
			return status.getPath().getName();
		}
		public void increaseReplication(int increment){
			Path p = status.getPath();
			try {
				fs.setReplication(p, (short)(status.getReplication()+increment));
			} catch (IOException e) {
				throw new RuntimeException("Failed to increase the replication of "+getPath()+" by "+increment+", "+e.getMessage());
			}
		}
		public byte[] getBytes(){
			return null;	// TODO
		}
		public void putBytes(byte[] bytes){
			// TODO
		}
	}









	private static FileSystem fs;

	public static FileSystem getFSByHadoopHome(String hadoopHome){
		return getFS(hadoopHome+"/etc/hadoop/core-site.xml");
	}

	public static FileSystem getFS(String coreSitePath){
		if(fs == null){
			try {
				Configuration conf = new Configuration();
				conf.addResource(new Path(coreSitePath));
				fs = FileSystem.get(conf);
				//URI uri = new URI(conf.get("fs.default.name"));
				//fs = FileSystem.get(uri, conf, "alekh");
			} catch (Exception e) {
				throw new RuntimeException("Failed to get the HDFS Filesystem! "+e.getMessage());
			}
		}
		return fs;
	}

	public static OutputStream getHDFSOutputStream(FileSystem hdfs, String filename, short replication, int bufferSize) {
		try {
			//FileSystem hdfs = getFS();
			Path path = new Path(filename);
			if(hdfs.exists(path))
				return new BufferedOutputStream(hdfs.append(path, replication), bufferSize);
			else
				return new BufferedOutputStream(hdfs.create(path, replication), bufferSize);
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Could not open the file:"+filename);
		} catch (IOException e) {
			throw new RuntimeException("Could not open the file:"+filename+", "+e.getMessage());
		}
	}
	
	public static InputStream getHDFSInputStream(FileSystem hdfs, String filename) {
		try {
			return hdfs.open(new Path(filename));
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("Could not read from file:"+filename);
		}
	}

	public static void writeFile(FileSystem hdfs, String filename, short replication, byte[] bytes, int offset, int length, boolean append) {
		try {
			Path path = new Path(filename);
			FSDataOutputStream os;
			if(append && hdfs.exists(path))
				os = hdfs.append(path);
			else
				os = hdfs.create(new Path(filename), replication);
			os.write(bytes, offset, length);
			os.flush();
			os.close();
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("Could not write to file:"+filename);
		}
	}

	public static byte[] readFile(FileSystem hdfs, String filename) {
		try {
			FSDataInputStream in = hdfs.open(new Path(filename));
			byte[] bytes = ByteStreams.toByteArray(in);
			in.close();
			return bytes;	
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("Could not read from file:"+filename);
		}
	}

	public static void closeHDFSInputStream(FSDataInputStream fsInputStream){
		try {
			fsInputStream.close();
		} catch (IOException e) {
			throw new RuntimeException("Cannot close file. "+e.getMessage());
		}
	}

	public static void createFile(String hadoopHome, String path, short replication){
		try {
			FSDataOutputStream os = getFSByHadoopHome(hadoopHome).create(new Path(path), replication);
			os.close();
		} catch (IOException e) {
			throw new RuntimeException("Failed to create the file: "+path+", "+e.getMessage());
		}
	}
	
	public static boolean tryCreateFile(FileSystem fs, String path){		
		try {
			return fs.createNewFile(new Path(path));
		} catch (IOException e1) {
			//e1.printStackTrace();
			return false;
		}
	}

	public static void safeCreateFile(String hadoopHome, String path, short replication){
		try {
			FileSystem fs = getFSByHadoopHome(hadoopHome);
			if(!fs.exists(new Path(path))) {
				FSDataOutputStream os = fs.create(new Path(path), replication);
				os.close();
			}
		} catch (IOException e) {
		}
	}

	public static List<String> readHDFSLines(String hadoopHome, String filename){
		try {
			Configuration conf = new Configuration();
			conf.addResource(new Path(hadoopHome+"/etc/hadoop/core-site.xml"));
			FileSystem fs = FileSystem.get(conf);
			Path path = new Path(filename);
			if(!fs.exists(path))
				return Lists.newArrayList();

			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
        	String line;
        	List<String> lines = Lists.newArrayList();
			while ((line=br.readLine()) != null)
				lines.add(line);
			br.close();

			return lines;
		} catch (IOException e) {
			throw new RuntimeException("could not read the inputstream!");
		}
	}
	
	public static List<String> readHDFSLines(FileSystem fs, String filename){
		try {
			Path path = new Path(filename);
			if(!fs.exists(path))
				return Lists.newArrayList();

			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
        	String line;
        	List<String> lines = Lists.newArrayList();
			while ((line=br.readLine()) != null)
				lines.add(line);
			br.close();

			return lines;
		} catch (IOException e) {
			throw new RuntimeException("could not read the inputstream!");
		}
	}

	public static void appendLine(String hadoopHome, String filepath, String line){
		try {
			FSDataOutputStream fout = getFSByHadoopHome(hadoopHome).append(new Path(filepath));
			fout.write(line.getBytes());
			fout.write('\n');
			fout.close();
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("could not append to file: "+filepath);
		}
	}

	public static void writeHDFSLines(String hadoopHome, String filename, List<String> lines){
		try {
			Configuration conf = new Configuration();
			conf.addResource(new Path(hadoopHome+"/etc/hadoop/core-site.xml"));
			FileSystem fs = FileSystem.get(conf);
			Path path = new Path(filename);
			if(fs.exists(path))
				fs.delete(path,true);

			FSDataOutputStream fout = fs.create(path);
			for(String line: lines){
				fout.write(line.getBytes());
				fout.write('\n');
			}

			fout.close();
			fs.close();

		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("failed to write the hdfs file: "+filename+", "+e.getMessage());
		}
	}
	
	public static OutputStream getOutputStreamWithRetry(FileSystem fs, String filename, long retryIntervalMs, int maxRetryCount){
		return getOutputStreamWithRetry(fs, filename, (short)3, retryIntervalMs, maxRetryCount);
	}
	
	public static OutputStream getOutputStreamWithRetry(FileSystem fs, String filename, short replication, long retryIntervalMs, int maxRetryCount){
		FSDataOutputStream fout = null;
		Path path = new Path(filename);
		
		int retryCount = 0;
		while(retryCount < maxRetryCount){
			try{
				if(!fs.exists(path))
					fout = fs.create(path, replication);
				else
					fout = fs.append(path);
				break;
			} catch (IOException e) {
				try {
					Thread.sleep(retryIntervalMs);
				} catch (InterruptedException i) {
					throw new RuntimeException("Failed to sleep the thread: "+i.getMessage());
				}
			}
		}
		if(fout==null)
			throw new RuntimeException("failed to write the hdfs file: "+filename);
		else
			return fout;
	}
	
//	public static void writeFileWithRetry(FileSystem fs, String filename, List<String> lines, long retryIntervalMs, int maxRetryCount){
//		try {
//			OutputStream fout = getOutputStreamWithRetry(fs, filename, retryIntervalMs, maxRetryCount);
//			for(String line: lines){
//				fout.write(line.getBytes());
//				fout.write('\n');
//			}
//			fout.close();
//		} catch (IOException e) {
//			e.printStackTrace();
//			throw new RuntimeException("failed to write the hdfs file: "+filename+", "+e.getMessage());
//		}
//	}

	public static boolean tryDelete(FileSystem hdfs, String filename, boolean recursive){
		try {
			return hdfs.delete(new Path(filename), recursive);			
		} catch (IOException e) {
			return false;
		}
	}
	
	public static void deleteFile(FileSystem hdfs, String filename, boolean recursive){
		try {
			hdfs.delete(new Path(filename), recursive);
		} catch (IOException e) {
			System.out.println("Failed to delete: "+filename);
			e.printStackTrace();
		}
	}

	public static List<String> getDataNodes(String hadoopHome){
		List<String> lines = Lists.newArrayList();
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(hadoopHome+"/conf/slaves"));
		} catch (FileNotFoundException e) {
			try {
				reader = new BufferedReader(new FileReader(hadoopHome+"/etc/hadoop/slaves"));
			} catch (FileNotFoundException e1) {
				throw new RuntimeException("Failed to read the slaves file: "+e.getMessage());
			}
		}
		finally{
			try {
				String line;
				while((line=reader.readLine())!=null)
					lines.add(line);
				reader.close();
			} catch (IOException e) {
				throw new RuntimeException("Failed to read the slaves file: "+e.getMessage());
			}
		}
		return lines;
	}
}
