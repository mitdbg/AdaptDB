package core.utils;

import java.io.IOException;
import java.io.OutputStream;

import junit.framework.TestCase;

import org.apache.hadoop.fs.FileSystem;
import org.junit.Assert;

import perf.benchmark.BenchmarkSettings;

public class TestHDFSBufferedOutputStream extends TestCase {
	FileSystem fs;

	String testFile;

	ConfUtils cfg;

	@Override
	public void setUp(){
		this.cfg = new ConfUtils(BenchmarkSettings.conf);
		this.testFile =  "/user/mdindex/testOutputStream.txt";
		this.fs = HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME());
		HDFSUtils.deleteFile(this.fs, this.testFile, false);
	}

	public void testAppends() {
		String outputString = "";
		String testLine = "Lorem ipsum dolor sit amet\n";
		OutputStream os = HDFSUtils.getBufferedHDFSOutputStream(this.fs, this.testFile, this.cfg.getHDFS_REPLICATION_FACTOR(), 10000);

		try {
			for (int i=0; i<10; i++) {
				os.write(testLine.getBytes());
				outputString += testLine;
			}
			os.close();
		} catch (IOException e) {
			Assert.assertTrue(false);
		}

		byte[] fileContentBuffer = HDFSUtils.readFile(this.fs, this.testFile);
		String fileContents = new String(fileContentBuffer);
		Assert.assertTrue(fileContents.equals(outputString));
	}

	public void testAppends2() {
		String outputString = "";
		String testLine = "Lorem ipsum dolor sit amet\n";
		String actualLine = "Lorem ipsum dolor sit amet";
		OutputStream os = HDFSUtils.getBufferedHDFSOutputStream(this.fs, this.testFile, this.cfg.getHDFS_REPLICATION_FACTOR(), 10000);

		try {
			for (int i=0; i<10000; i++) {
				byte[] stringBytes = testLine.getBytes();
				os.write(stringBytes, 0, stringBytes.length - 1);
				outputString += actualLine;
			}
			os.close();
		} catch (IOException e) {
			Assert.assertTrue(false);
		}

		byte[] fileContentBuffer = HDFSUtils.readFile(this.fs, this.testFile);
		String fileContents = new String(fileContentBuffer);
		Assert.assertTrue(fileContents.equals(outputString));
	}
}
