package core.utils;

import junit.framework.TestCase;

public class TestHDFSUtils extends TestCase {

	private String filepath;
	private String hadoopHome;
	
	public void setUp(){
		this.hadoopHome = "/Users/alekh/Softwares/sources/Hadoop/hadoop-2.0.6-alpha";
		this.filepath = "/testAppendFile";
	}
	
	public void testAppendLine(){
		HDFSUtils.createFile(hadoopHome, filepath, (short)1);
		HDFSUtils.appendLine(hadoopHome, filepath, "line1");
		HDFSUtils.appendLine(hadoopHome, filepath, "line2");
		HDFSUtils.appendLine(hadoopHome, filepath, "line3");
	}
}
