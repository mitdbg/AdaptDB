package core.utils;

import java.util.List;

import junit.framework.TestCase;

import org.junit.Assert;

import perf.benchmark.BenchmarkSettings;

public class TestHDFSUtils extends TestCase {
	private String hadoopHome;
	private String filepath;
	private short replication;

	@Override
	public void setUp(){
		ConfUtils cfg = new ConfUtils(BenchmarkSettings.conf);
		hadoopHome = cfg.getHADOOP_HOME();
		filepath = cfg.getHDFS_WORKING_DIR() + "/testUtils.txt";
		replication = cfg.getHDFS_REPLICATION_FACTOR();
	}

	public void testAppendLine(){
		HDFSUtils.createFile(hadoopHome, filepath, replication);
		HDFSUtils.appendLine(hadoopHome, filepath, "line1");
		HDFSUtils.appendLine(hadoopHome, filepath, "line2");
		List<String> lines = HDFSUtils.readHDFSLines(hadoopHome, filepath);
		Assert.assertTrue(lines.size() == 2);
		Assert.assertTrue(lines.get(0).equals("line1"));
		Assert.assertTrue(lines.get(1).equals("line2"));
	}
}
