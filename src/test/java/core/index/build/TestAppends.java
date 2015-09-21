package core.index.build;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.fs.FileSystem;

import perf.benchmark.BenchmarkSettings;
import core.utils.ConfUtils;
import core.utils.HDFSUtils;

/**
 * Failed attempt to see if parallel appends work.
 * They don't work. We get error of the form :
 * [istc5] out: Exception in thread "main" java.lang.RuntimeException: Could not open the file:/user/mdindex/lineitem1000//testAppends, Failed to create file [/user/mdindex/lineitem1000/testAppends] for [DFSClient_NONMAPREDUCE_-976788346_1] for client [128.30.77.78], because this file is already being created by [DFSClient_NONMAPREDUCE_-1724569306_1] on [128.30.77.86]
 * Particularly, two guys can't write to the same file.
 *
 * The other issue is xcievers are sockets open. Each is its seperate thread and takes ~1MB memory each.
 * So we can't increase the xcievers. (Currently its 4096 ~ 4GB memory).
 *
 * @author anil
 *
 */
public class TestAppends {
	ConfUtils cfg;
	String hdfsPartitionDir;
	String testFile;
	String sentence;
	int method;

	public TestAppends() {
		cfg = new ConfUtils(BenchmarkSettings.conf);
		hdfsPartitionDir = cfg.getHDFS_WORKING_DIR();
		testFile = hdfsPartitionDir + "/testAppends";
		sentence = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.";
	}

	public void runAppends() {
		FileSystem fs = HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME());
		OutputStream os = HDFSUtils.getHDFSOutputStream(fs, testFile, cfg.getHDFS_REPLICATION_FACTOR(), 1 << 20 /* 1 MB */);

		sentence = cfg.getMACHINE_ID() + " " + sentence + "\n";

		for (int i=0; i<100000; i++) {
			try {
				os.write(sentence.getBytes());
			} catch (IOException e) {
				System.out.println("Append failed");
				e.printStackTrace();
			}
		}

		try {
			os.flush();
			os.close();
		} catch (IOException e) {
			System.out.println("FLUSH FAILED !!!!!");
			e.printStackTrace();
		}
	}

	public void verifyAppends() {
		// The same string sentence will be coming from different machines.
		// Make sure they are still full sentences.
		// Run on a single machine.
		FileSystem fs = HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME());
		byte[] stringBytes = HDFSUtils.readFile(fs, testFile);
		String fileContents = new String(stringBytes);
		String[] sentences = fileContents.split("\n");

		boolean allGood = true;
		for (int i=0; i<sentences.length; i++) {
			char t = sentences[i].charAt(0);
			int tVal = Character.getNumericValue(t);
			if (tVal < 0 || tVal > 10) {
				System.out.println("Sentence not well formed ! " + tVal);
				allGood = false;
			}

			String baseSentence = sentences[i].substring(2);
			if (!baseSentence.equals(sentence)) {
				System.out.println("Saw wrong sentence " + baseSentence);
				allGood = false;
			}

			if (!allGood) {
				break;
			}
		}

		System.out.println("Read " + sentences.length + " lines. LGTM.");
	}

	public void loadSettings(String[] args) {
		int counter = 0;
		while (counter < args.length) {
			switch (args[counter]) {
			case "--method":
				method = Integer.parseInt(args[counter + 1]);
				counter += 2;
				break;
			default:
				// Something we don't use
				counter += 2;
				break;
			}
		}
	}

	public static void main(String[] args) {
		BenchmarkSettings.loadSettings(args);
		BenchmarkSettings.printSettings();

		TestAppends t = new TestAppends();
		t.loadSettings(args);

		if (t.method == 0) {
			System.out.println("Running Appends");
			t.runAppends();
		} else {
			System.out.println("Testing Appends");
			t.verifyAppends();
		}
	}
}
