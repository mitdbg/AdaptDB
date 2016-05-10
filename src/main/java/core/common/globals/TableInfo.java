package core.common.globals;

import com.google.common.base.Joiner;
import core.common.index.JoinRobustTree;
import core.common.index.RobustTree;
import core.utils.HDFSUtils;
import core.utils.TypeUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;

/**
 * Created by anil on 12/11/15.
 */
public class TableInfo {
    // Name of table.
    public String tableName;

    // Total number of tuples in dataset.
    public double numTuples;

    // TPC-H generated files use '|'. CSV uses ','.
    public char delimiter;

    // Schema of data set.
    public Schema schema;

    // -1 for non-join specific partition
    public int[] partitions;

    public int depth;

    public TableInfo(String tableName) {
        this(tableName, 0, '|', null);
    }

    public TableInfo(String tableName, double numTuples, char delimiter, Schema schema) {
        this.tableName = tableName;
        this.numTuples = numTuples;
        this.delimiter = delimiter;
        this.schema = schema;
        this.partitions = new int[]{-1};
        this.depth = 0;
    }

    private String partitionsString() {
        String res = "";
        for (int i = 0; i < partitions.length; i++) {
            if (i != 0) {
                res += ";";
            }
            res += Integer.toString(partitions[i]);
        }
        return res;
    }

    public TypeUtils.TYPE[] getTypeArray() {
        return schema.getTypeArray();
    }

    public void save(String hdfsWorkingDir, short replication, FileSystem fs) {
        String saveContent = "TOTAL_NUM_TUPLES: " + numTuples + "\n" +
                "DELIMITER: " + delimiter + "\n" +
                "SCHEMA: " + schema.toString() + "\n" +
                "PARTITION: " + partitionsString() + "\n" +
                "DEPTH: " + depth + "\n";
        byte[] saveContentBytes = saveContent.getBytes();
        String path = hdfsWorkingDir + "/" + tableName + "/info";
        HDFSUtils.writeFile(fs, path, replication,
                saveContentBytes, 0, saveContentBytes.length, false);
    }

    public void load(String hdfsWorkingDir, FileSystem fs) {
        String path = hdfsWorkingDir + "/" + tableName + "/info";
        byte[] fileContent = HDFSUtils.readFile(fs, path);
        String content = new String(fileContent);

        String[] settings = content.split("\n");

        if (settings.length != 5) {
            throw new RuntimeException();
        }

        for (int i = 0; i < settings.length; i++) {
            String setting = settings[i];
            String[] parts = setting.split(":");
            switch (parts[0].trim()) {
                case "TOTAL_NUM_TUPLES":
                    numTuples = Double.parseDouble(parts[1].trim());
                    break;
                case "DELIMITER":
                    delimiter = parts[1].trim().charAt(0);
                    break;
                case "SCHEMA":
                    schema = Schema.createSchema(parts[1].trim());
                    break;
                case "PARTITION":
                    String[] strPartitions = parts[1].trim().split(";");
                    partitions = new int[strPartitions.length];
                    for (int k = 0; k < strPartitions.length; k++) {
                        partitions[k] = Integer.parseInt(strPartitions[k]);
                    }
                    break;
                case "DEPTH":
                    depth = Integer.parseInt(parts[1].trim());
                    break;
                default:
                    System.out.println("Unknown setting found: " + parts[0].trim());
            }
        }
    }

    public void gc(String hdfsWorkingDir, FileSystem fs) {
        String path = hdfsWorkingDir + "/" + tableName;
        String pathToData = path + "/data";

        for (int i = 0; i < partitions.length; i++) {
            String pathToIndex;

            if (partitions[i] == -1) {
                pathToIndex = path + "/index";
            } else {
                pathToIndex = path + "/index." + partitions[i];
            }

            byte[] indexBytes = HDFSUtils.readFile(fs, pathToIndex);
            JoinRobustTree rt = new JoinRobustTree(this);
            rt.unmarshall(indexBytes);
            int[] bids = rt.getAllBuckets();
            HashSet<Integer> buckets = new HashSet<Integer>();
            for (int k = 0; k < bids.length; k++) {
                buckets.add(bids[k]);
            }
            try {
                FileStatus[] existingFiles = fs.listStatus(new Path(pathToData));
                for (int k = 0; k < existingFiles.length; k++) {
                    Path fp = existingFiles[k].getPath();
                    String fileName = FilenameUtils.getName(fp.toString());
                    int id = Integer.parseInt(fileName);
                    if (buckets.contains(id) == false) {
                        fs.delete(fp, false);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
