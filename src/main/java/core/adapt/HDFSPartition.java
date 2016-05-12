package core.adapt;

import java.io.IOException;
import java.sql.Timestamp;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.io.ByteStreams;

import core.utils.ConfUtils;
import core.utils.CuratorUtils;
import core.utils.HDFSUtils;

public class HDFSPartition extends Partition {
    private static final long serialVersionUID = 1L;

    protected FileSystem hdfs;
    protected short replication;

    private FSDataInputStream in;
    private long totalSize = 0;

    private CuratorFramework client;

    //private InterProcessSemaphoreMutex read_lock;

    public HDFSPartition(String pathAndPartitionId, String propertiesFile,
                         short replication) {
        super(pathAndPartitionId);
        ConfUtils conf = new ConfUtils(propertiesFile);
        String coreSitePath = conf.getHADOOP_HOME()
                + "/etc/hadoop/core-site.xml";
        Configuration e = new Configuration();
        e.addResource(new Path(coreSitePath));
        try {
            this.hdfs = FileSystem.get(e);
            this.replication = replication;
        } catch (IOException ex) {
            throw new RuntimeException("failed to get hdfs filesystem");
        }
        client = CuratorUtils.createAndStartClient(
                conf.getZOOKEEPER_HOSTS());

    }

    public HDFSPartition(FileSystem hdfs, String pathAndPartitionId,
                         short replication, CuratorFramework client) {
        super(pathAndPartitionId);
        this.hdfs = hdfs;
        this.replication = replication;
        this.client = client;
    }

    @Override
    public Partition clone() {
        Partition p = new HDFSPartition(hdfs, path + partitionId, replication, client);
        p.bytes = new byte[8192];
        p.state = State.NEW;
        return p;
    }

    public FileSystem getFS() {
        return hdfs;
    }

    public void setTotalSize(long size) {
        this.totalSize = size;
    }

    /*
    public boolean loadNext() {
        try {
            if (totalSize == 0) {
                Path p = new Path(path + "/" + partitionId);
                totalSize = hdfs.getFileStatus(p).getLen();
                in = hdfs.open(p);
            }

            if (readSize < totalSize) {
                bytes = new byte[(int) Math.min(MAX_READ_SIZE, totalSize
                        - readSize)];
                ByteStreams.readFully(in, bytes);
                readSize += bytes.length;
                return true;
            } else {
                in.close();
                readSize = 0;
                totalSize = 0;
                return false;
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to read file: " + path + "/"
                    + partitionId);
        }
    }
    */
    @Override
    public boolean load() {
        if (path == null || path.equals("")) {
            throw new RuntimeException();
        }
        try {

            InterProcessSemaphoreMutex l = CuratorUtils.acquireLock(client,
                    "/partition-lock-" + path.hashCode() + "-" + partitionId);
            System.out.println("LOCK: acquired lock,  " + "path=" + path
                    + " , partition id=" + partitionId + " , for loading, size: " + totalSize);

            Path p = new Path(path + "/" + partitionId);
            in = hdfs.open(p);
            bytes = new byte[(int) totalSize];
            ByteStreams.readFully(in, bytes);
            in.close();

            CuratorUtils.releaseLock(l);
            return true; // load the physical block for this partition
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to read file: " + path + "/"
                    + partitionId);
        }
    }

    @Override
    public void store(boolean append) {

        InterProcessSemaphoreMutex l = CuratorUtils.acquireLock(client,
                "/partition-lock-" + path.hashCode() + "-" + partitionId);
        System.out.println("LOCK: acquired lock,  " + "path=" + path
                + " , partition id=" + partitionId + " ,size= " + offset);


        String storePath = path + "/" + partitionId;
        if (!path.startsWith("hdfs"))
            storePath = "/" + storePath;

        Path e = new Path(storePath);

        FSDataOutputStream os = null;

        boolean shouldAppend = false;

        try {
            boolean overwrite = !append;
            os = hdfs.create(e, overwrite, hdfs.getConf().getInt("io.file.buffer.size", 4096),
                    replication, hdfs.getDefaultBlockSize(e));

            System.out.println("created partition " + partitionId);
        } catch (IOException ex) {
            shouldAppend = true;
        }

        try {
            if (shouldAppend) {
                os = hdfs.append(e);
            }
            os.write(bytes, 0, offset);
            os.flush();
            os.close();
            recordCount = 0;
        } catch (IOException ex) {
            System.out.println("exception: "
                    + (new Timestamp(System.currentTimeMillis())));
            //throw new RuntimeException(ex.getMessage());
        } finally {
            CuratorUtils.releaseLock(l);
            System.out.println("LOCK: released lock " + partitionId);
        }
    }

    @Override
    public void drop() {
        // HDFSUtils.deleteFile(hdfs, path + "/" + partitionId, false);
    }
}
