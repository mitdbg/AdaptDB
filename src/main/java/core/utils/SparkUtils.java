package core.utils;

import org.apache.spark.SparkConf;

/**
 * Created by ylu on 1/19/16.
 */
public class SparkUtils {

    public static SparkConf getSparkConf(String appName, ConfUtils cfg) {
        SparkConf sconf = new SparkConf().setMaster(cfg.getSPARK_MASTER())
                .setAppName(appName)
                .setSparkHome(cfg.getSPARK_HOME())
                .setJars(new String[]{cfg.getSPARK_APPLICATION_JAR()})
                .set("spark.hadoop.cloneConf", "false")
                .set("spark.executor.memory", cfg.getSPARK_EXECUTOR_MEMORY())
                .set("spark.driver.memory", cfg.getSPARK_DRIVER_MEMORY())
                .set("spark.task.cpus", cfg.getSPARK_TASK_CPUS())
                .set("spark.hadoop.dfs.replication", "1");

        return sconf;
    }
}
