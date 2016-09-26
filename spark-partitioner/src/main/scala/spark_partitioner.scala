import java.util.{Calendar, GregorianCalendar}


import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object spark_partitioner {

  /*
    input structure

    /user/yilu/tpch1000-spark/lineitem/part-00000
    ....

   */


  /*
    output structure

    drwxr-xr-x   - mdindex supergroup          0 2016-05-31 03:48 /user/yilu/tpch1000/lineitem/data
    -rw-r--r--   3 mdindex supergroup     401799 2016-05-31 03:48 /user/yilu/tpch1000/lineitem/index
    -rw-r--r--   3 mdindex supergroup        345 2016-05-31 03:48 /user/yilu/tpch1000/lineitem/info
    -rw-r--r--   3 mdindex supergroup   39084255 2016-05-31 03:48 /user/yilu/tpch1000/lineitem/sample
    drwxr-xr-x   - mdindex supergroup          0 2016-05-31 03:48 /user/yilu/tpch1000/lineitem/samples

   */


  // reads a table from ``table", return an RDD

  def readTable(sc: SparkContext, table: String): RDD[String] = {
    sc.textFile(table)
  }


  def partition(sc: SparkContext, table: RDD[String], pathToIndex: String): RDD[String] = {

    val tableWithKeys = table.mapPartitions(values => {
      var indexBytes = HDFSUtils.readFileToBytes(pathToIndex);
      val adaptDbIndex = new JoinRobustTree(indexBytes)
      val keyValuesPairs = values.map(x => ( adaptDbIndex.getBucketId(x), x));
      keyValuesPairs
    })

    val indexBytes = HDFSUtils.readFileToBytes(pathToIndex);
    val adaptDbIndex = new JoinRobustTree(indexBytes)

    val tableRepartitioned = tableWithKeys.partitionBy(new AdaptDBPartitioner(adaptDbIndex.numBuckets))
    tableRepartitioned.values
  }

  def saveTable(table: RDD[String], savePath: String): Unit = {
    table.saveAsTextFile(savePath)
  }


  def main(args: Array[String]) {

    val conf = new SparkConf()
    conf.setAppName("spark_partitioner")
    conf.setMaster("spark://istc13.csail.mit.edu:7077")

    val sc = new SparkContext(conf)

    val rawData  = args(0)
    val adpatbPath = args(1)
    val index = adpatbPath + "/index"
    val output = adpatbPath + "/data"

    val table = readTable(sc, rawData)
    val partitionedTable = partition(sc, table, index)

    saveTable(partitionedTable, output)

    HDFSUtils.rename(output)
  }
}
