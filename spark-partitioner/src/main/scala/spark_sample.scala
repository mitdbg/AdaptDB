/**
  * Created by ylu on 10/5/16.
  */

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}



/**
  * Created by ylu on 9/26/16.
  */
object spark_sample {

  def main(args: Array[String]) {

    val conf = new SparkConf()
    conf.setAppName("spark_partitioner")
    conf.setMaster("spark://istc1.csail.mit.edu:7077")

    val rawData  = args(0)
    val output = args(1)
    val sampleRate = args(2).toDouble

    val sc = new SparkContext(conf)

    val rdd = sc.textFile(rawData)
    val sample = rdd.sample(false, sampleRate)
    sample.saveAsTextFile(output)

  }
}
