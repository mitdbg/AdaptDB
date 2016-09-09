// ./spark-shell --master spark://26-2-88.dynamic.csail.mit.edu:7077 --executor-memory 4g --driver-memory 1g


import org.apache.spark.{SparkConf, SparkContext}



object pref_workload {


  def main(args: Array[String]) {

    val conf = new SparkConf()
    conf.setAppName("pref_workload")
    conf.setMaster("spark://localost:7077");
    val sc = new SparkContext(conf)

    val partitionNum = 200

    val partitionIdsRDD = sc.parallelize(0 to partitionNum - 1)



  }
}