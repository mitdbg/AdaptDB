import org.apache.spark.Partitioner

/**
  * Created by ylu on 9/19/16.
  */

class AdaptDBPartitioner(JRTree: JoinRobustTree) extends Partitioner {
  def numPartitions: Int = JRTree.numBuckets

  def getPartition(key: Any): Int = key match {
    case x: (Int, String) =>  x._1
    case _ => throw new ClassCastException
  }
}
