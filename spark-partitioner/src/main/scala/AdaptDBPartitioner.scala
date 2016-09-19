import org.apache.spark.Partitioner

/**
  * Created by ylu on 9/19/16.
  */

class AdaptDBPartitioner(num: Int) extends Partitioner {

  def numPartitions: Int = num

  def getPartition(key: Any): Int = key match {
    case x: Int =>  x
    case _ => throw new ClassCastException
  }

}
