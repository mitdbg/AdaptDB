import java.util.{Calendar, GregorianCalendar}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.collection.JavaConverters._

/**
  * Created by ylu on 9/26/16.
  */
object spark_join {
  var lineitem = ""
  var orders = ""


  val stringLineitem = "l_orderkey long, l_partkey int, l_suppkey int, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate date, l_commitdate date, l_receiptdate date, l_shipinstruct string, l_shipmode string"
  val stringOrders = "o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate date, o_orderpriority string, o_clerk string, o_shippriority int"

  val schemaLineitem = Schema.createSchema(stringLineitem)
  val schemaOrders = Schema.createSchema(stringOrders)

  def joinTable(sc:SparkContext): Unit ={

    val start = System.currentTimeMillis()

    val table1Rdd = sc.textFile(lineitem);
    val table2Rdd = sc.textFile(orders);

    val key1 = schemaLineitem.getAttributeId("l_orderkey")
    val key2 = schemaOrders.getAttributeId("o_orderkey")

    val table1RddWithKeys = table1Rdd.map( x => {
      val key = x.split(Global.SPLIT_DELIMITER)(key1)
      (key, x)
    })

    val table2RddWithKeys = table1Rdd.map( x => {
      val key = x.split(Global.SPLIT_DELIMITER)(key2)
      (key, x)
    })

    val resultRdd = table1RddWithKeys.join(table2RddWithKeys, 800).map( x=> x._2._1 + Global.DELIMITER + x._2._2 )
    val result = resultRdd.count()

    val end = System.currentTimeMillis()

    println("RES: Time Taken: " + (end - start) + " Result: " + result)

  }


  def main(args: Array[String]) {

    val conf = new SparkConf()
    conf.setAppName("spark_partitioner")
    conf.setMaster("spark://istc1.csail.mit.edu:7077")

    val sc = new SparkContext(conf)

    lineitem = args(0)
    orders = args(1)


    joinTable(sc);

  }
}
