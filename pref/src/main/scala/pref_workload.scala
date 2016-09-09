import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object pref_workload {
  val hadoopHome = "/Users/ylu/Documents/workspace/hadoop-2.6.0"

  val partitionNum = 200

  val lineitem = "hdfs://localhost:9000/user/ylu/tpch1/repartitioned_lineitem"
  val orders = "hdfs://localhost:9000/user/ylu/tpch1/repartitioned_orders"
  val customer = "hdfs://localhost:9000/user/ylu/tpch1/repartitioned_customer"
  val part = "hdfs://localhost:9000/user/ylu/tpch1/repartitioned_part"
  val supplier = "hdfs://localhost:9000/user/ylu/tpch1/repartitioned_supplier"


  val lineitemDistinctPath = "hdfs://localhost:9000/user/ylu/tpch1/repartitioned_lineitemdistinct"
  val ordersDistinctPath = "hdfs://localhost:9000/user/ylu/tpch1/repartitioned_orders_distinct"
  val customerDistinctPath = "hdfs://localhost:9000/user/ylu/tpch1/repartitioned_customer_distinct"
  val partDistinctPath = "hdfs://localhost:9000/user/ylu/tpch1/repartitioned_part_distinct"
  val supplierDistinctPath = "hdfs://localhost:9000/user/ylu/tpch1/repartitioned_supplier_distinct"

  // reads a table from ``table", return an RDD

  def readTable(sc: SparkContext, table: String): RDD[String] = {
    val partitionIdsRDD = sc.parallelize(0 to partitionNum - 1, partitionNum)
    val rdd = partitionIdsRDD.mapPartitionsWithIndex(
      (partitionId, values) => {
        val tablePath = "%s/part-%05d".format(table, partitionId)
        val fs = HDFSUtils.getFSByHadoopHome(hadoopHome)
        val text = HDFSUtils.readFile(fs, tablePath)

        text.split("\n").iterator
      }, true
    )
    rdd
  }

  // reads a table from ``otherTable", joins with table ``baseTable" on joinKeyA = joinKeyB. Assume the datatype is long

  def joinTable(sc: SparkContext, baseTable: RDD[String], otherTable: String, joinKeyA: Int, joinKeyB: Int): RDD[String] = {

    val rdd = baseTable.mapPartitionsWithIndex(
      (partitionId, values) => {
        val tablePath = "%s/part-%05d".format(otherTable, partitionId)
        val fs = HDFSUtils.getFSByHadoopHome(hadoopHome)
        // build a hash table over otherTable

        val tuples = HDFSUtils.readFile(fs, tablePath).split("\n");

        val hashTable = tuples.map(x => {
          val columns = x.split(Global.DELIMITER)
          (columns(joinKeyB).toLong, x)
        }).toMap

        //and probe it with baseTable

        val results = values.flatMap(x => {
          val key = x.split(Global.DELIMITER)(joinKeyA).toLong
          if (hashTable.contains(key)) {
            // join with this tuple
            Seq(x + Global.DELIMITER + hashTable.get(key))
          }
          else Seq()
        })

        results
      }, true
    )

    rdd
  }

  // put all strings into a set and write back to hdfs

  def selectDistinct(table: RDD[String]): RDD[String] = {
    table.mapPartitions(x => x.toSet.iterator, true)
  }

  def saveTable(table: RDD[String], savePath: String): Unit ={
      table.saveAsTextFile(savePath)
  }

  def testReadTable(sc: SparkContext): Unit = {

    val lineitemTable = readTable(sc, lineitem)
    println("no. of lineitem = " + lineitemTable.count())
    val distinctLineitem = selectDistinct(lineitemTable)
    println("no. of distinct lineitem = " + distinctLineitem.count())


    val partTable = readTable(sc, part)
    println("no. of part = " + partTable.count())
    val distinctPart = selectDistinct(partTable)
    println("no. of distinct part = " + distinctPart.count())

    val ordersTable = readTable(sc, orders)
    println("no. of orders = " + ordersTable.count())
    val distinctOrders = selectDistinct(ordersTable)
    println("no. of distinct orders = " + distinctOrders.count())

    val customerTable = readTable(sc, customer)
    println("no. of customer = " + customerTable.count())
    val distinctCustomer = selectDistinct(customerTable)
    println("no. of distinct customer = " + distinctCustomer.count())

    val supplierTable = readTable(sc, supplier)
    println("no. of supplier = " + supplierTable.count())
    val distinctSupplier = selectDistinct(supplierTable)
    println("no. of distinct supplier = " + distinctSupplier.count())


    println("no. of lineitem = " + lineitemTable.distinct().count())
    println("no. of part = " + partTable.distinct().count())
    println("no. of orders = " + ordersTable.distinct().count())
    println("no. of customer = " + customerTable.distinct().count())
    println("no. of supplier = " + supplierTable.distinct().count())
  }


  def testJoinTable(sc: SparkContext): Unit ={
    val lineitemTable = readTable(sc, lineitem)

    val l_join_o = joinTable(sc, lineitemTable, orders, 0, 0)
    val l_join_o_join_c = joinTable(sc, l_join_o, customer, 16, 0)
    val l_join_o_join_c_join_p = joinTable(sc, l_join_o_join_c, part, 1, 0)
    val l_join_o_join_c_join_p_join_s = joinTable(sc, l_join_o_join_c_join_p, supplier, 2, 0)

    println("no. of l_join_o = " + l_join_o.count())
    println("no. of l_join_o_join_c = " + l_join_o_join_c.count())
    println("no. of l_join_o_join_c_join_p = " + l_join_o_join_c_join_p.count())
    println("no. of l_join_o_join_c_join_p_join_s = " + l_join_o_join_c_join_p_join_s.count())
  }

  def testJoinDistinctTable(sc: SparkContext): Unit ={
    val lineitemTable = readTable(sc, lineitem)

    val l_join_o = joinTable(sc, lineitemTable, ordersDistinctPath, 0, 0)
    val l_join_o_join_c = joinTable(sc, l_join_o, customerDistinctPath, 16, 0)
    val l_join_o_join_c_join_p = joinTable(sc, l_join_o_join_c, partDistinctPath, 1, 0)
    val l_join_o_join_c_join_p_join_s = joinTable(sc, l_join_o_join_c_join_p, supplierDistinctPath, 2, 0)

    println("no. of l_join_o = " + l_join_o.count())
    println("no. of l_join_o_join_c = " + l_join_o_join_c.count())
    println("no. of l_join_o_join_c_join_p = " + l_join_o_join_c_join_p.count())
    println("no. of l_join_o_join_c_join_p_join_s = " + l_join_o_join_c_join_p_join_s.count())
  }

  def testSaveDistinctTable(sc: SparkContext): Unit ={

    val ordersTable = readTable(sc, orders)
    val ordersTableDistinct = selectDistinct(ordersTable)
    saveTable(ordersTableDistinct, ordersDistinctPath)

    val customerTable = readTable(sc, customer)
    val customerTableDistinct = selectDistinct(customerTable)
    saveTable(customerTableDistinct, customerDistinctPath)

    val partTable = readTable(sc, part)
    val partTableDistinct = selectDistinct(partTable)
    saveTable(partTableDistinct, partDistinctPath)

    val supplierTable = readTable(sc, supplier)
    val supplierTableDistinct = selectDistinct(supplierTable)
    saveTable(supplierTableDistinct, supplierDistinctPath)
  }

  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setAppName("pref_workload")
    conf.setMaster("spark://26-2-88.dynamic.csail.mit.edu:7077")
    conf.set("spark.cores.max", "2");

    val sc = new SparkContext(conf)

    //testSaveDistinctTable(sc)

    testJoinDistinctTable(sc)
  }
}
