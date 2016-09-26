import java.util.{Calendar, GregorianCalendar}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.collection.JavaConverters._

/**
  * Created by ylu on 9/26/16.
  */
class spark_queryexecutor {
  var lineitem = ""
  var orders = ""
  var customer = ""
  var part = ""
  var supplier = ""


  val stringLineitem = "l_orderkey long, l_partkey int, l_suppkey int, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate date, l_commitdate date, l_receiptdate date, l_shipinstruct string, l_shipmode string"
  val stringOrders = "o_orderkey long, o_custkey long, o_orderstatus string, o_totalprice double, o_orderdate date, o_orderpriority string, o_clerk string, o_shippriority int"
  val stringCustomer = "c_custkey long, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string"
  val stringPart = "p_partkey long, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double"
  val stringSupplier = "s_suppkey long, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string"

  val schemaLineitem = Schema.createSchema(stringLineitem)
  val schemaOrders = Schema.createSchema(stringOrders)
  val schemaCustomer = Schema.createSchema(stringCustomer)
  val schemaPart = Schema.createSchema(stringPart)
  val schemaSupplier = Schema.createSchema(stringSupplier)

  val mktSegmentVals = Array("AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY")
  val regionNameVals = Array("AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST")
  val partTypeVals = Array("BRASS", "COPPER", "NICKEL", "STEEL", "TIN")
  val shipModeVals = Array("AIR", "FOB", "MAIL", "RAIL", "REG AIR", "SHIP", "TRUCK")

  val rand = new Random()


  // reads a table from ``table", return an RDD
  // query is applied to table

  def readTableWithQuery(sc: SparkContext, table: String, query: Query): RDD[(Long, String)] = {

    val queryString = query.toString

    val pathToIndex = table + "/index"
    val indexBytes = HDFSUtils.readFileToBytes(pathToIndex)
    val adaptDbIndex = new JoinRobustTree(indexBytes)

    val blocks = adaptDbIndex.getRoot().search(query.getPredicates).asScala.map(x => x.bucketId)

    val partitionIdsRDD = sc.parallelize(blocks, blocks.size)
    val rdd = partitionIdsRDD.mapPartitionsWithIndex(
      (_, partitionIds) => {

        val partitionId = partitionIds.next()

        if (partitionIds.hasNext) {
          throw new RuntimeException("sizeof(partitionIds) should be 1.")
        }

        val q = new Query(queryString)
        val tablePath = "%s/d".format(table, partitionId)
        var tuples = HDFSUtils.readFile(tablePath)
        val filteredTuples = tuples.filter(x => q.qualifies(x)).map(x => {

          val columns = x.split(Global.SPLIT_DELIMITER)
          val joinKey = columns(q.getJoinAttribute).toLong
          (joinKey, x)
        })
        filteredTuples.iterator
      }, true
    )
    rdd
  }


  // reads a table from ``otherTable", joins with table ``baseTable" on joinKeyA = joinKeyB. Assume the datatype is long
  // query is applied to otherTable

  def joinTable(sc: SparkContext, baseTable: RDD[(Long, String)], otherTable: RDD[(Long, String)], newJoinKey: Int): RDD[(Long, String)] = {
    baseTable.join(otherTable).map(x => {
      val concate = x._2._1 + Global.DELIMITER + x._2._2
      val columns = concate.split(Global.SPLIT_DELIMITER)
      if (newJoinKey == -1) {
        val key = 1L * newJoinKey
        (key, concate)
      } else {
        val key = columns(newJoinKey).toLong
        (key, concate)
      }
    })
  }


  def tpch3(sc: SparkContext): Unit = {
    val rand_3 = rand.nextInt(mktSegmentVals.length)
    val c_mktsegment = mktSegmentVals(rand_3)
    val c = new GregorianCalendar()
    val dateOffset = (rand.nextFloat() * (31 + 28 + 31)).toInt
    c.set(1995, Calendar.MARCH, 01)
    c.add(Calendar.DAY_OF_MONTH, dateOffset)
    val d3 = new TypeUtils.SimpleDate(c.get(Calendar.YEAR), c.get(Calendar.MONTH), c.get(Calendar.DAY_OF_MONTH))


    val p1_3 = new Predicate(schemaCustomer, "c_mktsegment", TypeUtils.TYPE.STRING, c_mktsegment, Predicate.PREDTYPE.LEQ)
    val p2_3 = new Predicate(schemaOrders, "o_orderdate", TypeUtils.TYPE.DATE, d3, Predicate.PREDTYPE.LT)
    val p3_3 = new Predicate(schemaLineitem, "l_shipdate", TypeUtils.TYPE.DATE, d3, Predicate.PREDTYPE.GT)

    val q_o = new Query(schemaOrders.getAttributeId("o_orderkey"), Array(p2_3))
    val q_l = new Query(schemaLineitem.getAttributeId("l_orderkey"), Array(p3_3))


    val q_c = {
      if (rand_3 > 0) {
        val c_mktsegment_prev = mktSegmentVals(rand_3 - 1)
        val p4_3 = new Predicate(schemaCustomer, "c_mktsegment", TypeUtils.TYPE.STRING, c_mktsegment_prev, Predicate.PREDTYPE.GT)
        new Query(schemaCustomer.getAttributeId("c_custkey"), Array(p1_3, p4_3))
      } else {
        new Query(schemaCustomer.getAttributeId("c_custkey"), Array(p1_3))
      }
    }


    val start = System.currentTimeMillis()

    val table_l = readTableWithQuery(sc, lineitem, q_l)
    val table_o = readTableWithQuery(sc, orders, q_o)
    val table_c = readTableWithQuery(sc, customer, q_c)


    val string_l_join_o = stringLineitem + ", " + stringOrders
    val schema_l_join_o = Schema.createSchema(string_l_join_o)

    val table_l_join_o = joinTable(sc, table_l, table_o, schema_l_join_o.getAttributeId("o_custkey"))

    val table_l_join_o_join_c = joinTable(sc, table_l_join_o, table_c, -1)

    val result = table_l_join_o_join_c.count()

    //table_l_join_o_join_c.foreach( x => println("### " + x + " ###"))

    val end = System.currentTimeMillis()

    println("RES: Time Taken: " + (end - start) + " Result: " + result)
  }


  def tpch5(sc: SparkContext): Unit = {

    val rand_5 = rand.nextInt(regionNameVals.length)
    val r_name_5 = regionNameVals(rand_5)
    val year_5 = 1993 + rand.nextInt(5)
    val d5_1 = new TypeUtils.SimpleDate(year_5, 1, 1)
    val d5_2 = new TypeUtils.SimpleDate(year_5 + 1, 1, 1)
    val p1_5 = new Predicate(schemaCustomer, "c_region", TypeUtils.TYPE.STRING, r_name_5, Predicate.PREDTYPE.LEQ)
    val p2_5 = new Predicate(schemaSupplier, "s_region", TypeUtils.TYPE.STRING, r_name_5, Predicate.PREDTYPE.LEQ)
    val p3_5 = new Predicate(schemaOrders, "o_orderdate", TypeUtils.TYPE.DATE, d5_1, Predicate.PREDTYPE.GEQ)
    val p4_5 = new Predicate(schemaOrders, "o_orderdate", TypeUtils.TYPE.DATE, d5_2, Predicate.PREDTYPE.LT)

    val q_o = new Query(schemaOrders.getAttributeId("o_custkey"), Array(p3_5, p4_5))
    val q_l = new Query(schemaLineitem.getAttributeId("l_suppkey"), Global.EmptyPredicates)

    val q_c = {
      if (rand_5 > 0) {
        val r_name_prev_5 = regionNameVals(rand_5 - 1)
        val p5_5 = new Predicate(schemaCustomer, "c_region", TypeUtils.TYPE.STRING, r_name_prev_5, Predicate.PREDTYPE.GT)
        new Query(schemaCustomer.getAttributeId("c_custkey"), Array(p1_5, p5_5))
      } else {
        new Query(schemaCustomer.getAttributeId("c_custkey"), Array(p1_5))
      }
    }

    val q_s = {
      if (rand_5 > 0) {
        val r_name_prev_5 = regionNameVals(rand_5 - 1)
        val p6_5 = new Predicate(schemaSupplier, "s_region", TypeUtils.TYPE.STRING, r_name_prev_5, Predicate.PREDTYPE.GT)
        new Query(schemaSupplier.getAttributeId("s_suppkey"), Array(p2_5, p6_5))
      } else {
        new Query(schemaSupplier.getAttributeId("s_suppkey"), Array(p2_5))
      }
    }

    val start = System.currentTimeMillis()

    val table_l = readTableWithQuery(sc, lineitem, q_l)
    val table_o = readTableWithQuery(sc, orders, q_o)
    val table_c = readTableWithQuery(sc, customer, q_c)
    val table_s = readTableWithQuery(sc, supplier, q_s)


    val string_l_join_s = stringLineitem + ", " + stringSupplier
    val schema_l_join_s = Schema.createSchema(string_l_join_s)

    val table_l_join_s = joinTable(sc, table_l, table_s, schema_l_join_s.getAttributeId("l_orderkey"))


    val string_c_join_o = stringCustomer + ", " + stringOrders
    val schema_c_join_o = Schema.createSchema(string_c_join_o)

    val table_c_join_o = joinTable(sc, table_c, table_o, schema_c_join_o.getAttributeId("o_orderkey"))


    val table_l_join_o_join_c_join_s = joinTable(sc, table_l_join_s, table_c_join_o, -1)

    val result = table_l_join_o_join_c_join_s.count()

    val end = System.currentTimeMillis()


    //table_l_join_o_join_c_join_s.foreach( x => println("### " + x + " ###"))

    println("RES: Time Taken: " + (end - start) + " Result: " + result)


  }

  def tpch6(sc: SparkContext): Unit = {
    val year_6 = 1993 + rand.nextInt(5)
    val d6_1 = new TypeUtils.SimpleDate(year_6, 1, 1)
    val d6_2 = new TypeUtils.SimpleDate(year_6 + 1, 1, 1)
    val discount = rand.nextDouble() * 0.07 + 0.02
    val quantity = rand.nextInt(2) + 24.0
    val p1_6 = new Predicate(schemaLineitem, "l_shipdate", TypeUtils.TYPE.DATE, d6_1, Predicate.PREDTYPE.GEQ)
    val p2_6 = new Predicate(schemaLineitem, "l_shipdate", TypeUtils.TYPE.DATE, d6_2, Predicate.PREDTYPE.LT)
    val p3_6 = new Predicate(schemaLineitem, "l_discount", TypeUtils.TYPE.DOUBLE, discount - 0.01, Predicate.PREDTYPE.GT)
    val p4_6 = new Predicate(schemaLineitem, "l_discount", TypeUtils.TYPE.DOUBLE, discount + 0.01, Predicate.PREDTYPE.LEQ)
    val p5_6 = new Predicate(schemaLineitem, "l_quantity", TypeUtils.TYPE.DOUBLE, quantity, Predicate.PREDTYPE.LEQ)

    val q_l = new Query(schemaLineitem.getAttributeId("l_suppkey"), Array(p1_6, p2_6, p3_6, p4_6, p5_6))


    val start = System.currentTimeMillis()

    val table_l = readTableWithQuery(sc, lineitem, q_l)
    val result = table_l.count()

    val end = System.currentTimeMillis()

    println("RES: Time Taken: " + (end - start) + " Result: " + result)
  }

  def tpch8(sc: SparkContext): Unit = {

    val rand_8_1 = rand.nextInt(regionNameVals.length)
    val r_name_8 = regionNameVals(rand_8_1)
    val d8_1 = new TypeUtils.SimpleDate(1995, 1, 1)
    val d8_2 = new TypeUtils.SimpleDate(1996, 12, 31)
    val p_type_8 = partTypeVals(rand.nextInt(partTypeVals.length))
    val p1_8 = new Predicate(schemaCustomer, "c_region", TypeUtils.TYPE.STRING, r_name_8, Predicate.PREDTYPE.LEQ)
    val p2_8 = new Predicate(schemaOrders, "o_orderdate", TypeUtils.TYPE.DATE, d8_1, Predicate.PREDTYPE.GEQ)
    val p3_8 = new Predicate(schemaOrders, "o_orderdate", TypeUtils.TYPE.DATE, d8_2, Predicate.PREDTYPE.LEQ)
    val p4_8 = new Predicate(schemaPart, "p_type", TypeUtils.TYPE.STRING, p_type_8, Predicate.PREDTYPE.EQ)

    val q_o = new Query(schemaOrders.getAttributeId("o_custkey"), Array(p2_8, p3_8))
    val q_p = new Query(schemaPart.getAttributeId("p_partkey"), Array(p4_8))
    val q_l = new Query(schemaLineitem.getAttributeId("l_partkey"), Global.EmptyPredicates)

    val q_c = {
      if (rand_8_1 > 0) {
        val r_name_prev_8 = regionNameVals(rand_8_1 - 1)
        val p5_8 = new Predicate(schemaCustomer, "c_region", TypeUtils.TYPE.STRING, r_name_prev_8, Predicate.PREDTYPE.GT)
        new Query(schemaCustomer.getAttributeId("c_custkey"), Array(p1_8, p5_8))
      } else {
        new Query(schemaCustomer.getAttributeId("c_custkey"), Array(p1_8))
      }
    }

    val start = System.currentTimeMillis()

    val table_l = readTableWithQuery(sc, lineitem, q_l)
    val table_o = readTableWithQuery(sc, orders, q_o)
    val table_c = readTableWithQuery(sc, customer, q_c)
    val table_p = readTableWithQuery(sc, part, q_p)

    val string_l_join_p = stringLineitem + ", " + stringPart
    val schema_l_join_p = Schema.createSchema(string_l_join_p)

    val table_l_join_p = joinTable(sc, table_l, table_p, schema_l_join_p.getAttributeId("l_orderkey"))

    val string_o_join_c = stringOrders + ", " + stringCustomer
    val schema_o_join_c = Schema.createSchema(string_o_join_c)

    val table_o_join_c = joinTable(sc, table_o, table_c, schema_o_join_c.getAttributeId("o_orderkey"))


    val table_l_join_o_join_c_join_p = joinTable(sc, table_l_join_p, table_o_join_c, -1)

    val result = table_l_join_o_join_c_join_p.count()

    //table_l_join_o_join_c_join_p.foreach( x => println("### " + x + " ###"))

    val end = System.currentTimeMillis()

    println("RES: Time Taken: " + (end - start) + " Result: " + result)
  }

  def tpch10(sc: SparkContext): Unit = {
    val l_returnflag_10 = "R"
    val l_returnflag_prev_10 = "N"
    val year_10 = 1993
    var monthOffset = rand.nextInt(24)
    val d10_1 = new TypeUtils.SimpleDate(year_10 + monthOffset / 12, monthOffset % 12 + 1, 1)
    monthOffset = monthOffset + 3
    val d10_2 = new TypeUtils.SimpleDate(year_10 + monthOffset / 12, monthOffset % 12 + 1, 1)
    val p1_10 = new Predicate(schemaLineitem, "l_returnflag", TypeUtils.TYPE.STRING, l_returnflag_10, Predicate.PREDTYPE.LEQ)
    val p4_10 = new Predicate(schemaLineitem, "l_returnflag", TypeUtils.TYPE.STRING, l_returnflag_prev_10, Predicate.PREDTYPE.GT)
    val p2_10 = new Predicate(schemaOrders, "o_orderdate", TypeUtils.TYPE.DATE, d10_1, Predicate.PREDTYPE.GEQ)
    val p3_10 = new Predicate(schemaOrders, "o_orderdate", TypeUtils.TYPE.DATE, d10_2, Predicate.PREDTYPE.LT)

    val q_l = new Query(schemaLineitem.getAttributeId("l_orderkey"), Array(p1_10, p4_10))
    val q_o = new Query(schemaOrders.getAttributeId("o_orderkey"), Array(p2_10, p3_10))
    val q_c = new Query(schemaCustomer.getAttributeId("c_custkey"), Global.EmptyPredicates)

    val start = System.currentTimeMillis()

    val table_l = readTableWithQuery(sc, lineitem, q_l)
    val table_o = readTableWithQuery(sc, orders, q_o)
    val table_c = readTableWithQuery(sc, customer, q_c)

    val string_l_join_o = stringLineitem + ", " + stringOrders
    val schema_l_join_o = Schema.createSchema(string_l_join_o)

    val table_l_join_o = joinTable(sc, table_l, table_o, schema_l_join_o.getAttributeId("o_custkey"))

    val table_l_join_o_join_c = joinTable(sc, table_l_join_o, table_c, -1)
    val result = table_l_join_o_join_c.count()

    //table_l_join_o_join_c.foreach( x => println("### " + x + " ###"))

    val end = System.currentTimeMillis()

    println("RES: Time Taken: " + (end - start) + " Result: " + result)
  }

  def tpch12(sc: SparkContext): Unit = {
    val rand_12 = rand.nextInt(shipModeVals.length)
    val shipmode_12 = shipModeVals(rand_12)
    val year_12 = 1993 + rand.nextInt(5)
    val d12_1 = new TypeUtils.SimpleDate(year_12, 1, 1)
    val d12_2 = new TypeUtils.SimpleDate(year_12 + 1, 1, 1)
    val p1_12 = new Predicate(schemaLineitem, "l_shipmode", TypeUtils.TYPE.STRING, shipmode_12, Predicate.PREDTYPE.LEQ)
    val p2_12 = new Predicate(schemaLineitem, "l_receiptdate", TypeUtils.TYPE.DATE, d12_1, Predicate.PREDTYPE.GEQ)
    val p3_12 = new Predicate(schemaLineitem, "l_receiptdate", TypeUtils.TYPE.DATE, d12_2, Predicate.PREDTYPE.LT)

    val q_o = new Query(schemaOrders.getAttributeId("o_orderkey"), Global.EmptyPredicates)

    val q_l = {
      if (rand_12 > 0) {
        val shipmode_prev_12 = shipModeVals(rand_12 - 1)
        val p4_12 = new Predicate(schemaLineitem, "l_shipmode", TypeUtils.TYPE.STRING, shipmode_prev_12, Predicate.PREDTYPE.GT)
        new Query(schemaLineitem.getAttributeId("l_orderkey"), Array(p1_12, p2_12, p3_12, p4_12))
      } else {
        new Query(schemaLineitem.getAttributeId("l_orderkey"), Array(p1_12, p2_12, p3_12))
      }
    }

    val start = System.currentTimeMillis()

    val table_l = readTableWithQuery(sc, lineitem, q_l)
    val table_o = readTableWithQuery(sc, orders, q_o)

    val table_l_join_o = joinTable(sc, table_l, table_o, -1)

    val result = table_l_join_o.count()

    val end = System.currentTimeMillis()

    //table_l_join_o.foreach( x => println("### " + x + " ###"))

    println("RES: Time Taken: " + (end - start) + " Result: " + result)

  }

  def tpch14(sc: SparkContext): Unit = {
    val year_14 = 1993
    var monthOffset_14 = rand.nextInt(60)
    val d14_1 = new TypeUtils.SimpleDate(year_14 + monthOffset_14 / 12, monthOffset_14 % 12 + 1, 1)
    monthOffset_14 += 1
    val d14_2 = new TypeUtils.SimpleDate(year_14 + monthOffset_14 / 12, monthOffset_14 % 12 + 1, 1)
    val p1_14 = new Predicate(schemaLineitem, "l_shipdate", TypeUtils.TYPE.DATE, d14_1, Predicate.PREDTYPE.GEQ)
    val p2_14 = new Predicate(schemaLineitem, "l_shipdate", TypeUtils.TYPE.DATE, d14_2, Predicate.PREDTYPE.LT)

    val q_l = new Query(schemaLineitem.getAttributeId("l_partkey"), Array(p1_14, p2_14))
    val q_p = new Query(schemaPart.getAttributeId("p_partkey"), Global.EmptyPredicates)

    val start = System.currentTimeMillis()

    val table_l = readTableWithQuery(sc, lineitem, q_l)
    val table_p = readTableWithQuery(sc, part, q_p)
    val table_l_join_p = joinTable(sc, table_l, table_p, -1)

    val result = table_l_join_p.count()

    val end = System.currentTimeMillis()

    //table_l_join_p.foreach( x => println("### " + x + " ###"))

    println("RES: Time Taken: " + (end - start) + " Result: " + result)
  }

  def tpch19(sc: SparkContext): Unit = {

    val brand_19 = "Brand#" + (rand.nextInt(5) + 1) + "" + (rand.nextInt(5) + 1)
    val shipInstruct_19 = "DELIVER IN PERSON"
    var quantity_19 = rand.nextInt(10) + 1
    val p1_19 = new Predicate(schemaLineitem, "l_shipinstruct", TypeUtils.TYPE.STRING, shipInstruct_19, Predicate.PREDTYPE.EQ)
    val p2_19 = new Predicate(schemaPart, "p_brand", TypeUtils.TYPE.STRING, brand_19, Predicate.PREDTYPE.EQ)
    val p3_19 = new Predicate(schemaPart, "p_container", TypeUtils.TYPE.STRING, "SM CASE", Predicate.PREDTYPE.EQ)
    val p4_19 = new Predicate(schemaLineitem, "l_quantity", TypeUtils.TYPE.DOUBLE, quantity_19, Predicate.PREDTYPE.GT)
    quantity_19 += 10
    val p5_19 = new Predicate(schemaLineitem, "l_quantity", TypeUtils.TYPE.DOUBLE, quantity_19, Predicate.PREDTYPE.LEQ)
    val p6_19 = new Predicate(schemaPart, "p_size", TypeUtils.TYPE.INT, 1, Predicate.PREDTYPE.GEQ)
    val p7_19 = new Predicate(schemaPart, "p_size", TypeUtils.TYPE.INT, 5, Predicate.PREDTYPE.LEQ)
    val p8_19 = new Predicate(schemaLineitem, "l_shipmode", TypeUtils.TYPE.STRING, "AIR", Predicate.PREDTYPE.LEQ)

    val q_l = new Query(schemaLineitem.getAttributeId("l_partkey"), Array(p1_19, p4_19, p5_19, p8_19))
    val q_p = new Query(schemaPart.getAttributeId("p_partkey"), Array(p2_19, p3_19, p6_19, p7_19))

    val start = System.currentTimeMillis()

    val table_l = readTableWithQuery(sc, lineitem, q_l)
    val table_p = readTableWithQuery(sc, part, q_p)
    val table_l_join_p = joinTable(sc, table_l, table_p, -1)

    val result = table_l_join_p.count()

    //table_l_join_p.foreach( x => println("### " + x + " ###"))

    val end = System.currentTimeMillis()

    println("RES: Time Taken: " + (end - start) + " Result: " + result)
  }


  def main(args: Array[String]) {

    val conf = new SparkConf()
    conf.setAppName("spark_partitioner")
    conf.setMaster("spark://istc13.csail.mit.edu:7077")

    val sc = new SparkContext(conf)

    lineitem = args(0)
    orders = args(1)
    customer = args(2)
    part = args(3)
    supplier = args(4)

    args(5) match {
      case "tpch3" => {
        tpch3(sc)
      }
      case "tpch5" => {
        tpch5(sc)
      }
      case "tpch6" => {
        tpch6(sc)
      }
      case "tpch8" => {
        tpch8(sc)
      }
      case "tpch10" => {
        tpch10(sc)
      }
      case "tpch12" => {
        tpch12(sc)
      }
      case "tpch14" => {
        tpch14(sc)
      }
      case "tpch19" => {
        tpch19(sc)
      }
    }

  }
}
