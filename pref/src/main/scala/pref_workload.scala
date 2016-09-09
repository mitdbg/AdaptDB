import java.util.{Calendar, GregorianCalendar}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

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

  val stringLineitem = "l_orderkey long, l_partkey int, l_suppkey int, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate date, l_commitdate date, l_receiptdate date, l_shipinstruct string, l_shipmode string, l_comment string"
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

  def readTableWithQuery(sc: SparkContext, table: String, query: Query): RDD[String] = {
    val partitionIdsRDD = sc.parallelize(0 to partitionNum - 1, partitionNum)
    val rdd = partitionIdsRDD.mapPartitionsWithIndex(
      (partitionId, values) => {
        val tablePath = "%s/part-%05d".format(table, partitionId)
        val fs = HDFSUtils.getFSByHadoopHome(hadoopHome)
        val tuples = HDFSUtils.readFile(fs, tablePath).split("\n")
        val filteredTuples = tuples.filter(x => query.qualifies(x))
        filteredTuples.iterator
      }, true
    )
    rdd
  }

  def readTable(sc: SparkContext, table: String): RDD[String] = {
    readTableWithQuery(sc, table, Global.EmptyQuery)
  }


  // reads a table from ``otherTable", joins with table ``baseTable" on joinKeyA = joinKeyB. Assume the datatype is long
  // query is applied to otherTable

  def joinTableWithQuery(sc: SparkContext, baseTable: RDD[String], otherTable: String, joinKeyA: Int, joinKeyB: Int, query: Query): RDD[String] = {

    val rdd = baseTable.mapPartitionsWithIndex(
      (partitionId, values) => {

        val tablePath = "%s/part-%05d".format(otherTable, partitionId)
        val fs = HDFSUtils.getFSByHadoopHome(hadoopHome)
        // build a hash table over otherTable

        val tuples = HDFSUtils.readFile(fs, tablePath).split("\n")
        val filteredTuples = tuples.filter(x => query.qualifies(x))

        val hashTable = filteredTuples.map(x => {
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

  def joinTable(sc: SparkContext, baseTable: RDD[String], otherTable: String, joinKeyA: Int, joinKeyB: Int): RDD[String] = {
    joinTableWithQuery(sc, baseTable, otherTable, joinKeyA, joinKeyB, Global.EmptyQuery)
  }


  // put all strings into a set and write back to hdfs

  def selectDistinct(table: RDD[String]): RDD[String] = {
    table.mapPartitions(x => x.toSet.iterator, true)
  }

  def saveTable(table: RDD[String], savePath: String): Unit = {
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


  def testJoinTable(sc: SparkContext): Unit = {
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

  def testJoinDistinctTable(sc: SparkContext): Unit = {
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

  def testSaveDistinctTable(sc: SparkContext): Unit = {

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

    val q_o = new Query(Array(p2_3))
    val q_l = new Query(Array(p3_3))


    val q_c = {
      if (rand_3 > 0) {
        val c_mktsegment_prev = mktSegmentVals(rand_3 - 1)
        val p4_3 = new Predicate(schemaCustomer, "c_mktsegment", TypeUtils.TYPE.STRING, c_mktsegment_prev, Predicate.PREDTYPE.GT)
        new Query(Array(p1_3, p4_3))
      } else {
        new Query(Array(p1_3))
      }
    }


    val start = System.currentTimeMillis()

    val table_l = readTableWithQuery(sc, lineitem, q_l)
    val table_l_join_o = joinTableWithQuery(sc, table_l, orders, schemaLineitem.getAttributeId("l_orderkey"), schemaOrders.getAttributeId("o_orderkey"), q_o)

    val string_l_join_o = stringLineitem + ", " + stringOrders
    val schema_l_join_o = Schema.createSchema(string_l_join_o)

    val table_l_join_o_join_c = joinTableWithQuery(sc, table_l_join_o, customer, schema_l_join_o.getAttributeId("o_custkey"), schemaCustomer.getAttributeId("c_custkey"), q_c)
    val result = table_l_join_o_join_c.count()

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

    val q_o = new Query(Array(p3_5, p4_5))
    val q_l = Global.EmptyQuery

    val q_c = {
      if (rand_5 > 0) {
        val r_name_prev_5 = regionNameVals(rand_5 - 1)
        val p5_5 = new Predicate(schemaCustomer, "c_region", TypeUtils.TYPE.STRING, r_name_prev_5, Predicate.PREDTYPE.GT)
        new Query(Array(p1_5, p5_5))
      } else {
        new Query(Array(p1_5))
      }
    }

    val q_s = {
      if (rand_5 > 0) {
        val r_name_prev_5 = regionNameVals(rand_5 - 1)
        val p6_5 = new Predicate(schemaSupplier, "s_region", TypeUtils.TYPE.STRING, r_name_prev_5, Predicate.PREDTYPE.GT)
        new Query(Array(p2_5, p6_5))
      } else {
        new Query(Array(p2_5))
      }
    }

    val start = System.currentTimeMillis()

    val table_l = readTableWithQuery(sc, lineitem, q_l)
    val table_l_join_o = joinTableWithQuery(sc, table_l, orders, schemaLineitem.getAttributeId("l_orderkey"), schemaOrders.getAttributeId("o_orderkey"), q_o)

    val string_l_join_o = stringLineitem + ", " + stringOrders
    val schema_l_join_o = Schema.createSchema(string_l_join_o)

    val table_l_join_o_join_c = joinTableWithQuery(sc, table_l_join_o, customer, schema_l_join_o.getAttributeId("o_custkey"), schemaCustomer.getAttributeId("c_custkey"), q_c)

    val string_l_join_o_join_c = string_l_join_o + ", " + stringCustomer
    val schema_l_join_o_join_c = Schema.createSchema(string_l_join_o_join_c)

    val table_l_join_o_join_c_join_s = joinTableWithQuery(sc, table_l_join_o_join_c, supplier, schema_l_join_o_join_c.getAttributeId("l_suppkey"), schemaSupplier.getAttributeId("s_suppkey"), q_s)

    val result = table_l_join_o_join_c_join_s.count()

    val end = System.currentTimeMillis()

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

    val q_l = new Query(Array(p1_6, p2_6, p3_6, p4_6, p5_6))


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

    val q_o = new Query(Array(p2_8, p3_8))
    val q_p = new Query(Array(p4_8))
    val q_l = Global.EmptyQuery

    val q_c = {
      if (rand_8_1 > 0) {
        val r_name_prev_8 = regionNameVals(rand_8_1 - 1)
        val p5_8 = new Predicate(schemaCustomer, "c_region", TypeUtils.TYPE.STRING, r_name_prev_8, Predicate.PREDTYPE.GT)
        new Query(Array(p1_8, p5_8))
      } else {
        new Query(Array(p1_8))
      }
    }

    val start = System.currentTimeMillis()

    val table_l = readTableWithQuery(sc, lineitem, q_l)
    val table_l_join_o = joinTableWithQuery(sc, table_l, orders, schemaLineitem.getAttributeId("l_orderkey"), schemaOrders.getAttributeId("o_orderkey"), q_o)

    val string_l_join_o = stringLineitem + ", " + stringOrders
    val schema_l_join_o = Schema.createSchema(string_l_join_o)

    val table_l_join_o_join_c = joinTableWithQuery(sc, table_l_join_o, customer, schema_l_join_o.getAttributeId("o_custkey"), schemaCustomer.getAttributeId("c_custkey"), q_c)

    val string_l_join_o_join_c = string_l_join_o + ", " + stringCustomer
    val schema_l_join_o_join_c = Schema.createSchema(string_l_join_o_join_c)

    val table_l_join_o_join_c_join_p = joinTableWithQuery(sc, table_l_join_o_join_c, part, schema_l_join_o_join_c.getAttributeId("l_partkey"), schemaPart.getAttributeId("p_partkey"), q_p)

    val result = table_l_join_o_join_c_join_p.count()

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

    val q_l = new Query(Array(p1_10, p4_10))
    val q_o = new Query(Array(p2_10, p3_10))
    val q_c = Global.EmptyQuery

    val start = System.currentTimeMillis()

    val table_l = readTableWithQuery(sc, lineitem, q_l)
    val table_l_join_o = joinTableWithQuery(sc, table_l, orders, schemaLineitem.getAttributeId("l_orderkey"), schemaOrders.getAttributeId("o_orderkey"), q_o)

    val string_l_join_o = stringLineitem + ", " + stringOrders
    val schema_l_join_o = Schema.createSchema(string_l_join_o)

    val table_l_join_o_join_c = joinTableWithQuery(sc, table_l_join_o, customer, schema_l_join_o.getAttributeId("o_custkey"), schemaCustomer.getAttributeId("c_custkey"), q_c)
    val result = table_l_join_o_join_c.count()

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

    val q_o = Global.EmptyQuery

    val q_l = {
      if (rand_12 > 0) {
        val shipmode_prev_12 = shipModeVals(rand_12 - 1)
        val p4_12 = new Predicate(schemaLineitem, "l_shipmode", TypeUtils.TYPE.STRING, shipmode_prev_12, Predicate.PREDTYPE.GT)
        new Query(Array(p1_12, p2_12, p3_12, p4_12))
      } else {
        new Query(Array(p1_12, p2_12, p3_12))
      }
    }

    val start = System.currentTimeMillis()

    val table_l = readTableWithQuery(sc, lineitem, q_l)
    val table_l_join_o = joinTableWithQuery(sc, table_l, orders, schemaLineitem.getAttributeId("l_orderkey"), schemaOrders.getAttributeId("o_orderkey"), q_o)

    val result = table_l_join_o.count()

    val end = System.currentTimeMillis()

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

    val q_l = new Query(Array(p1_14, p2_14))
    val q_p = Global.EmptyQuery

    val start = System.currentTimeMillis()

    val table_l = readTableWithQuery(sc, lineitem, q_l)
    val table_l_join_p = joinTableWithQuery(sc, table_l, part, schemaLineitem.getAttributeId("l_partkey"), schemaPart.getAttributeId("p_partkey"), q_p)

    val result = table_l_join_p.count()

    val end = System.currentTimeMillis()

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

    val q_l = new Query(Array(p1_19, p4_19, p5_19, p8_19))
    val q_p = new Query(Array(p2_19, p3_19, p6_19, p7_19))

    val start = System.currentTimeMillis()

    val table_l = readTableWithQuery(sc, lineitem, q_l)
    val table_l_join_p = joinTableWithQuery(sc, table_l, part, schemaLineitem.getAttributeId("l_partkey"), schemaPart.getAttributeId("p_partkey"), q_p)

    val result = table_l_join_p.count()

    val end = System.currentTimeMillis()

    println("RES: Time Taken: " + (end - start) + " Result: " + result)
  }

  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setAppName("pref_workload")
    conf.setMaster("spark://26-2-88.dynamic.csail.mit.edu:7077")
    conf.set("spark.cores.max", "2")

    val sc = new SparkContext(conf)

    args(0) match {
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
