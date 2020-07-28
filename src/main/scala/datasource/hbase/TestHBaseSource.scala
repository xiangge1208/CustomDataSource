package datasource.hbase

import org.apache.spark.sql.SparkSession

object TestHBaseSource {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("TestHBaseSource")
      .master("local[*]")
      .getOrCreate()

    val hbaseTable = spark.read
      .format("datasource.hbase.HBaseSource")
      .options(Map(
        "sparksql_table_schema" -> "(key String, math String, chinese String, name String)",
        "hbase_table_name" -> "tblStudent",
        "row_range" -> "001 -> 004",  //row [1,3)
        "time_range" -> "1595921586470 -> 1595921588438", //row [2,4]
        "hbase_table_schema" -> "(:key, Grade:math, Grade:chinese, Info:name)"
      )).load()

    hbaseTable.printSchema()
    hbaseTable.show()

//    Thread.sleep(10000000)

  }
}
