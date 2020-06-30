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
        "sparksql_table_schema" -> "(key String, math String, chinese String, english String, age String, name String)",
        "hbase_table_name" -> "tblStudent",
        "hbase_table_schema" -> "(:key, Grade:math, Grade:chinese, Grade:english, Info:age, Info:name)"
      )).load()

    hbaseTable.printSchema()
    hbaseTable.show()

    Thread.sleep(10000000)

  }
}
