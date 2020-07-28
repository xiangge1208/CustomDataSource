package datasource.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


case class HBaseRelation(sqlContext: SQLContext, parameters: Map[String, String]) extends BaseRelation with Serializable with TableScan {

  val hbaseTableName: String = parameters.getOrElse("hbase_table_name", sys.error("not valid schema"))
  val hbaseTableSchema: String = parameters.getOrElse("hbase_table_schema", sys.error("not valid schema"))
  val registerTableSchema: String = parameters.getOrElse("sparksql_table_schema", sys.error("not valid schema"))
  val rowKey_Range: String = parameters.getOrElse("row_range", "->")
  val time_Range: String = parameters.getOrElse("time_range", "->")
  //get start row and end row
  val rowKeyRange: Array[String] = rowKey_Range.split("->", -1)
  val startRowKey: String = rowKeyRange(0).trim
  val endRowKey: String = rowKeyRange(1).trim
  //get start timestemp and end timestemp
  val timeRange: Array[String] = time_Range.split("->", -1)
  val startTime: String = timeRange(0).trim
  val endTime: String = timeRange(1).trim
  protected val tempHBaseFields: Array[HBaseSchemaField] = extractHBaseSchema(hbaseTableSchema) //do not use this, a temp field
  val registerTableFields: Array[RegisteredSchemaField] = extractRegisterSchema(registerTableSchema)
  val tempFieldRelation: mutable.LinkedHashMap[HBaseSchemaField, RegisteredSchemaField] = tableSchemaFieldMapping(tempHBaseFields, registerTableFields)
  val hbaseTableFields: Array[HBaseSchemaField] = feedType(tempFieldRelation)
  val fieldsRelations: mutable.Map[HBaseSchemaField, RegisteredSchemaField] = tableSchemaFieldMapping(hbaseTableFields, registerTableFields)
  val queryColumns: String = getQueryTargetCloumns(hbaseTableFields)


  def feedType[T <: mutable.LinkedHashMap[HBaseSchemaField, RegisteredSchemaField]](zipSchema: mutable.LinkedHashMap[HBaseSchemaField, RegisteredSchemaField]): Array[HBaseSchemaField] = {
    val finalHbaseSchema: mutable.Iterable[HBaseSchemaField] = zipSchema.map {
      case (hbaseSchema, registerSchema) =>
        hbaseSchema.copy(fieldType = registerSchema.fieldType)
    }
    finalHbaseSchema.toArray[HBaseSchemaField]
  }


  def isRowKey(field: HBaseSchemaField): Boolean = {
    val cfColArray = field.fieldName.split(":", -1)
    val cfName = cfColArray(0)
    val colName = cfColArray(1)
    if (cfName == "" && colName == "key") true else false
  }


  def getQueryTargetCloumns(hbaseTableFields: Array[HBaseSchemaField]): String = {
    val str = ArrayBuffer[String]()
    hbaseTableFields.foreach { field =>
      if (!isRowKey(field)) {
        str.append(field.fieldName)
      }
    }
    println(str.mkString(" "))
    str.mkString(" ")
  }

  lazy val schema: StructType = {
    val fields = hbaseTableFields.map { field =>
      val name = fieldsRelations.getOrElse(field, sys.error("table schema is not match the definition.")).fieldName
      val relatedType = field.fieldType match {
        case "String" =>
          SchemaType(StringType, nullable = true)
        case "Int" =>
          SchemaType(IntegerType, nullable = true)
        case "Long" =>
          SchemaType(LongType, nullable = true)
        case "Double" =>
          SchemaType(DoubleType, nullable = true)

      }
      StructField(name, relatedType.dataType, relatedType.nullable)
    }
    StructType(fields)
  }


  def tableSchemaFieldMapping[T <: Array[HBaseSchemaField], S <: Array[RegisteredSchemaField]](tmpHbaseSchemaField: Array[HBaseSchemaField], registerTableFields: Array[RegisteredSchemaField]): mutable.LinkedHashMap[HBaseSchemaField, RegisteredSchemaField] = {
    if (tmpHbaseSchemaField.length != registerTableFields.length) {
      sys.error("两个scchema不一致")
    }
    //把两个表拉链起来HBaseSchemaField
    val zip: Array[(HBaseSchemaField, RegisteredSchemaField)] = tmpHbaseSchemaField.zip(registerTableFields)
    //封装到有序的map里面
    val map = new mutable.LinkedHashMap[HBaseSchemaField, RegisteredSchemaField]
    for (arr <- zip) {
      map.put(arr._1, arr._2)
    }
    map
  }

  /**
   * spark sql schema will be register
   * registerTableSchema   '(rowkey string, value string, column_a string)'
   */
  def extractRegisterSchema(registerTableSchema: String): Array[RegisteredSchemaField] = {
    val fieldsStr = registerTableSchema.trim.drop(1).dropRight(1)
    val fieldsArray = fieldsStr.split(",").map(_.trim) //sorted
    fieldsArray.map { fieldString =>
      val splitedField = fieldString.split("\\s+", -1) //sorted
      RegisteredSchemaField(splitedField(0), splitedField(1))
    }
  }


  def extractHBaseSchema(externalTableSchema: String): Array[HBaseSchemaField] = {
    val fieldsStr = externalTableSchema.trim.drop(1).dropRight(1)
    val fieldsArray = fieldsStr.split(",").map(_.trim)
    fieldsArray.map(fildString => HBaseSchemaField(fildString, ""))
  }

  // By making this a lazy val we keep the RDD around, amortizing the cost of locating splits.
  lazy val buildScan: RDD[Row] = {
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "192.168.131.11")
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.set(TableInputFormat.INPUT_TABLE, hbaseTableName)
    hbaseConf.set(TableInputFormat.SCAN_COLUMNS, queryColumns)
    hbaseConf.set(TableInputFormat.SCAN_ROW_START, startRowKey)
    hbaseConf.set(TableInputFormat.SCAN_ROW_STOP, endRowKey)
    hbaseConf.set(TableInputFormat.SCAN_TIMERANGE_START, startTime)
    hbaseConf.set(TableInputFormat.SCAN_TIMERANGE_END, endTime)

    val hbaseRdd: RDD[(ImmutableBytesWritable, Result)] = sqlContext.sparkContext.newAPIHadoopRDD(
      hbaseConf,
      classOf[org.apache.hadoop.hbase.mapreduce.TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result]
    )

    val rs = hbaseRdd.map(_._2).map((result: Result) => {
      var values = new ArrayBuffer[Any]()
      hbaseTableFields.foreach { field =>
        values += Resolver.resolve(field, result)
      }
      Row.fromSeq(values)
    })
    rs
  }

  private case class SchemaType(dataType: DataType, nullable: Boolean)

}

