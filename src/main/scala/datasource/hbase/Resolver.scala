package datasource.hbase

import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes


object Resolver extends Serializable {
  def resolve(hbaseField: HBaseSchemaField, result: Result): Any = {
    val cfColArray = hbaseField.fieldName.split(":", -1)
    val cfName = cfColArray(0)
    val colName = cfColArray(1)
    var fieldRs: Any = null
    //resolve row key otherwise resolve column
    if (cfName == "" && colName == "key") {
      fieldRs = resolveRowKey(result, hbaseField.fieldType)
    } else {
      fieldRs = resolveColumn(result, cfName, colName, hbaseField.fieldType)
    }
    fieldRs
  }

  def resolveRowKey(result: Result, resultType: String): Any = {
    val rowkey = resultType match {
      case "String" =>
        result.getRow.map(_.toChar).mkString
      case "Int" =>
        result.getRow.map(_.toChar).mkString.toInt
      case "Long" =>
        result.getRow.map(_.toChar).mkString.toLong
      case "Float" =>
        result.getRow.map(_.toChar).mkString.toLong
      case "Double" =>
        result.getRow.map(_.toChar).mkString.toDouble
    }
    rowkey
  }

  def resolveColumn(result: Result, columnFamily: String, columnName: String, resultType: String): Any = {

    val column = if (result.containsColumn(columnFamily.getBytes, columnName.getBytes)) {
      resultType match {
        case "String" =>
          Bytes.toString(result.getValue(columnFamily.getBytes, columnName.getBytes))
        case "Int" =>
          Bytes.toInt(result.getValue(columnFamily.getBytes, columnName.getBytes))
        case "Long" =>
          Bytes.toLong(result.getValue(columnFamily.getBytes, columnName.getBytes))
        case "Float" =>
          Bytes.toFloat(result.getValue(columnFamily.getBytes, columnName.getBytes))
        case "Double" =>
          Bytes.toDouble(result.getValue(columnFamily.getBytes, columnName.getBytes))

      }
    } else {
      resultType match {
        case "String" =>
          ""
        case "Int" =>
          0
        case "Long" =>
          0
        case "Double" =>
          0.0
      }
    }
    column
  }
}