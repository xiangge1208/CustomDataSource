package datasource

import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.collection.mutable

package object hbase {

  abstract class SchemaField extends Serializable

  case class RegisteredSchemaField(fieldName: String, fieldType: String) extends SchemaField with Serializable

  case class HBaseSchemaField(fieldName: String, fieldType: String) extends SchemaField with Serializable

  case class Parameter(name: String)

  //sparksql_table_schema
  protected val SPARK_SQL_TABLE_SCHEMA: Parameter = Parameter("sparksql_table_schema")
  protected val HBASE_TABLE_NAME: Parameter = Parameter("hbase_table_name")
  protected val HBASE_TABLE_SCHEMA: Parameter = Parameter("hbase_table_schema")
  protected val ROW_RANGE: Parameter = Parameter("row_range")

  /**
   * Adds a method, `hbaseTable`, to SQLContext that allows reading data stored in hbase table.
   */
  implicit class HBaseContext(sqlContext: SQLContext) {
    def hbaseTable(sparksqlTableSchema: String, hbaseTableName: String, hbaseTableSchema: String, rowRange: String = "->"): DataFrame = {
      var params = new mutable.HashMap[String, String]
      params += (SPARK_SQL_TABLE_SCHEMA.name -> sparksqlTableSchema)
      params += (HBASE_TABLE_NAME.name -> hbaseTableName)
      params += (HBASE_TABLE_SCHEMA.name -> hbaseTableSchema)
      //get star row and end row
      params += (ROW_RANGE.name -> rowRange)
      sqlContext.baseRelationToDataFrame(HBaseRelation(sqlContext, params.asInstanceOf[Map[String, String]]))
    }
  }

}
