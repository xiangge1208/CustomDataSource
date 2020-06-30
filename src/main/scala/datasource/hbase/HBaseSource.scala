package datasource.hbase

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.RelationProvider

import scala.collection.immutable.Map


class HBaseSource extends RelationProvider {
  def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): HBaseRelation = HBaseRelation(sqlContext, parameters)

}
