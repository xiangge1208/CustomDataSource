package datasource.rest

import java.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.reader.{DataReaderFactory, DataSourceReader}
import org.apache.spark.sql.types.StructType


/**
 * 创建RestDataSourceReader
 *
 * @param url          REST服务的的api
 * @param params       请求需要的参数
 * @param xPath        JSON数据的xPath
 * @param schemaString 用户传入的schema字符串
 */
class RestDataSourceReader(url: String, params: String, xPath: String, schemaString: String, method: String) extends DataSourceReader {

  // 使用StructType.fromDDL方法将schema字符串转成StructType类型
    val schema: StructType = StructType.fromDDL(schemaString)
//    val schema: StructType = SchemaUtil.generateStructType(schemaString)
  //根据用户给定的schemaString推导出一个JSON structure，再生成StructType
//  lazy val schema: StructType = Try(DataType.fromJson(schemaString)).getOrElse(LegacyTypeStringParser.parse(schemaString)) match {
//    case t: StructType => t
//    case _:Any => throw new RuntimeException(s"Failed parsing StructType: $schemaString")
//  }




  /**
   * 生成schema
   *
   * @return schema
   */
  override def readSchema(): StructType = schema

  /**
   * 创建工厂类
   *
   * @return List[实例]
   */
  override def createDataReaderFactories(): util.List[DataReaderFactory[Row]] = {
    import collection.JavaConverters._
    Seq(
      new RestDataReaderFactory(url, params, xPath, method).asInstanceOf[DataReaderFactory[Row]]
    ).asJava
  }
}
