package datasource.rest

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory}

/**
 * RestDataReaderFactory工厂类
 *
 * @param url    REST服务的的api
 * @param params 请求需要的参数
 * @param xPath  JSON数据的xPath
 */
class RestDataReaderFactory(url: String, params: String, xPath: String, method: String) extends DataReaderFactory[Row] {
  override def createDataReader(): DataReader[Row] = new RestDataReader(url, params, xPath, method)
}
