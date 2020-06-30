package datasource.rest

import com.alibaba.fastjson.JSONObject
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.reader.DataReader

/**
 * RestDataReader类
 *
 * @param url    REST服务的的api
 * @param params 请求需要的参数
 * @param xPath  JSON数据的xPath
 */
class RestDataReader(url: String, params: String, xPath: String, method: String) extends DataReader[Row] {
  // 使用Iterator模拟数据
  val data: Iterator[List[Any]] = getIterator

  override def next(): Boolean = {
    data.hasNext
  }

  override def get(): Row = {
    val seq = data.next().map {
      // 浮点类型会自动转为BigDecimal，导致Spark无法转换
      case decimal: BigDecimal =>
        decimal.doubleValue()
      case x => x
    }
    Row(seq: _*)
  }

  override def close(): Unit = {
    println("close source")
  }

  def getIterator: Iterator[List[Any]] = {
    import scala.collection.JavaConverters._
    val res: List[Any] = RestDataSource.requestData(url, params, xPath, method)
    res.map(r => {
      r.asInstanceOf[JSONObject].asScala.values.toList
    }).toIterator
  }


}
