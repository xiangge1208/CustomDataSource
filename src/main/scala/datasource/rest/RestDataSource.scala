package datasource.rest

import com.alibaba.fastjson.{JSONArray, JSONPath}
import org.apache.http.client.methods._
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}

import scala.collection.JavaConverters._

class RestDataSource extends DataSourceV2 with ReadSupport {
  override def createReader(options: DataSourceOptions): DataSourceReader = {
    new RestDataSourceReader(
      options.get("url").get(),
      options.get("params").get(),
      options.get("xPath").get(),
      options.get("schema").get(),
      options.get("method").get()
    )
  }
}

object RestDataSource {
  def requestData(url: String, params: String, xPath: String, method: String): List[Any] = {
    @transient var responseEntity = ""
    method match {
      case "post" =>
        val client = HttpClientBuilder.create.build
        val httpPost = new HttpPost(url)
        httpPost.setEntity(new StringEntity(params))
        val response = client.execute(httpPost)
        responseEntity = EntityUtils.toString(response.getEntity)
      case "get" =>
        if (params.isEmpty || params.equals("")) {
          val client = HttpClientBuilder.create.build
          val httpGet = new HttpGet(url)
          val response = client.execute(httpGet)
          responseEntity = EntityUtils.toString(response.getEntity)
        } else {
          val client = HttpClientBuilder.create.build
          val httpGet = new HttpGet(url + params)
          val response = client.execute(httpGet)
          responseEntity = EntityUtils.toString(response.getEntity)
        }
      case _ =>
        throw new RuntimeException(s"Unsupported request method : $method")
    }
    JSONPath.read(responseEntity, xPath).asInstanceOf[JSONArray].asScala.toList
  }


}
