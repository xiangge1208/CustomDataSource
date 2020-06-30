package datasource.rest

import com.alibaba.fastjson.JSON
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions.mapAsScalaMap


object TestRestDataSource {


  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()

    val df = spark.read
      .format("datasource.rest.RestDataSource")
      .option("url", "https://free-api.heweather.net/s6/weather/forecast")
      .option("params", getParm(args(0)))
      .option("xPath", "$.HeWeather6[0].daily_forecast")
      .option("method", "get")
      .option("schema", "`cond_code_d` String,`cond_code_n` String," +
        "`cond_txt_d` String,`cond_txt_n` String,`date` String,`hum` String," +
        "`mr` String,`ms` String,`pcpn` String,`pop` String,`pres` String," +
        "`sr` String,`ss` String,`tmp_max` String,`tmp_min` String," +
        "`uv_index` String,`vis` String,`wind_deg` String," +
        "`wind_dir` String,`wind_sc` String,`wind_spd` String")
      .load()
      .select("cond_code_d","cond_code_n","cond_txt_d")


    df.printSchema()
//    df.rdd.map(x => (x,1)).groupByKey().map()
    df.show(false)

  }

  def getParm(parm: String): String = {
    val sb = new StringBuffer()
    sb.append("?")
    var parmStr = ""
    for ((k, v) <- JSON.parseObject(parm)) {
      parmStr =
        StringUtils.substringBeforeLast(sb.append(k + "=" + v + "&").toString, "&")
    }
    parmStr
  }
}
