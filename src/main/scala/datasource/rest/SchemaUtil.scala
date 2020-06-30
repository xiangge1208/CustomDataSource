package datasource.rest

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.types.{Metadata, StringType, StructField, StructType}


object SchemaUtil {

  /**
   * 从字段映射关系中获取目标资源库的StructType
   * @param schema 字符串字段
   * @return StructType
   */
  def generateStructType(schema: String): StructType = {
    val json = JSON.parseObject(schema).asInstanceOf[Map[String, String]]
    val targetColumns = json.values.toArray
    val fields = targetColumns.map(x => {
      StructField(x.toString, StringType, nullable = true, Metadata.empty)
    })
    StructType(fields)
  }

}
