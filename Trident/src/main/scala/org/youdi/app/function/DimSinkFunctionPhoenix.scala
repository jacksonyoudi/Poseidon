package org.youdi.app.function

import com.alibaba.fastjson.JSONObject
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.youdi.common.TridentConfig

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

class DimSinkFunctionPhoenix extends RichSinkFunction[JSONObject] {
  var connection: Connection = _

  override def open(parameters: Configuration): Unit = {
    Class.forName(TridentConfig.PHOENIX_DRIVER)
    connection = DriverManager.getConnection(TridentConfig.PHOENIX_SERVER)
    connection.setAutoCommit(true)
  }


  def genUpsertSql(sinkTable: String, data: JSONObject): String = {
    val keySets: util.Set[String] = data.keySet()
    val values: util.Collection[AnyRef] = data.values()

    val fields: String = keySets.mkString(",")
    var valueString: String = values.mkString("','")
    valueString = "'" + valueString + "'"


    "upsert into " + TridentConfig.Hbase_SCHEMA + "." + sinkTable + "( " + fields + ") values ( " + valueString + ")"

  }

  // value {"sinkTable":"base_trademark_dim","database":"poseidon","before":{},"after":{"tm_name":"test"},"type":"insert","table":"base_trademark"}
  // sql
  override def invoke(value: JSONObject, context: SinkFunction.Context): Unit = {
    // 获取 sql语句
    try {
      var upsertSql: String = genUpsertSql(value.getString("sinkTable"), value.getJSONObject("after"))
      println(upsertSql)

      // 预编译
      val statement: PreparedStatement = connection.prepareStatement(upsertSql)


      // 执行插入操作
      statement.executeUpdate()
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }
}

