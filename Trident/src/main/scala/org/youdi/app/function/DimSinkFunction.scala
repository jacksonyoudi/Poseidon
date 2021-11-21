package org.youdi.app.function

import com.alibaba.fastjson.JSONObject
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.hadoop.conf
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.youdi.common.TridentConfig

import java.io.IOException
import java.util
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

class DimSinkFunction extends RichSinkFunction[JSONObject] {
  var connection: Connection = _

  override def open(parameters: Configuration): Unit = {
    val configuration: conf.Configuration = HBaseConfiguration.create()

    configuration.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181")
    configuration.set(HConstants.ZOOKEEPER_QUORUM, "localhost")
    configuration.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/hbase")

    try {
      connection = ConnectionFactory.createConnection(configuration)
    } catch {
      case e: IOException =>
        e.printStackTrace()
    }
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
  // 保存一个 rowkey
  override def invoke(value: JSONObject, context: SinkFunction.Context): Unit = {
    try {
      val table: Table = connection.getTable(TableName.valueOf(TridentConfig.Hbase_SCHEMA + "." + value.getString("sinkTable")))
      val put: Put = new Put(Bytes.toBytes(value.getString("rowkey")))

      val jSONObject: JSONObject = value.getJSONObject("after")

      jSONObject.entrySet().forEach(
        v => {
          put.addColumn(Bytes.toBytes("a"), Bytes.toBytes(v.getKey.toString), Bytes.toBytes(v.getValue.toString))
        }
      )
      table.put(put)
      table.close()
    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally {
      connection.close()
    }


  }
}

