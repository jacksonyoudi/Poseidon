package org.youdi.app.function

import com.alibaba.fastjson.JSONObject
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.hadoop.conf
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.youdi.utils.DimUtil

import java.io.IOException

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

  // value {"sinkTable":"base_trademark_dim","database":"poseidon","before":{},"after":{"tm_name":"test"},"type":"insert","table":"base_trademark"}
  // 保存一个 rowkey
  override def invoke(value: JSONObject, context: SinkFunction.Context): Unit = {
    try {
      val table: Table = connection.getTable(TableName.valueOf(value.getString("sinkTable")))

      val jSONObject: JSONObject = value.getJSONObject("after")

      // 判断 如果是更新数据,就删除数据 可以直接更新,注意过期时间
      if (value.getString("type").equals("update")) {
        DimUtil.delRedisDimInfo(value.getString("sinkType"), jSONObject.getString("id"))
      }

      val put: Put = new Put(Bytes.toBytes(jSONObject.getString(value.getString("rowkey"))))
      println(jSONObject.toString)

      jSONObject.entrySet().forEach(
        v => {
          println(v.toString)
          put.addColumn(Bytes.toBytes("a"), Bytes.toBytes(v.getKey.toString), Bytes.toBytes(v.getValue.toString))
        }
      )
      table.put(put)
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }
}

