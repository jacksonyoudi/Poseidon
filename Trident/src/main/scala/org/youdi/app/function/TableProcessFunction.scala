package org.youdi.app.function

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.state.{BroadcastState, MapStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.youdi.bean.TableProcess
import org.youdi.common.TridentConfig
import java.sql.{Connection, DriverManager}

class TableProcessFunction(tag: OutputTag[JSONObject], state: MapStateDescriptor[String, TableProcess]) extends BroadcastProcessFunction[JSONObject, String, JSONObject] {
  var connection: Connection = _
  var streamTag: OutputTag[JSONObject] = tag
  var stateDesc: MapStateDescriptor[String, TableProcess] = state


  override def open(parameters: Configuration): Unit = {
    Class.forName(TridentConfig.PHOENIX_DRIVER)
    connection = DriverManager.getConnection(TridentConfig.PHOENIX_SERVER)
  }

  override def processElement(value: JSONObject, ctx: BroadcastProcessFunction[JSONObject, String, JSONObject]#ReadOnlyContext, out: Collector[JSONObject]): Unit = {


  }

  // 建表语句  create table if not exists db.tn(id varchar primary key, kk varchar )xxx;
  private def checkTable(sinkTable: String, sinkColumns: String, sinkPkS: String, sinkExtend: String) = {
    var sinkPk: String = sinkPkS

    if (sinkPk == null) {
      sinkPk = "id"
    }


    val bf: StringBuffer = new StringBuffer("create table if not exists")
      .append(TridentConfig.Hbase_SCHEMA)
      .append(".")
      .append(sinkTable)
      .append("(")

    val strings: Array[String] = sinkColumns.split(",")

    for ((field, index) <- strings.zipWithIndex) {

      // 判断是否是主键
      if (field.equals(sinkPk)) {
        bf.append(field).append(" varchar primary key")
      } else {
        bf.append(field).append(" varchar")
      }

      // 判断是否是最后一个字段
      if (index < strings.length - 1) {
        bf.append(",")
      }
    }
    bf.append(") ")

    if (sinkExtend != null) {
      bf.append(sinkExtend)
    }

  }

  override def processBroadcastElement(value: String, ctx: BroadcastProcessFunction[JSONObject, String, JSONObject]#Context, out: Collector[JSONObject]): Unit = {
    val js: JSONObject = JSON.parseObject(value)
    val data: String = js.getString("after")
    val tableprocess: TableProcess = JSON.parseObject(data, TableProcess.getClass)

    // 建表
    if (tableprocess.sinkType.equals(TableProcess.SINK_TYPE_HBASE)) {
      checkTable(tableprocess.sinkTable, tableprocess.sinkColumn, tableprocess.sinkPk, tableprocess.sinkExtend)
    }

    // 写入状态，广播出去
    val broadCast: BroadcastState[String, TableProcess] = ctx.getBroadcastState(stateDesc)
    val key: String = tableprocess.sourceTable + "-" + tableprocess.operationType

    broadCast.put(key, tableprocess)

  }
}
