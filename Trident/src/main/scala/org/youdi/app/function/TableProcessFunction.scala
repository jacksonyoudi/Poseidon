package org.youdi.app.function

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.state.{BroadcastState, MapStateDescriptor, ReadOnlyBroadcastState}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.youdi.bean.TableProcess
import org.youdi.common.TridentConfig

import java.sql.{Connection, DriverManager, PreparedStatement, SQLException}

class TableProcessFunction(tag: OutputTag[JSONObject], state: MapStateDescriptor[String, TableProcess]) extends BroadcastProcessFunction[JSONObject, String, JSONObject] {
  var connection: Connection = _
  var streamTag: OutputTag[JSONObject] = tag
  var stateDesc: MapStateDescriptor[String, TableProcess] = state


  override def open(parameters: Configuration): Unit = {
    Class.forName(TridentConfig.PHOENIX_DRIVER)
    connection = DriverManager.getConnection(TridentConfig.PHOENIX_SERVER)
  }

  private def filterColumn(data: JSONObject, sinkColumns: String): Unit = {
    val colums: Set[String] = sinkColumns.split(",").toSet

    //    val iterator: util.Iterator[Map.Entry[String, AnyRef]] = data.entrySet().iterator()

    //    while (iterator.hasNext) {
    //      val next: Map.Entry[String, AnyRef] = iterator.next()
    //      if (!colums.contains(next.getKey)) {
    //        iterator.remove()
    //      }
    //    }

    data.entrySet().removeIf(next => !colums.contains(next.getKey()));

  }

  override def processElement(value: JSONObject, ctx: BroadcastProcessFunction[JSONObject, String, JSONObject]#ReadOnlyContext, out: Collector[JSONObject]): Unit = {
    // 获取广播流
    val rbs: ReadOnlyBroadcastState[String, TableProcess] = ctx.getBroadcastState(stateDesc)
    val key: String = value.getString("tableName") + "-" + value.getString("type")

    val process: TableProcess = rbs.get(key)
    if (process != null) {
      val data: JSONObject = value.getJSONObject("after")
      filterColumn(data, process.sinkColumns)

      // 分流
      if (process.sinkType.equals(TableProcess.SINK_TYPE_KaFKA)) {


        out.collect(value)
      } else if (process.sinkType.equals(TableProcess.SINK_TYPE_HBASE)) {


        ctx.output(streamTag, value)
      }


    } else {
      println(key + " no exits")
    }

  }

  // 建表语句  create table if not exists db.tn(id varchar primary key, kk varchar )xxx;
  private def checkTable(sinkTable: String, sinkColumns: String, sinkPkS: String, sinkExtend: String) = {
    var statement: PreparedStatement = null
    try {
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

      println(bf.toString)


      statement = connection.prepareStatement(bf.toString)
      statement.execute()

    } catch {
      case e: SQLException => {
        throw new RuntimeException("phoenix " + sinkTable + " 建表失败! ")
      }
    } finally {
      if (statement != null) {
        statement.close()
      }
    }


  }

  override def processBroadcastElement(value: String, ctx: BroadcastProcessFunction[JSONObject, String, JSONObject]#Context, out: Collector[JSONObject]): Unit = {
    val js: JSONObject = JSON.parseObject(value)
    val data: String = js.getString("after")
    val tableprocess: TableProcess = JSON.parseObject(data, TableProcess.getClass)

    // 建表
    print("table........")
    if (tableprocess.sinkType.equals(TableProcess.SINK_TYPE_HBASE)) {
      checkTable(tableprocess.sinkTable, tableprocess.sinkColumns, tableprocess.sinkPk, tableprocess.sinkExtend)
    }

    // 写入状态，广播出去
    val broadCast: BroadcastState[String, TableProcess] = ctx.getBroadcastState(stateDesc)
    val key: String = tableprocess.sourceTable + "-" + tableprocess.operationType

    broadCast.put(key, tableprocess)

  }
}
