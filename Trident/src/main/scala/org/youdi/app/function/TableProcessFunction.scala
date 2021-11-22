package org.youdi.app.function

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.state.{BroadcastState, MapStateDescriptor, ReadOnlyBroadcastState}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.apache.hadoop.conf
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}
import org.youdi.bean.{TableProcess, TableProcessConfig}
import org.youdi.common.TridentConfig
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

import java.io.IOException


class TableProcessFunction(tag: OutputTag[JSONObject], state: MapStateDescriptor[String, TableProcess]) extends BroadcastProcessFunction[JSONObject, String, JSONObject] {
  var connection: Connection = _
  var streamTag: OutputTag[JSONObject] = tag
  var stateDesc: MapStateDescriptor[String, TableProcess] = state


  override def open(parameters: Configuration): Unit = {
    val configuration: conf.Configuration = HBaseConfiguration.create()

    configuration.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181")
    configuration.set(HConstants.ZOOKEEPER_QUORUM, "localhost")
    configuration.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/hbase")

    println("connect starting....")
    try {
      connection = ConnectionFactory.createConnection(configuration)
    } catch {
      case e: IOException =>
        e.printStackTrace()
    }
    println("connect completed")
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


    val key: String = value.getString("table") + "-" + value.getString("type")

    val process: TableProcess = rbs.get(key)
    if (process != null) {
      val data: JSONObject = value.getJSONObject("after")
      filterColumn(data, process.sinkColumns)

      value.put("sinkTable", process.sinkTable)
      // 分流
      if (process.sinkType.equals(TableProcessConfig.SINK_TYPE_KAFKA)) {

        out.collect(value)
      } else if (process.sinkType.equals(TableProcessConfig.SINK_TYPE_HBASE)) {
        // 写入侧输出流
        value.put("rowkey", process.sinkPk)
        ctx.output(streamTag, value)
      }

    } else {
      println(key + " no exits")
    }

  }

  // 建表语句  create table if not exists db.tn(id varchar primary key, kk varchar )xxx;
  private def checkTable(sinkTable: String, sinkColumns: String, sinkPkS: String, sinkExtend: String) = {
    var admin: Admin = connection.getAdmin

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

      println("check table")
      if (admin.tableExists(TableName.valueOf(sinkTable))) {
        println(sinkTable + " is exist!")
      } else {
        println("create table")
        val tableBuilder: TableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(sinkTable))
        //        strings.toList.foreach {
        //          columnFamily: String => {
        //            val cfDesc: ColumnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamily))
        //            cfDesc.setMaxVersions(1)
        //            val build: ColumnFamilyDescriptor = cfDesc.build
        //            tableBuilder.setColumnFamily(build)
        //          }
        //        }

        // 只创建一个 family
        val cfDesc: ColumnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("a"))
        cfDesc.setMaxVersions(1)
        val build: ColumnFamilyDescriptor = cfDesc.build
        tableBuilder.setColumnFamily(build)

        admin.createTable(tableBuilder.build)
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally {
      admin.close()
    }


  }

  override def processBroadcastElement(value: String, ctx: BroadcastProcessFunction[JSONObject, String, JSONObject]#Context, out: Collector[JSONObject]): Unit = {
    val js: JSONObject = JSON.parseObject(value)
    val data: String = js.getString("after")
    println(data)
    val tableprocess: TableProcess = JSON.parseObject(data, TableProcess().getClass)

    // 建表
    if (tableprocess.sinkType.equals(TableProcessConfig.SINK_TYPE_HBASE)) {
      checkTable(tableprocess.sinkTable, tableprocess.sinkColumns, tableprocess.sinkPk, tableprocess.sinkExtend)
    }

    // 写入状态，广播出去
    val broadCast: BroadcastState[String, TableProcess] = ctx.getBroadcastState(stateDesc)
    val key: String = tableprocess.sourceTable + "-" + tableprocess.operateType

    broadCast.put(key, tableprocess)

  }
}
