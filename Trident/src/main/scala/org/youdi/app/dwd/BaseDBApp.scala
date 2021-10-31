package org.youdi.app.dwd

import com.alibaba.fastjson.{JSON, JSONObject}
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import org.youdi.app.function.{CDCDeserialization, TableProcessFunction}
import org.youdi.bean.TableProcess
import org.youdi.utils.KafkaUtils

object BaseDBApp {
  def main(args: Array[String]): Unit = {
    // 获取配置
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 开启checkpointing

    env.setStateBackend(new RocksDBStateBackend("file:///opt/module/applog/gmall2020/BaseLogApp/"))

    env.enableCheckpointing(5000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(10000L)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(3000)

    // 消费ods_base_db 主题数据创建流
    val sourceTopic: String = "消费ods_base_db"
    val groupid: String = "base_db_app"
    val odsSource: DataStream[String] = env.addSource(KafkaUtils.getKafkaConsumer(sourceTopic, groupid))
    // 消费ods_base_db
    val dbDS: DataStream[JSONObject] = odsSource
      .map(JSON.parseObject(_))
      .filter(_.getString("type") != "delete")

    // 广播流
    val sourceFunction: DebeziumSourceFunction[String] = MySQLSource
      .builder[String]()
      .hostname("localhost")
      .port(3306)
      .username("root")
      .password("root")
      .databaseList("bigdata")
      .tableList("table_process")
      .deserializer(new CDCDeserialization)
      .startupOptions(StartupOptions.initial())
      .build()

    val ruleDS: DataStream[String] = env.addSource(sourceFunction)
    val braodcastState: MapStateDescriptor[String, TableProcess] = new MapStateDescriptor[String, TableProcess]("tableprocess-state", classOf[String], classOf[TableProcess])

    val broadcastDS: BroadcastStream[String] = ruleDS.broadcast(braodcastState)


    // 广播流 和 主流 连接
    val resultDS: BroadcastConnectedStream[JSONObject, String] = dbDS.connect(broadcastDS)


    // 分流 处理数据
    val hbaseTag: OutputTag[JSONObject] = new OutputTag[JSONObject]("hbase-tag")
    resultDS.process(new TableProcessFunction(hbaseTag, braodcastState))


    // 提取kafka流数据和hbase流数据


    // 提取kafka


    // 将kafka数据写入到kafka主题中


  }
}
