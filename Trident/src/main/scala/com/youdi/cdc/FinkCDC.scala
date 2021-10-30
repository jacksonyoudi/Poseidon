package com.youdi.cdc

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}


object FinkCDC {
  def main(args: Array[String]): Unit = {
    //  evn
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 开启checkpointing

    env.setStateBackend(new RocksDBStateBackend("file:///opt/module/applog/gmall2020/backend/"))

    env.enableCheckpointing(5000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(10000L)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(3000)

    // 重启策略


    // 通过cdc
    val sourceFunction: DebeziumSourceFunction[String] = MySQLSource
      .builder[String]()
      .hostname("localhost")
      .port(3306)
      .username("root")
      .password("root")
      .databaseList("poseidon")
      //  .tableList()
      .deserializer(new CDCDeserialization)
      .startupOptions(StartupOptions.initial())
      .build()

    val ds: DataStream[String] = env.addSource(sourceFunction)
    ds.print()


    env.execute("flink-cdc")
  }
}
