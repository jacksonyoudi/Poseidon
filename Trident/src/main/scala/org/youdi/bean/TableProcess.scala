package org.youdi.bean

case class TableProcess(
                         sourceTable: String,
                         operationType: String,
                         sinkType: String,
                         sinkTable: String,
                         sinkColumns: String,
                         sinkPk: String,
                         sinkExtend: String
                       )


object TableProcess {
  val SINK_TYPE_HBASE: String = "hbase"
  val SINK_TYPE_KaFKA: String = "kafka"
  val SINK_TYPE_CK: String = "clickhouse"
}