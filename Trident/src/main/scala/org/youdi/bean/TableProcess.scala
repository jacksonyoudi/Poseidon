package org.youdi.bean

import scala.beans.BeanProperty

case class TableProcess(
                         @BeanProperty var sourceTable: String = "",
                         @BeanProperty var operateType: String = "",
                         @BeanProperty var sinkType: String = "",
                         @BeanProperty var sinkTable: String = "",
                         @BeanProperty var sinkColumns: String = "",
                         @BeanProperty var sinkPk: String = "",
                         @BeanProperty var sinkExtend: String = ""
                       )

object TableProcessConfig {
  val SINK_TYPE_HBASE: String = "hbase"
  val SINK_TYPE_KAFKA: String = "kafka"
  val SINK_TYPE_CK: String = "clickhouse"
}