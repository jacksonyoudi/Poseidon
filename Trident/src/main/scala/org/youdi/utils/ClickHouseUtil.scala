package org.youdi.utils

import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExecutionOptions, JdbcSink, JdbcStatementBuilder}
import org.youdi.common.TridentConfig.{CLICKHOUSE_DRIVER, CLICKHOUSE_URL}
import org.youdi.bean.TransientSink

import java.lang.reflect.Field
import java.sql.PreparedStatement

object ClickHouseUtil {
  def getSink[T](sql: String): SinkFunction[T] = {

    JdbcSink.sink[T](
      sql, new JdbcStatementBuilder[T] {
        override def accept(t: PreparedStatement, u: T): Unit = {
          val fields: Array[Field] = u.getClass.getDeclaredFields

          // 遍历字段
          var offset: Int = 0
          for (field <- fields) {
            field.setAccessible(true)
            val annotation: TransientSink = field.getAnnotation(classOf[TransientSink])

            if (annotation == null) {
              offset += 1
              val value: AnyRef = field.get(u)
              t.setObject(offset, value)
            }
          }


        }
      },

      new JdbcExecutionOptions.Builder()
        .withBatchSize(5)
        .build(),
      new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .withDriverName(CLICKHOUSE_DRIVER)
        .withUrl(CLICKHOUSE_URL)
        .build()
    )
  }


}
