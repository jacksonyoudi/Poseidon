package org.youdi.utils

import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExecutionOptions, JdbcSink, JdbcStatementBuilder}
import org.youdi.common.TridentConfig.{CLICKHOUSE_DRIVER, CLICKHOUSE_URL}

import java.lang.reflect.Field
import java.sql.PreparedStatement

object ClickHouseUtil {
  def getSink[T](sql: String): SinkFunction[T] = {

    JdbcSink.sink[T](
      sql, new JdbcStatementBuilder[T] {
        override def accept(t: PreparedStatement, u: T): Unit = {
          val fields: Array[Field] = u.getClass.getDeclaredFields

          // 遍历字段
          for (i <- 0 until fields.length) {
            val value: AnyRef = fields(i).get(u)

            t.setObject(i + 1, value)
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
