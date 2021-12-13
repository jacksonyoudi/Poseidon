package org.youdi.app.function

import com.alibaba.fastjson.JSONObject
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}
import org.youdi.common.TridentConfig
import org.youdi.utils.{DimUtil, ThreadPoolUtil}

import java.sql.{Connection, DriverManager}
import java.util.Collections
import java.util.concurrent.ThreadPoolExecutor

abstract class DimAsyncFucntion[T] extends RichAsyncFunction[T, T] with DimAsyncJoinFunction[T] {
  private var connection: Connection = _
  private var threadPoolExecutor: ThreadPoolExecutor = _
  private var tableName: String = _


  def this(tb: String) {
    this
    tableName = tb
  }


  override def asyncInvoke(input: T, resultFuture: ResultFuture[T]): Unit = {
    threadPoolExecutor.submit(
      new Runnable {
        override def run(): Unit = {
          // 查询主键

          val id: String = getKey(input)

          // 查询维度信息
          val js: JSONObject = DimUtil.getDimInfo(connection, tableName, id)

          // 补充维度信息
          join(input, js)
          // 将数据输出
          resultFuture.complete(List(input))

        }
      }
    )
  }

  override def open(parameters: Configuration): Unit = {
    Class.forName(TridentConfig.PHOENIX_DRIVER)
    connection = DriverManager.getConnection(TridentConfig.PHOENIX_SERVER)
    threadPoolExecutor = ThreadPoolUtil.getThreadPool
  }

  override def timeout(input: T, resultFuture: ResultFuture[T]): Unit = {
    println("TimeOut:" + input)
  }

}
