package com.youdi.streamlet.datastream.async

import org.apache.flink.streaming.api.scala.async.{AsyncFunction, ResultFuture}

class HbaseAsyncFunc extends  AsyncFunction[String, String]{
  override def asyncInvoke(input: String, resultFuture: ResultFuture[String]): Unit = {
//    new HbaseCallback(resultFuture)
  }
}
