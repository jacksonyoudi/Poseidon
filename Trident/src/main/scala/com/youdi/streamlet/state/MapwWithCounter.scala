package com.youdi.streamlet.state

import org.apache.flink.api.common.functions.RichMapFunction

class MapwWithCounter extends RichMapFunction[String, String] {
  

  override def map(value: String): String = {

  }
}
