package org.youdi.utils

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer

object KafkaUtils {
  def getKafkaProducer(topic: String): FlinkKafkaProducer[String] = {
    new FlinkKafkaProducer[String]("localhost:9092", topic, new SimpleStringSchema())
  }

}
