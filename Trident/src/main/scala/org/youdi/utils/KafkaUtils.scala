package org.youdi.utils

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.kafka.clients.consumer.ConsumerConfig

import java.util.Properties

object KafkaUtils {
  private val brokerList = "localhost:9092"


  def getKafkaProducer(topic: String): FlinkKafkaProducer[String] = {
    new FlinkKafkaProducer[String](brokerList, topic, new SimpleStringSchema())
  }


  def getKafkaConsumer(topic: String, groupid: String): FlinkKafkaConsumer[String] = {
    val properties: Properties = new Properties
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupid)
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)

    new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), properties)
  }

}
