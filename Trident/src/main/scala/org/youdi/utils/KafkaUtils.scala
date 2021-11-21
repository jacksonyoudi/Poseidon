package org.youdi.utils

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord

import java.lang
import java.util.Properties

object KafkaUtils {
  private val brokerList: String = "localhost:9092"
  private val default_topic: String = "DWD_DEFAULT_TOPIC"


  def getKafkaProducer(topic: String): FlinkKafkaProducer[String] = {
    new FlinkKafkaProducer[String](brokerList, topic, new SimpleStringSchema())
  }

  def getKafkaProducer[T](kafkaSerializationSchema: KafkaSerializationSchema[T]): FlinkKafkaProducer[T] = {
    val properties: Properties = new Properties
    //    properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupid)
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)

    new FlinkKafkaProducer[T](default_topic,
      kafkaSerializationSchema,
      properties,
      FlinkKafkaProducer.Semantic.NONE
    )
  }


  def getKafkaConsumer(topic: String, groupid: String): FlinkKafkaConsumer[String] = {
    val properties: Properties = new Properties
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupid)
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)

    new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), properties)
  }
}
