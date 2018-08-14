package com.jspcore.learn.kafka

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties
import java.util.UUID

class KafkaClient(private val brokerHosts: String = "localhost:9092") {

  private val kafkaProducer: KafkaProducer<String, String>
  private val kafkaConsumer: KafkaConsumer<String, String>

  init {
    this.kafkaProducer = KafkaProducer(producerProperties())
    this.kafkaConsumer = KafkaConsumer(consumerProperties())
  }

  private fun consumerProperties(): Properties {
    val properties = Properties()
    properties[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = brokerHosts
    properties[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
    properties[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
    properties[ConsumerConfig.GROUP_ID_CONFIG] = UUID.randomUUID().toString()
    properties[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
    properties[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = false

    return properties
  }

  private fun producerProperties(): Properties {
    val properties = Properties()
    properties[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = brokerHosts
    properties[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
    properties[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java

    return properties
  }
}
