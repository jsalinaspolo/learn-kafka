package com.jspcore.learnkafka

import com.jspcore.learn.kafka.KafkaClient
import org.apache.kafka.clients.producer.ProducerRecord
import spock.lang.Shared
import spock.lang.Specification

class KafkaShould extends Specification {

  @Shared kafkaClient = new KafkaClient("localhost:9092")

  def "consumer read messages produced"() {
    given:
    kafkaClient.kafkaProducer.send(new ProducerRecord<String, String>("topic", "key", "value"))
    kafkaClient.kafkaConsumer.subscribe(["topic"])
//    kafkaClient.kafkaConsumer.poll(50)

    when:
    def messages = kafkaClient.kafkaConsumer.poll(50)

    then:

    messages.forEach { record -> println(record) }
    true
  }
}
