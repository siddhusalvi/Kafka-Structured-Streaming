package com.flutura.StructuredStreaming

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties

object Temp {

  def main(args: Array[String]): Unit = {


    val properties = new Properties()
    properties.put("bootstrap.servers", "localhost:9092")
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val topic = "dataframe"

    var producer: KafkaProducer[String, String] = new KafkaProducer[String, String](properties)



    //Sending record
    val fileContents = "a, b , c,d,"
    val record = new ProducerRecord[String, String](topic, fileContents)
    producer.send(record)


  }
}