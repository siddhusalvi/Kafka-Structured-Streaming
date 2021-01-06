package com.flutura.StructuredStreaming

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.LogManager
import org.apache.spark.sql.{ForeachWriter, Row}
import java.util.Properties
import scala.collection.mutable.ListBuffer

object KafkaSink {

  val properties = PropertyManager.getProperties()

  //Setting up logger
  val logger = LogManager.getLogger(properties.getProperty("logger"))

  val kafkaProperties = new Properties()
  kafkaProperties.put("bootstrap.servers", properties.getProperty("kafka-host")+":"+properties.getProperty("kafka-port"))
  kafkaProperties.put("key.serializer", properties.getProperty("kafka-key-serializer"))
  kafkaProperties.put("value.serializer", properties.getProperty("kafka-value-serializer"))
  val topic = properties.getProperty("kafka-producer-topic")
  var producer: KafkaProducer[String, String] = new KafkaProducer(kafkaProperties)

  logger.info("KafkaSink object initialized")

  def send: ForeachWriter[Row] = {
    logger.info("Starting kafkaSink.send() ")
    new ForeachWriter[Row] {
      override def open(partitionId: Long, version: Long): Boolean = {
        true
      }

      //Overriding process to send data
      override def process(row: Row): Unit = {

        val indexOfValue = properties.getProperty("index-of-value").toInt
        //Parsing value from row
        val data = row.getString(indexOfValue)

        //Parsing json to list of objects
        val listBuffer: ListBuffer[SensorDataClass] = JsonHandler.toObjectList(data)

        //Writing objects to the kafka
        if (listBuffer != null) {
          listBuffer.foreach(obj => {
            if(obj != null) {
              producer.send(new ProducerRecord(topic, (obj.id + " " + obj.t + " " + obj.v)))
            }
          })
        }
      }
      override def close(errorOrNull: Throwable): Unit = {
          if(errorOrNull != null){

          logger.error(errorOrNull.printStackTrace())
        }
      }
    }
  }

  //Releasing the resources
  def close():Unit={
    if(producer!= null){
      logger.info("Closing kafka Producer")
      producer.close()
    }
  }

}