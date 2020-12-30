package com.flutura.StructuredStreaming
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.{ForeachWriter, Row}

object KafkaSink
{


  val properties = new Properties()
  properties.put("bootstrap.servers", "localhost:9092")
  properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val topic="dataframe"
  val servers="localhost:9092"
  val results = new scala.collection.mutable.HashMap[String, String]
  var producer: KafkaProducer[String, String] = _


  def send :ForeachWriter[Row] = {

    new ForeachWriter[Row] {
      override def open(partitionId: Long,version: Long): Boolean = {
        producer = new KafkaProducer(properties)
        true
      }

      override def process(value: Row): Unit = {
        producer.send(new ProducerRecord(topic, value.toString()))
      }

      def close(errorOrNull: Throwable): Unit = {
        producer.close()
      }
    }
  }

}