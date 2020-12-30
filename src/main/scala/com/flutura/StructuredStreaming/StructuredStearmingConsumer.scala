package com.flutura.StructuredStreaming

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

class StructuredStearmingConsumer(properties: Properties) {

  //Creating sparksession
  val spark = SparkSession.builder()
    .master(properties.getProperty("spark-master"))
    .appName(properties.getProperty("app-name"))
    .getOrCreate()

  //  val logger = LogManager.getLogger(properties.getProperty("logger"))
  //  logger.info("created SparkSession")

  val kafkaSource = properties.getProperty("kafka-source")
  val kafkaTopic = properties.getProperty("kafka-topic")
  val kafkaServers = properties.getProperty("kafka-host") + ":" + properties.getProperty("kafka-port")
  val kafkaOffset = properties.getProperty("kafka-offset")

  val kafkaProperties = new Properties()
  kafkaProperties.put("bootstrap.servers", "localhost:9092")
  kafkaProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  kafkaProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  var producer: KafkaProducer[String, String] = null
  val topic = "dataframe"

  //Starting consumer
  def start(): Unit = {
    val dataframe = getStreamDF()

    try {
    dataframe.writeStream.outputMode(OutputMode.Update()).foreach(KafkaSink.send).start()
    } catch {
      case exception: Exception => println("Exception occured")
    }

    spark.streams.awaitAnyTermination()
  }

  //Function to get kafka Stream
  def getStreamDF(): DataFrame = {
    val streamDF = spark.readStream
      .format(kafkaSource)
      .option("kafka.bootstrap.servers", kafkaServers)
      .option("subscribe", kafkaTopic)
      .option("startingOffsets", kafkaOffset)
      .load()
      .selectExpr("CAST(value AS STRING)")
    streamDF
  }
}
