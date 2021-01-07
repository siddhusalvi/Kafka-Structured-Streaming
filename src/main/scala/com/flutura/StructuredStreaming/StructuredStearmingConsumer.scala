package com.flutura.StructuredStreaming

import org.apache.log4j.LogManager
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SparkSession}

class StructuredStearmingConsumer() {

  //Getting properties
  val properties = PropertyManager.getProperties()

  //Creating sparksession
  val spark = SparkSession.builder()
    .master(properties.getProperty("spark-master"))
    .appName(properties.getProperty("app-name"))
    .getOrCreate()

  val logger = LogManager.getLogger(properties.getProperty("logger"))
  logger.info("created SparkSession")

  val kafkaSource = properties.getProperty("kafka-source")
  val kafkaTopic = properties.getProperty("kafka-topic")
  val kafkaServers = properties.getProperty("kafka-host") + ":" + properties.getProperty("kafka-port")
  val kafkaOffset = properties.getProperty("kafka-offset")
  var dataframe: DataFrame = null

  logger.info("Done setup properties for kafka consumer")

  //Starting consumer
  def start(): Unit = {

    logger.info("Starting Consumer")

    try {
      //getting streaming dataframe
      dataframe = getStreamDF()
      //Complete mode : produce resulted table
      dataframe.writeStream.outputMode(OutputMode.Update()).foreach(KafkaSink.send).start()

    } catch {
      case exception: Exception => logger.error(exception.printStackTrace())
    }

    //waiting for termination
    spark.streams.awaitAnyTermination()

  }

  //Function to get kafka Stream
  def getStreamDF(): DataFrame = {
    val streamDataFrame = spark.readStream
      .format(kafkaSource)
      .option("kafka.bootstrap.servers", kafkaServers)
      .option("subscribe", kafkaTopic)
      .option("startingOffsets", kafkaOffset)
      .load()
      //casting kafka value to the string
      .selectExpr("CAST(value AS STRING)")
    streamDataFrame
  }

}
