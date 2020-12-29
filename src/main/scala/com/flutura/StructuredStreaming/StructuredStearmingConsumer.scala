package com.flutura.StructuredStreaming

import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties
class StructuredStearmingConsumer (properties: Properties){




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

  //Starting consumer
  def start(): Unit = {
    val dataFrame = getStreamDF()

    printStreamDF(dataFrame)

    spark.streams.awaitAnyTermination()

  }

    //Function to get kafka Stream
  def getStreamDF(): DataFrame = {
    val streamDF = spark.readStream
      .format(kafkaSource)
      .option("kafka.bootstrap.servers",kafkaServers)
      .option("subscribe", kafkaTopic)
      .option("startingOffsets", kafkaOffset)
      .load()
      .selectExpr("CAST(value AS STRING)")
    streamDF
  }

  //Function to printStream
  def printStreamDF(dataFrame: DataFrame):Unit= {
    dataFrame.writeStream
      .outputMode("append")
      .format("console")
      .start()
  }



  }
