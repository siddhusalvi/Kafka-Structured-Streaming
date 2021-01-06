package com.flutura.StructuredStreaming
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}

case class KafkaMessage(partition: Int, offset: Long, value: String)
object Temp {
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
  //  var dataframe:DataFrame=null

  logger.info("Setup properties for kafka consumer")


  logger.info("Starting Consumer")

  def main(args: Array[String]): Unit = {
    try {
      //getting streaming dataframe
      val dataframe = getStreamDF()
      val newDf = dataframe
        .selectExpr("partition", "offset", "CAST(value AS STRING)")

      printStreamDF(newDf)

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
      .option("startingOffsets", 160)
      .load()
    //casting kafka value to the
    streamDataFrame
  }

  //Function to print stream
  def printStreamDF(dataFrame: DataFrame): Unit = {
    dataFrame.writeStream
      .outputMode("append")
      .format("console")
      .start()
  }
}

