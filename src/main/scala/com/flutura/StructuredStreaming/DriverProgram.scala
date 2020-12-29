package com.flutura.StructuredStreaming

import org.apache.log4j.{LogManager, Logger}

object DriverProgram {
  def main(args: Array[String]): Unit = {
    var logger: Logger = null

    try {

      val propertiesPath = "src/main/resources/config.properties"
      //Getting properties
      val properties = PropertyMgr.getProperties(propertiesPath)
      //Setting up logger
      logger = LogManager.getLogger(properties.getProperty("logger"))

      val kafkaConsumer = new StructuredStearmingConsumer(properties)
      kafkaConsumer.start()

    } catch {
      case exception: Exception => logger.debug(exception.printStackTrace())
    } finally {

    }
  }
}
