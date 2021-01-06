package com.flutura.StructuredStreaming
import org.apache.log4j.{LogManager, Logger}

object DriverProgram {
  def main(args: Array[String]): Unit = {
    var logger: Logger = null
    var structuredStreamingConsumer:StructuredStearmingConsumer=null
    try {

      //Getting properties
      val properties = PropertyManager.getProperties()

      //Setting up logger
      logger = LogManager.getLogger(properties.getProperty("logger"))

      structuredStreamingConsumer = new StructuredStearmingConsumer()
      structuredStreamingConsumer.start()

    } catch {
      case exception: Exception => logger.debug(exception.printStackTrace())
    } finally {
      //Releasing resources
      logger.info("Running finally block")
      KafkaSink.close()
    }
  }
}
