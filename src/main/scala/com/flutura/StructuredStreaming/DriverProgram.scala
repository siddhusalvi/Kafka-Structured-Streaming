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
//      logger = LogManager.getLogger(properties.getProperty("logger"))

      val kafkaConsumer = new StructuredStearmingConsumer(properties)
      kafkaConsumer.start()

    } catch {
      case exception: Exception => logger.debug(exception.printStackTrace())
    } finally {

    }
  }
}

/*

 {"1": {"id": "TAG123345","v": 8.35599995321,"t": 1604473997894},"2":{"id": "TAG2345","v": 50358,"t": 1604474000159},"3":{"id": "TAG3","v": 1,"t": 1604468962890},"4":{"id": "TAG4958","v": 8.3519001,"t": 1604474000159},"5":{"id": "TAG567","v": 23,"t": 1604386843456}}

* */