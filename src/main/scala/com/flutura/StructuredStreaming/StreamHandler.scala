package com.flutura.StructuredStreaming

import org.apache.log4j.LogManager
import org.json.JSONObject
import redis.clients.jedis.Jedis


class SensorParamters{
  var id:String=null
  var v:String=null
  var t:String=null
}


object StreamHandler {

  val propertiesPath = "src/main/resources/config.properties"
  //Getting properties
  val properties = PropertyMgr.getProperties(propertiesPath)

  val jedis: Jedis = new Jedis(properties.getProperty("redisHost"),
    properties.getProperty("redisPort").toInt)
  jedis.auth(properties.getProperty("redisPassword"))
  jedis.select(properties.getProperty("redisDbIndex","0").toInt)

  //Setting up logger
  val logger = LogManager.getLogger(properties.getProperty("logClass"))
  logger.info("Stream handler class initialized")

  val idKey = "id"
  val vKey = "v"
  val tKey = "t"

  //Function to process stream
  def processStream(streamData: String): Unit = {
    logger.info("Processing Stream")
    try {
      new JSONObject(streamData)
      parseJson(streamData)
    } catch {
      case exception: Exception => logger.error("Invalid JSON file " + exception.printStackTrace())
    }
  }

  //Function to parse json
  def parseJson(jsonStr: String): Unit = {
    try {
      //Getting single json object
      val jsonObj = new JSONObject(jsonStr)

      //Getting object keys
      val keys = jsonObj.keys()

      //Lopping through keys
      while (keys.hasNext) {
        val key = keys.next()

        //Fetching single JSON object
        val singleRecord = convertToSensorObject(jsonObj.getJSONObject(key))
        writeToRedisDB(singleRecord)
      }

    } catch {
      case exception: Exception => println(exception.printStackTrace())
    }
  }

  //Function to convert JSON into object
  def convertToSensorObject(record: JSONObject): SensorParamters = {

    var dataObj = new SensorParamters()
    try {
      if (record.has(idKey)) {
        dataObj.id = record.getString(idKey)
      }

      if (record.has(vKey)) {
        dataObj.v = record.getDouble(vKey).toString
      }
      if (record.has(tKey)) {
        dataObj.t = record.getBigInteger(tKey).toString
      }
    } catch {
      case exception: Exception => print(exception.printStackTrace())
    }
    dataObj
  }

  //Function to write record into database
  def writeToRedisDB(singleRecord: SensorParamters): Unit = {
        println(singleRecord.id+" "+singleRecord.t+" "+singleRecord.v)
    try {
//      jedis.hsetnx(singleRecord.id, tKey, singleRecord.t)
//      jedis.hsetnx(singleRecord.id, vKey, singleRecord.v)
//      logger.debug("Record Written to the database")
    } catch {
      case exception: Exception => logger.error(exception.printStackTrace())
    }
  }

  //Function to close the database
  def close(): Unit = {
    if(jedis != null){
      jedis.close()
    }
  }

}
