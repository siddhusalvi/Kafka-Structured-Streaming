package com.flutura.StructuredStreaming

import org.apache.log4j.LogManager
import org.json.JSONObject

import scala.collection.mutable.ListBuffer

object JsonHandler {

  val properties = PropertyManager.getProperties()
  val logger = LogManager.getLogger(properties.getProperty("logger"))

  val idKey = "id"
  val vKey = "v"
  val tKey = "t"

  logger.info("Initialized JsonHandler object")

  //Function to process given JSON String
  def toObjectList(givenJsonData: String): ListBuffer[SensorDataClass] = {
    try{
      val jsonObj = new JSONObject(givenJsonData)
      return parseJson(givenJsonData)
    }catch {
      case exception: Exception =>{
        logger.error(exception.printStackTrace())
        new ListBuffer[SensorDataClass]
      }
    }
  }

  //Function to parse json
  def parseJson(jsonStr: String): ListBuffer[SensorDataClass] = {
    var objList = new ListBuffer[SensorDataClass]
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
        objList.append(singleRecord)
      }
    } catch {
      case exception: Exception => println(exception.printStackTrace())
    }
    objList
  }

  //Function to convert JSON into object
  def convertToSensorObject(record: JSONObject): SensorDataClass = {

    var dataObject = new SensorDataClass()
    try {
      if (record.has(idKey)) {
        dataObject.id = record.getString(idKey)
      }

      if (record.has(vKey)) {
        dataObject.v = record.getDouble(vKey).toString
      }
      if (record.has(tKey)) {
        dataObject.t = record.getBigInteger(tKey).toString
      }
    } catch {
      case exception: Exception => logger.error(exception.printStackTrace())
    }
    dataObject
  }
}
