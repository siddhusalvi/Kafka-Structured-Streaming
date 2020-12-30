package com.flutura.StructuredStreaming

import org.apache.log4j.LogManager
import org.json.JSONObject
import redis.clients.jedis.Jedis

import scala.collection.mutable.{ArrayBuffer, ListBuffer}


class SensorParamters{
  var id:String=null
  var v:String=null
  var t:String=null
}


object StreamHandler {


  val idKey = "id"
  val vKey = "v"
  val tKey = "t"



  //Function to process stream
  def processStream(streamData: String): ListBuffer[SensorParamters] = {
      if(isJSONValid(streamData)){
        return parseJson(streamData)
      }
    return new ListBuffer[SensorParamters]
  }

  //check json is valid or not
  def isJSONValid(jsonStr: String): Boolean = {
    try {
      new JSONObject(jsonStr)
      true
    } catch {
      case _: Exception => false
    }
  }

  //Function to parse json
  def parseJson(jsonStr: String): ListBuffer[SensorParamters] = {
    var arrbff = new ListBuffer[SensorParamters]
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
        arrbff.append(singleRecord)
      }
    } catch {
      case exception: Exception => println(exception.printStackTrace())
    }
    println(arrbff)
    arrbff
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
      case exception: Exception => println(exception.printStackTrace())
    }
  }

  //Function to close the database
  def close(): Unit = {
//    if(jedis != null){
//      jedis.close()
//    }
  }

}
