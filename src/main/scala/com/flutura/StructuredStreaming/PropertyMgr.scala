package com.flutura.StructuredStreaming
import java.io.FileInputStream
import java.util.Properties

//Class to import kafka properties
object PropertyMgr {
  //Function to get properties
  def getProperties(filePath: String): Properties = {
    val properties = new Properties()
    try {
      val fileInputStream = new FileInputStream(filePath)
      properties.load(fileInputStream)
      fileInputStream.close()
    } catch {
      case exception: Exception => println(exception.printStackTrace())
    }
    return properties
  }

}
