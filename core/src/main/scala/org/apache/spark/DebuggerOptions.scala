package org.apache.spark

object DebuggerOptions {
  def driverHost = System.getProperty("spark.driver.host")
  def driverPort = System.getProperty("spark.driver.port").toInt
  def driverAddress = (driverHost, driverPort)

  def enabled = System.getProperty("spark.debugger.enabled", "true").toBoolean
  def checksum = System.getProperty("spark.debugger.checksum", "true").toBoolean
  def logPath = System.getProperty("spark.debugger.logPath")
}
