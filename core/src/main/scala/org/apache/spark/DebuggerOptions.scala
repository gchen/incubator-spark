package org.apache.spark

object DebuggerOptions {
  def driverHost = System.getProperty("spark.driver.host")
  def driverPort = System.getProperty("spark.driver.port").toInt
  def driverAddress = (driverHost, driverPort)

  def eventLoggingEnabled = System.getProperty("spark.debugger.enabled", "false").toBoolean
  def logPath = System.getProperty("spark.debugger.logPath")
}
