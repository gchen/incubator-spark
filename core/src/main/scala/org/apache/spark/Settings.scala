package org.apache.spark

import com.typesafe.config.ConfigFactory

object Settings {
  private[this] val config = ConfigFactory.load()

  val masterHost = config.getString("spark.master.host")
  val masterPort = config.getInt("spark.master.port")
  val masterAddress = (masterHost, masterPort)
  val masterWebUIPort = config.getInt("spark.master.webui.port")

  val enabled = config.getBoolean("spark.debugger.enabled")
  val checksum = config.getBoolean("spark.debugger.checksum")
  val logPath = config.getString("spark.debugger.logPath")
}
