package org.apache.spark

import java.io._

case class EventLogInputStream(stream: InputStream, context: SparkContext)
  extends ObjectInputStream(stream) {

  override def resolveClass(desc: ObjectStreamClass) =
    Class.forName(desc.getName, false, Thread.currentThread().getContextClassLoader)
}

case class EventLogOutputStream(stream: OutputStream)
  extends ObjectOutputStream(stream)
