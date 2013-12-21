package org.apache.spark

import java.io._

class EventLogInputStream(stream: InputStream, val context: SparkContext)
  extends ObjectInputStream {

  override def resolveClass(desc: ObjectStreamClass) =
    Class.forName(desc.getName, false, Thread.currentThread().getContextClassLoader)
}

class EventLogOutputStream(stream: OutputStream)
  extends ObjectOutputStream