/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark

import java.io._

/**
 * A specialized `ObjectInputStream` to handle event logging.  In some cases, special actions need
 * to be taken when de-serializing objects from `EventLogInputStream`, e.g., recovering origin
 * source location information for `RDD`s.
 *
 * @constructor Creates a new `EventLogInputStream`
 * @param stream The underlying input stream
 * @param context The corresponding `SparkContext`
 * @see [[org.apache.spark.rdd.RDD]]
 * @see [[org.apache.spark.rdd.ParallelCollectionRDD]]
 */
private[spark] class EventLogInputStream(stream: InputStream, val context: SparkContext)
  extends ObjectInputStream(stream)

/**
 * A specialized `ObjectOutputStream` to handle event logging.  In some cases, special actions need
 * to be taken when serializing objects to `EventLogOutputStream`, e.g., recording origin source
 * location information for `RDD`s.
 *
 * @constructor Creates a new `EventLogOutputStream`
 * @param stream The underlying output stream
 * @see [[org.apache.spark.rdd.RDD]]
 * @see [[org.apache.spark.rdd.ParallelCollectionRDD]]
 */
private[spark] class EventLogOutputStream(stream: OutputStream) extends ObjectOutputStream(stream)
