/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package whisk.connector.lean

import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue

import scala.collection.concurrent.Map
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.FiniteDuration
import scala.util.Success
import scala.util.Try

import akka.actor.ActorSystem
import whisk.common.Logging
import whisk.core.WhiskConfig
import whisk.core.connector.MessageConsumer
import whisk.core.connector.MessageProducer
import whisk.core.connector.MessagingProvider

/**
 * A simple implementation of MessagingProvider
 */
object LeanMessagingProvider extends MessagingProvider {

  val queues: Map[String, BlockingQueue[Array[Byte]]] =
    new TrieMap[String, BlockingQueue[Array[Byte]]]

  def getConsumer(config: WhiskConfig, groupId: String, topic: String, maxPeek: Int, maxPollInterval: FiniteDuration)(
    implicit logging: Logging,
    actorSystem: ActorSystem): MessageConsumer = {

    var queue = queues.getOrElseUpdate(topic, new LinkedBlockingQueue[Array[Byte]](maxPeek))
    new LeanConsumer(queue, maxPeek)
  }

  def getProducer(config: WhiskConfig)(implicit logging: Logging, actorSystem: ActorSystem): MessageProducer =
    new LeanProducer(queues)

  def ensureTopic(config: WhiskConfig, topic: String, topicConfig: String)(implicit logging: Logging): Try[Unit] = {
    if (queues.contains(topic)) {
      Success(logging.info(this, s"topic $topic already existed"))
    } else {
      queues.put(topic, new LinkedBlockingQueue[Array[Byte]](Integer.MAX_VALUE))
      Success(logging.info(this, s"topic $topic created"))
    }
  }
}
