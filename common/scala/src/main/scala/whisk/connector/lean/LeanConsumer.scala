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

import scala.concurrent.duration._
import whisk.common.Logging
import whisk.core.connector.MessageConsumer
import java.util.concurrent.BlockingQueue
import java.util.concurrent.TimeUnit

class LeanConsumer(queue: BlockingQueue[Array[Byte]], override val maxPeek: Int)(implicit logging: Logging)
    extends MessageConsumer {

    /**
     */
    override def peek(duration: FiniteDuration, retry: Int): Iterable[(String, Int, Long, Array[Byte])] = {
      val record = queue.poll(duration.toMillis, TimeUnit.MILLISECONDS)

      //TODO: currently returns record in the format below, should be refactored of kafka later
      if(record != null){
        Iterable(("", 0, 0, record))
      }else{
        Iterable()
      }
    }

    /**
     */
    override def commit(retry: Int): Unit = { /*do nothing*/ }

    override def close(): Unit = {
        logging.info(this, s"closing myconsumer")
    }
}
