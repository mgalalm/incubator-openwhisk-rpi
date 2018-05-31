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

package whisk.core.loadBalancer

import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.LongAdder

import akka.actor.{ActorSystem, Props}
import akka.event.Logging.InfoLevel
import akka.stream.ActorMaterializer
import org.apache.kafka.clients.producer.RecordMetadata
import whisk.spi.SpiLoader
import whisk.core.entity._
import whisk.common.LoggingMarkers._
import whisk.common._
import whisk.core.WhiskConfig._
import whisk.core.connector._
import whisk.core.WhiskConfig

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}
import whisk.core.invoker.InvokerReactive
import whisk.utils.ExecutionContextFactory
/**
 * A loadbalancer that uses "horizontal" sharding to not collide with fellow loadbalancers.
 *
 * Horizontal sharding means, that each invoker's capacity is evenly divided between the loadbalancers. If an invoker
 * has at most 16 slots available, those will be divided to 8 slots for each loadbalancer (if there are 2).
 */
class LeanBalancer(config: WhiskConfig, controllerInstance: InstanceId)(
  implicit val actorSystem: ActorSystem,
  logging: Logging,
  materializer: ActorMaterializer)
    extends LoadBalancer {

  private implicit val executionContext: ExecutionContext = actorSystem.dispatcher

  /** State related to invocations and throttling */
  private val activations = TrieMap[ActivationId, ActivationEntry]()
  private val activationsPerNamespace = TrieMap[UUID, LongAdder]()
  private val totalActivations = new LongAdder()

  actorSystem.scheduler.schedule(0.seconds, 10.seconds) {
    MetricEmitter.emitHistogramMetric(LOADBALANCER_ACTIVATIONS_INFLIGHT(controllerInstance), totalActivations.longValue)
  }

  /** Loadbalancer interface methods */
  override def invokerHealth(): Future[IndexedSeq[InvokerHealth]] = Future.successful(IndexedSeq.empty[InvokerHealth])
  override def activeActivationsFor(namespace: UUID): Future[Int] =
    Future.successful(activationsPerNamespace.get(namespace).map(_.intValue()).getOrElse(0))
  override def totalActiveActivations: Future[Int] = Future.successful(totalActivations.intValue())
  override def clusterSize: Int = 1
  val invokerName = InstanceId(0)

  /** 1. Publish a message to the loadbalancer */
  override def publish(action: ExecutableWhiskActionMetaData, msg: ActivationMessage)(
    implicit transid: TransactionId): Future[Future[Either[ActivationId, WhiskActivation]]] = {
    logging.info(this, "in LeanBalancer.publish")
    val entry = setupActivation(msg, action, invokerName)
    sendActivationToInvoker(messageProducer, msg, invokerName).map { _ =>
      entry.promise.future
    }
  }

  /** 2. Update local state with the to be executed activation */
  private def setupActivation(msg: ActivationMessage,
                              action: ExecutableWhiskActionMetaData,
                              instance: InstanceId): ActivationEntry = {
    
    totalActivations.increment()
    activationsPerNamespace.getOrElseUpdate(msg.user.uuid, new LongAdder()).increment()
    
    logging.info(this, "in LeanBalancer.setupActivation after increments totalActivations: " + totalActivations + "activationsPerNamespace.getOrElseUpdate(msg.user.uuid, new LongAdder()): " + activationsPerNamespace.getOrElseUpdate(msg.user.uuid, new LongAdder()))

    val timeout = action.limits.timeout.duration.max(TimeLimit.STD_DURATION) + 1.minute
    // Install a timeout handler for the catastrophic case where an active ack is not received at all
    // (because say an invoker is down completely, or the connection to the message bus is disrupted) or when
    // the active ack is significantly delayed (possibly dues to long queues but the subject should not be penalized);
    // in this case, if the activation handler is still registered, remove it and update the books.
    activations.getOrElseUpdate(
      msg.activationId, {
        val timeoutHandler = actorSystem.scheduler.scheduleOnce(timeout) {
          processCompletion(Left(msg.activationId), msg.transid, forced = true, invoker = instance)
        }

        // please note: timeoutHandler.cancel must be called on all non-timeout paths, e.g. Success
        ActivationEntry(
          msg.activationId,
          msg.user.uuid,
          instance,
          timeoutHandler,
          Promise[Either[ActivationId, WhiskActivation]]())
      })
  }

  private val messagingProvider = SpiLoader.get[MessagingProvider]
  private val messageProducer = messagingProvider.getProducer(config)

  /** 3. Send the activation to the invoker */
  private def sendActivationToInvoker(producer: MessageProducer,
                                      msg: ActivationMessage,
                                      invoker: InstanceId): Future[RecordMetadata] = {
    implicit val transid: TransactionId = msg.transid
    logging.info(this, "in LeanBalancer.sendActivationToInvoker")

    val topic = s"invoker${invoker.toInt}"

    MetricEmitter.emitCounterMetric(LoggingMarkers.LOADBALANCER_ACTIVATION_START)
    val start = transid.started(
      this,
      LoggingMarkers.CONTROLLER_KAFKA,
      s"posting topic '$topic' with activation id '${msg.activationId}'",
      logLevel = InfoLevel)

    logging.info(this, "before producer.send")
    producer.send(topic, msg).andThen {
      case Success(status) =>
        logging.info(this, "producer.send finished with Success")
        transid.finished(
          this,
          start,
          s"posted to $topic",
          logLevel = InfoLevel)
      case Failure(_) => 
        logging.info(this, "producer.send finished with Failure")
        transid.failed(this, start, s"error on posting to topic $topic")
    }
  }

  /**
   * Subscribes to active acks (completion messages from the invokers), and
   * registers a handler for received active acks from invokers.
   */
  private val activeAckTopic = s"completed${controllerInstance.toInt}"
  private val maxActiveAcksPerPoll = 128
  private val activeAckPollDuration = 1.second
  private val activeAckConsumer =
    messagingProvider.getConsumer(config, activeAckTopic, activeAckTopic, maxPeek = maxActiveAcksPerPoll)

  private val activationFeed = actorSystem.actorOf(Props {
    new MessageFeed(
      "activeack",
      logging,
      activeAckConsumer,
      maxActiveAcksPerPoll,
      activeAckPollDuration,
      processActiveAck)
  })

  /** 4. Get the active-ack message and parse it */
  private def processActiveAck(bytes: Array[Byte]): Future[Unit] = Future {
    val raw = new String(bytes, StandardCharsets.UTF_8)
    CompletionMessage.parse(raw) match {
      case Success(m: CompletionMessage) =>
        processCompletion(m.response, m.transid, forced = false, invoker = m.invoker)
        activationFeed ! MessageFeed.Processed

      case Failure(t) =>
        activationFeed ! MessageFeed.Processed
        logging.error(this, s"failed processing message: $raw with $t")
    }
  }

  /** 5. Process the active-ack and update the state accordingly */
  private def processCompletion(response: Either[ActivationId, WhiskActivation],
                                tid: TransactionId,
                                forced: Boolean,
                                invoker: InstanceId): Unit = {
    val aid = response.fold(l => l, r => r.activationId)

    // treat left as success (as it is the result of a message exceeding the bus limit)
    val isSuccess = response.fold(_ => true, r => !r.response.isWhiskError)
    logging.info(this, "in processCompletion, aid: " + aid)
    logging.info(this, "totalActivations: " + totalActivations.toString() + " ctivationsPerNamespace.get(entry.namespaceId): " + activationsPerNamespace.toString())
    
    activations.remove(aid) match {
      case Some(entry) =>
        logging.info(this, "in processCompletion Some!!!")
        totalActivations.decrement()
        activationsPerNamespace.get(entry.namespaceId).foreach(_.decrement())
//        schedulingState.invokerSlots.lift(invoker.toInt).foreach(_.release())

        if (!forced) {
          entry.timeoutHandler.cancel()
          entry.promise.trySuccess(response)
        } else {
          entry.promise.tryFailure(new Throwable("no active ack received"))
        }

        logging.info(this, s"${if (!forced) "received" else "forced"} active ack for '$aid'")(tid)
        // Active acks that are received here are strictly from user actions - health actions are not part of
        // the load balancer's activation map. Inform the invoker pool supervisor of the user action completion.
//        invokerPool ! InvocationFinishedMessage(invoker, isSuccess)
      case None if !forced =>
        // the entry has already been removed but we receive an active ack for this activation Id.
        // This happens for health actions, because they don't have an entry in Loadbalancerdata or
        // for activations that already timed out.
//        invokerPool ! InvocationFinishedMessage(invoker, isSuccess)
        logging.info(this, "in processCompletion None1")
        logging.info(this, s"received active ack for '$aid' which has no entry")(tid)
      case None =>
        // the entry has already been removed by an active ack. This part of the code is reached by the timeout.
        // As the active ack is already processed we don't have to do anything here.
        logging.info(this, "in processCompletion None2")
        logging.info(this, s"forced active ack for '$aid' which has no entry")(tid)
    }
  }

  
  
  private def getInvoker(){
    implicit val ec = ExecutionContextFactory.makeCachedThreadPoolExecutionContext()
    val actorSystema: ActorSystem =
      ActorSystem(name = "invoker-actor-system", defaultExecutionContext = Some(ec))
    val invoker = new InvokerReactive(config, InstanceId(0), messageProducer)(actorSystema, implicitly)
  }
  
  getInvoker()
  
//  val invoker = new InvokerReactive(config, InstanceId(0), messageProducer)
}

object LeanBalancer extends LoadBalancerProvider {

  override def loadBalancer(whiskConfig: WhiskConfig, instance: InstanceId)(
    implicit actorSystem: ActorSystem,
    logging: Logging,
    materializer: ActorMaterializer): LoadBalancer = new LeanBalancer(whiskConfig, instance)

    def requiredProperties =
      Map(servicePort -> 8080.toString(), dockerRegistry -> null, dockerImagePrefix -> null) ++
      ExecManifest.requiredProperties ++
      wskApiHost ++ Map(dockerImageTag -> "latest")
}
