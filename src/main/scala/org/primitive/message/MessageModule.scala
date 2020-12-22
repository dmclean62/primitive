/*
 * Copyright (C) 2020, Donald McLean. All rights reserved.
 *
 * This program and the accompanying materials are licensed under
 * the terms of the GNU General Public License version 3.0
 * as published by the Free Software Foundation.
 */
package org.primitive.message

import java.io.{PrintWriter, StringWriter}

import net.liftweb.actor.LiftActor
import net.liftweb.util.{Helpers, Schedule}
import org.primitive.message.messages.MessageCondition
import org.primitive.ml._
import org.primitive.services._
import org.primitive.{NeedsLogging, ThreadTransactionMarker}
import org.slf4j.Logger

import scala.collection.mutable
import scala.xml.Node

/**
  * The message module creates and registers a MessageService that provides application message services.
  */
class MessageModule extends Module {

  private var logger: Logger = _
  private var messageDispatchService: MessagingService = _

  /**
    * Called by the ModuleLoader at program startup so that the module can initialize its functional components
    * and register its services.
    *
    * @param context - the specific EFLContext within which this module will be operating.
    * @param data - XML giving module specific initialization and configuration details.
    */
  def start(context: Context, data: Node): OpResult[Unit] = {
    context.findService[LoggerFactory] match {
      case None => return OpResult.failure("No logger factory service was found.")
      case Some(loggerFactory) => logger = loggerFactory.getLogger(getClass.getName)
    }

    logger.debug("[start] enter.")

    messageDispatchService = new MessagingService(context, logger)
    context.addService(messageDispatchService)

    OpResult.simpleSuccess()
  }

  /**
    * Called by the ModuleLoader when the context is unloaded (usually at or just before program termination)
    * so that modules will be able to write out unsaved data, free up resources, etc.
    *
    * @param context - the EFLContext this module has been running in.
    */
  def stop(context: Context): Unit = {
    logger.debug("[stop] enter.")
    waitForShutdown()

    messageDispatchService.shutDown()
    context.removeService(messageDispatchService)
    messageDispatchService = null
  }

  /**
    * Called by a context that wants to import this module from a parent module.
    *
    * @param context - the context that wishes to use this module's services
    */
  def addContext(context: Context): Unit = {
    context.addService(messageDispatchService)
  }

  /**
    * Called by an importing context at shutdown
    *
    * @param context - the context that no longer needs this module's services
    */
  def removeContext(context: Context): Unit = {
    context.removeService(messageDispatchService)
  }

  private def waitForShutdown(): Unit = {
    var iterationCount = 0

    while (messageDispatchService.registerCount > 0 && iterationCount < 25 ) {
      Thread.sleep(50)
      iterationCount += 1
    }

    logger.trace("[waitForShutdown] waited {} iterations for shutdown.", iterationCount)
  }
}

case object MESSAGING_SHUTDOWN
class MessageReuseException extends RuntimeException

abstract class CallableRegistration(val name: String) extends MessageRegistration {
  def call(message: Message): Unit
}

private class MessagingService(context: Context, logger: Logger) extends MessageDispatchService {

  val actor = new MessageActor(context, this)
  var registerCount = 0

  def shutDown(): Unit = {actor ! MESSAGING_SHUTDOWN}

  def post(message: Message): Unit = {
    logger.trace("[MessagingService.post] ({}) posting: {}", context.name, message.name, "")
    actor ! message
  }

  def delayedPost(message: Message, delay: Int): Unit = {
    Schedule.schedule(actor, message, Helpers.TimeSpan(delay))
  }

  override def register(name: String, callback: Message => Unit): MessageRegistration = {
    logger.trace("[MessagingService.register] ({}) registering for: {}", context.name, name, "")
    val registration = new SimpleRegistration(name, callback)
    val message = RegistrationMessage(registration)
    actor ! message
    registration
  }

  override def registerConditionally(name: String, callback: Message => Unit, condition: (String, Any)): MessageRegistration = {
    val result = new ValueConditionalRegistration(name, callback, condition)
    val message = RegistrationMessage(result)
    actor ! message
    result
  }

  override def registerConditionally(name: String, callback: Message => Unit, condition: MessageCondition): MessageRegistration = {
    val result = new ConditionalRegistration(name, callback, condition)
    val message = RegistrationMessage(result)
    actor ! message
    result
  }

  override def registerActor(name: String, actor: LiftActor): MessageRegistration = {
    val message = new ActorRegistration(name, actor)
    this.actor ! RegisterActorMessage(message)
    message
  }

  class SimpleRegistration(name: String, val callback: Message => Unit) extends CallableRegistration(name) {
    def call(message: Message): Unit = {
      if (isAcceptingMessages) callback(message)
    }

    def deregister(): Unit = {
      actor ! this
    }

    override def equals(obj: scala.Any): Boolean = callback.equals(obj)

    override def hashCode(): Int = callback.hashCode()
  }

  class ValueConditionalRegistration(name: String, val callback: Message => Unit, val condition: (String, Any)) extends CallableRegistration(name) {
    def call(message: Message): Unit = {
      if (! isAcceptingMessages) return

      message.getProperty(condition._1) match {
        case None => // condition fails, do nothing
        case Some(v) => if (v == condition._2) callback(message)
      }
    }

    def deregister(): Unit = {
      actor ! this
    }
  }

  class ConditionalRegistration(name: String, val callback: Message => Unit, val condition: MessageCondition) extends CallableRegistration(name) {
    def call(message: Message): Unit = {
      if (isAcceptingMessages && condition(message)) callback(message)
    }

    def deregister(): Unit = {
      actor ! this
    }
  }

  class ActorRegistration(name: String, val callback: LiftActor) extends CallableRegistration(name) {
    def call(message: Message): Unit = {
      if (isAcceptingMessages) callback ! message
    }

    def deregister(): Unit = {
      actor ! this
    }
  }

  class MessageActor(context: Context, parent: MessagingService) extends LiftActor with ThreadTransactionMarker with NeedsLogging {
    initializeLogger(context)

    val byName = new mutable.HashMap[String, mutable.HashSet[CallableRegistration]]
    val actorByName = new mutable.HashMap[String, mutable.HashSet[ActorRegistration]]

    private def shutDown(): Unit = {
      if (logger == null) return

      logger.debug("[shutDown] Registrations not unregistered: {}", Integer.valueOf(byName.size))
      byName.keys.foreach(k => logger.debug("[shutDown] {}", k))
      byName.clear()

      if (logger != null) {
        logger.debug("[shutDown] Actor registrations not unregistered: {}", actorByName.size)
      }
      actorByName.keys.foreach(k => logger.debug("[shutDown] {}", k))
      actorByName.clear()
    }

    protected def messageHandler: PartialFunction[Any, Unit] = {
      case RegisterActorMessage(r) => registerActor(r.name, r)
      case ar: ActorRegistration => deregisterActor(ar)
      case m: RegistrationMessage => register(m.r)
      case c: CallableRegistration => deregister(c)
      case message: Message => doDispatch(message)
      case MESSAGING_SHUTDOWN => shutDown()
      case a: Any =>
        val transKey = beginTrans(logger)
        logger.warn("[MessagingService$MessagingActor.messageHandler] got unknown message: {}", a)
        endTrans(logger, transKey)
    }

    def register(r: CallableRegistration): Unit = {
      val transKey = beginTrans(logger, "register." + context.name + "." + r.name)
      try {
        val forMessage = {
          byName.get(r.name) match {
            case None =>
              val result = new mutable.HashSet[CallableRegistration]
              byName.put(r.name, result)
              result
            case Some(s) => s
          }
        }
        logger.trace("[register] {}", byName)

        logger.trace("[register] adding: {}", r)
        forMessage.add(r)
        r.update(MRSActive)
      }
      catch {
        case t: Throwable => logger.error("[register]", t)
      }

      parent.registerCount = byName.size + actorByName.size
      endTrans(logger, transKey)
    }

    def deregister(c: CallableRegistration): Unit = {
      val transKey = beginTrans(logger, "deregister." + context.name + "." + c.name)
      try {
        byName.get(c.name) match {
          case None =>
            logger.warn("[deregister] not found.")
          case Some(hashSet) =>
            hashSet.remove(c)
            logger.trace("[deregister] remaining callbacks: {}", hashSet.size)
            if (hashSet.isEmpty) {
              logger.trace("[deregister] removing empty hash set.")
              byName.remove(c.name)
            }
        }

        c.update(MRSTerminal)
      }
      catch {
        case t: Throwable => logger.error("[MessagingService$MessagingActor.deregister]", t)
      }

      parent.registerCount = byName.size + actorByName.size
      endTrans(logger, transKey)
    }

    def registerActor(name: String, reg: ActorRegistration): Unit = {
      val transKey = beginTrans(logger, "MessagingService.registerActor." + context.name + "." + name)

      try {
        val forMessage = {
          actorByName.get(name) match {
            case None =>
              val result = new mutable.HashSet[ActorRegistration]
              actorByName.put(name, result)
              result
            case Some(m) => m
          }
        }

        forMessage.add(reg)
        reg.update(MRSActive)
      }
      catch {
        case t: Throwable => logger.error("[registerActor]", t)
      }

      parent.registerCount = byName.size + actorByName.size
      endTrans(logger, transKey)
    }

    def deregisterActor(ar: ActorRegistration): Unit = {
      val transKey = beginTrans(logger, "deregisterActor." + context.name + "." + ar.name)

      try {
        actorByName.get(ar.name) match {
          case None => logger.warn("[deregisterActor] did not find registration.")
          case Some(hashSet) =>
            hashSet.remove(ar)
            if (hashSet.isEmpty) actorByName.remove(ar.name)
        }

        ar.update(MRSTerminal)
      }
      catch {
        case t: Throwable => logger.error("[deregisterActor]", t)
      }

      parent.registerCount = byName.size + actorByName.size
      endTrans(logger, transKey)
    }

    def doDispatch(message: Message): Unit = {
      val transKey = beginTrans(logger, "messageHandler.dispatching." + message.name)
      try {
        message.context = context
        message.update(MSPending)
        logger.trace("[doDispatch] {}", byName)
        logger.trace("[doDispatch] {}", actorByName)

        byName.get(message.name) match {
          case None => logger.trace("[doDispatch] no regular callbacks.")
          case Some(set) =>
            logger.trace("[doDispatch] {} callbacks.", set.size)
            set.foreach(_.call(message))
        }

        actorByName.get(message.name) match {
          case None => logger.trace("[doDispatch] no actor callbacks.")
          case Some(set) =>
            logger.trace("[doDispatch] actors: {}", set.size)
            set.foreach(_.call(message))
        }
      }
      catch {
        case t: Throwable => logger.error("[doDispatch] ", t)
      }

      message.update(MSComplete)
      endTrans(logger, transKey)
    }
  }

  case class RegistrationMessage(r: CallableRegistration)
  case class RegisterActorMessage(r: ActorRegistration)

  /**
    * Gets the exception stack trace as a string.
    * @return String representation of exception.
    */
  def getStackTraceAsString(exception: Exception): String = {
    val sw = new StringWriter
    val pw: PrintWriter = new PrintWriter (sw)
    pw.print (" [ ")
    pw.print (exception.getClass.getName )
    pw.print (" ] ")
    pw.print (exception.getMessage )
    exception.printStackTrace (pw)
    sw.toString
  }
}
