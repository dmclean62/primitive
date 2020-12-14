/*
 * Copyright (C) 2020, Donald McLean. All rights reserved.
 *
 * This program and the accompanying materials are licensed under
 * the terms of the GNU General Public License version 3.0
 * as published by the Free Software Foundation.
 */
package org.primitive.logging

import java.io.ByteArrayInputStream
import java.net.InetAddress

import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.joran.JoranConfigurator
import ch.qos.logback.core.joran.spi.JoranException
import ch.qos.logback.core.util.StatusPrinter
import org.primitive.ml.{Context, Module, OpResult}
import org.primitive.services.HostNameService
import org.slf4j.Logger

import scala.util.control.Breaks.{break, breakable}
import scala.xml.Node

/**
  *  This module creates a LoggerFactory that creates Logger objects with the SLF4J
  *  definition using the Logback implementation.
  *
  *  The "start" method expects to receive XML containing a "configuration" element as
  *  per the Logback documentation.
  *
  *  To use this style of logging, you will need to:
  *
  *  1. Ensure that the SLF4J API, Logback Core and Logback Classic JAR files are included in
  *     the classpath.
  *
  *  2. Add a module entry for this class in the ModuleLoader configuration file (probably in
  *     the default EFLContext). Inside this entry, include the Logback "configuration" entry.
  *
  *  3. The class needing logging services should get the EFLContext object in which this
  *     module was loaded and then use the findService (or findRequiredService) method,
  *     passing the LoggerFactory class.
  *
  *  4. The class needing logging services can then get a Logger (either the default or by name)
  *     from the LoggerFactory object.
  *
  *  The module block in the manifest XML may contain multiple configuration blocks with different
  *  settings. The module will check each block to see if it specifies a host name, and if it matches
  *  that block will be used. Otherwise, it will check to if it specifies a log type, and if that
  *  matches the system property "logType" then that block will be used. If neither is specified, then
  *  that block will be used. If multiple blocks are given, the "fall through" case should be last.
  * */
class LogbackLoggingModule extends Module {

  private var service: LogbackService = _
  private var hostName: String = _
  private val logType: String = System.getProperty("logType")

  private var loggerContext: LoggerContext = _

  /**
    * Called by the ModuleLoader at program startup so that the module can initialize its functional components
    * and register its services.
    *
    * @param context - the specific EFLContext within which this module will be operating.
    * @param data - XML giving module specific initialization and configuration details.
    */
  def start(context: Context, data: Node): OpResult[Unit] = {
    hostName = context.findService[HostNameService] match {
      case Some(s) => s.hostName
      case None => InetAddress.getLocalHost.getHostName // if there is no service use real host name
    }

    // assume SLF4J is bound to logback in the current environment
    configureLogback(data)

    service = new LogbackService(loggerContext)
    context.addService(service)

    OpResult.simpleSuccess()
  }

  /**
    * Called at context shutdown (may or may not be during program shutdown) so
    * that modules will be able to write out unsaved data, free up resources, etc.
    *
    * @param context - the EFLContext this module has been running in.
    */
  def stop(context: Context): Unit = {
    context.removeService(service)
    service.stop()
    service = null
  }

  /**
    * Called by a context that wants to import this module from a parent module.
    *
    * @param context - the context that wishes to use this module's services
    */
  def addContext(context: Context): Unit = {
    context.addService(service)
  }

  /**
    * Called by an importing context when that context is unloaded
    *
    * @param context - the context that no longer needs this module's services
    */
  def removeContext(context: Context): Unit = {
    context.removeService(service)
  }

  private def configurationAsStream(data: Node): ByteArrayInputStream = {
    for (configuration <- data \ "configuration") {
      breakable {
        configuration.attribute("host") match {
          case Some(h) =>
            println("[LogbackLoggingModule.configurationAsStream] using host specific config.")
            val targetHost = h.toString()
            if (targetHost == hostName) {
              println("[LogbackLoggingModule.configurationAsStream] using host specific config.")
              return new ByteArrayInputStream(configuration.toString().getBytes("UTF-8"))
            } else break // host name doesn't match
          case None => // do nothing and fall through
        }

        configuration.attribute("logType") match {
          case Some(t) =>
            val typeValue = t.toString()
            if (typeValue == logType) {
              println("[LogbackLoggingModule.configurationAsStream] using type specific config: " + logType)
              return new ByteArrayInputStream(configuration.toString().getBytes("UTF-8"))
            } else break // log type doesn't match
          case None => // do nothing and fall through
        }

        println("[LogbackLoggingModule.configurationAsStream] using untyped config\n" + configuration)
        return new ByteArrayInputStream(configuration.toString().getBytes("UTF-8"))
      }
    }

    println("[LogbackLoggingModule.configurationAsStream] fell through to empty config.")
    new ByteArrayInputStream(new Array[Byte](0))
  }

  private def configureLogback(data: Node): Unit = {
    loggerContext = new LoggerContext()

    try {
      val configurator = new JoranConfigurator()
      configurator.setContext(loggerContext)
      // the context was probably already configured by default configuration rules
      loggerContext.reset()

      val stream = configurationAsStream(data)
      configurator.doConfigure(stream)

      val logger = loggerContext.getLogger(getClass.getName)
      logger.debug("[LogbackLoggingModule.configureLogback] Start logging.")

      val el = configurator.getContext.getStatusManager.getCopyOfStatusList
      if (el == null) {
        logger.info("[LogbackLoggingModule.configureLogback] no errors in configuration.")
      }
      else {
        val elI = el.iterator()
        logger.info("[LogbackLoggingModule.configureLogback] Got {} messages.", el.size())
        while (elI.hasNext) {
          val e = elI.next()
          logger.info("[LogbackLoggingModule.configureLogback] {}", e)
        }
      }

    }
    catch {
      case e: JoranException =>
        println("Got exception: ")
        e.printStackTrace()
    }
    StatusPrinter.printInCaseOfErrorsOrWarnings(loggerContext)
  }
}

/**
  * The service object that uses the SLF4J/Logback mechanisms, as initialized in
  * the module start method to create or access Logger instances as requested.
  */
class LogbackService(loggerContext: LoggerContext) extends org.primitive.services.LoggerFactory {

  def stop(): Unit = {
    loggerContext.stop()
  }

  def getLogger(name: String): Logger = {
    loggerContext.getLogger(name)
  }

  def getDefaultLogger: Logger = {
    loggerContext.getLogger("")
  }
}
