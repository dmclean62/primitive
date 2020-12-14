/*
 * Copyright (C) 2020, Donald McLean. All rights reserved.
 *
 * This program and the accompanying materials are licensed under
 * the terms of the GNU General Public License version 3.0
 * as published by the Free Software Foundation.
 */
package org.primitive.services

import java.net.InetAddress

import org.primitive.ml.{Context, Module, OpResult}
import org.slf4j.{Logger, LoggerFactory}

import scala.xml.Node

/**
  * This is a module that either provides the real host name, or, in a test
  * environment, will simulate providing the host name and report the name
  * configured in the manifest.
  *
  * Note that this module is unable to use a context logging service since
  * it must be initialized BEFORE logging, since one of it's purposes is
  * for log module testing.
  */
class HostNameModule extends Module {

  var service: HostNameService = _
  val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  /**
    * Called by the ModuleLoader at program startup so that the module can initialize its functional components
    * and register its services.
    *
    * @param context - the specific EFLContext within which this module will be operating.
    * @param data    - XML giving module specific initialization and configuration details.
    */
  override def start(context: Context, data: Node): OpResult[Unit] = {
    super.setName(data)

    data.attribute("real") match {
      case Some(_) =>
        service = new HostNameService(InetAddress.getLocalHost.getHostName)
        logger.warn("[HostNameModule.start] using real host name: {}", service.hostName)
      case None => // ignore
    }

    // in simulate, get the name from
    data.attribute("simulate") match {
      case Some(nameAttribute) =>
        val propertyName = nameAttribute.toString()
        service = new HostNameService(System.getProperty(propertyName))
        logger.warn("[HostNameModule.start] using simulated host name: {}", service.hostName)
      case None => // ignore
    }

    if (service != null) {
      context.addService(name, service)
      OpResult.simpleSuccess()
    }
    else OpResult.failure("Missing a configuration attribute, 'real' or 'simulate'.")
  }

  /**
    * Called at context shutdown (may or may not be during program shutdown) so
    * that modules will be able to write out unsaved data, free up resources, etc.
    *
    * @param context - the EFLContext this module has been running in.
    */
  override def stop(context: Context): Unit = {
    context.removeService(service)
    service = null
  }

  /**
    * Called by a context that wants to import this module from a parent module.
    *
    * @param context - the context that wishes to use this module's services
    */
  override def addContext(context: Context): Unit = {
    context.addService(service)
  }

  /**
    * Called by an importing context at shutdown
    *
    * @param context - the context that no longer needs this module's services
    */
  override def removeContext(context: Context): Unit = {
    context.removeService(service)
  }
}

case class HostNameService(hostName: String)