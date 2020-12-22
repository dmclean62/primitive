/*
 * Copyright (C) 2020, Donald McLean. All rights reserved.
 *
 * This program and the accompanying materials are licensed under
 * the terms of the GNU General Public License version 3.0
 * as published by the Free Software Foundation.
 */
package org.primitive

import org.primitive.ml.{ModuleLoader, Context}
import org.primitive.services.LoggerFactory
import org.slf4j.Logger

/**
  * Utility trait to add logging to a class as a mixin.
  */
trait NeedsLogging {
  var logger: Logger = _

  def initializeLogger(): Unit = {
    initializeLogger(ModuleLoader.getDefaultContext)
  }

  def initializeLogger(context: Context): Unit = {
    if (logger == null) {
      context.findService[LoggerFactory] match {
        case None => println("Did not find a logger factory.")
        case Some(f) => logger = f.getLogger(getClass.getName)
      }
    }
  }
}
