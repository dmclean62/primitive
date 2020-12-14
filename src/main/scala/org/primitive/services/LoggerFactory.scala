/*
 * Copyright (C) 2020, Donald McLean. All rights reserved.
 *
 * This program and the accompanying materials are licensed under
 * the terms of the GNU General Public License version 3.0
 * as published by the Free Software Foundation.
 */
package org.primitive.services

import org.slf4j.Logger

/**
  * A class that is and EFL service and creates Logger objects.
  */
trait LoggerFactory extends java.lang.Object {
  /**
    * Gets the root logger.
    */
  def getDefaultLogger : Logger

  /**
    * Gets the named Logger.
    *
    * @param name - name of the logger to be created or fetched.
    */
  def getLogger(name : String) : Logger
}
