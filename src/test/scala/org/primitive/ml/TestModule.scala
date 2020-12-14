/*
 * Copyright (C) 2020, Donald McLean. All rights reserved.
 *
 * This program and the accompanying materials are licensed under
 * the terms of the GNU General Public License version 3.0
 * as published by the Free Software Foundation.
 */
package org.primitive.ml

import scala.xml.Node

/**
  * A simple test module for validating basic loading in the ModuleLoader.
  */
class TestModule extends Module {
  def start(context: Context, data: Node): OpResult[Unit] = {
    println("[TestModule.start] enter.")

    OpResult.simpleSuccess()
  }

  def stop(context: Context): Unit = {
    println("[TestModule.stop] enter.")
  }

  /**
    * Called by a context that wants to import this module from a parent module.
    *
    * @param context - the context that wishes to use this module's services
    */
  override def addContext(context: Context): Unit = {}

  /**
    * Called by an importing context at shutdown
    *
    * @param context - the context that no longer needs this module's services
    */
  override def removeContext(context: Context): Unit = {}
}