/*
 * Copyright (C) 2020, Donald McLean. All rights reserved.
 *
 * This program and the accompanying materials are licensed under
 * the terms of the GNU General Public License version 3.0
 * as published by the Free Software Foundation.
 */
package org.primitive

import org.primitive.db.{NeoConnectorSuite, PropertyTypeTest}
import org.primitive.logging.LoggerFactorySuite
import org.primitive.ml.{ModuleLoaderSuite, ModuleLoaderTest}
import org.scalatest.Sequential

class MasterSuite extends Sequential(
  new ModuleLoaderSuite,
  new LoggerFactorySuite,
  new PropertyTypeTest,
  new NeoConnectorSuite
)