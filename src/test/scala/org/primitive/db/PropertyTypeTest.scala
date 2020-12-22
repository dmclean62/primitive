/*
 * Copyright (C) 2020, Donald McLean. All rights reserved.
 *
 * This program and the accompanying materials are licensed under
 * the terms of the GNU General Public License version 3.0
 * as published by the Free Software Foundation.
 */
package org.primitive.db

import org.primitive.NeedsLogging
import org.primitive.ml.{Context, ModuleLoader}
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable
import scala.xml.Node

class PropertyTypeTest extends AnyFunSuite with BeforeAndAfter with Matchers with NeedsLogging {
  var defaultContext: Context = _

  before {
    ModuleLoader.shutdown()
    Thread.sleep(100)

    val url = getClass.getResource("neoConnectorTM.xml")
    url should not be null
    ModuleLoader.initialize(url)
    defaultContext = ModuleLoader.getDefaultContext
    initializeLogger(defaultContext)
  }

  private val requiredUnique = <property name="name" type="String" required="true" unique="true"/>
  private val nonRequiredNonUnique = <property name="name" type="String" required="false" unique="false"/>

  test("create a PropertyType") {
    createAPropertyType(requiredUnique)
  }

  test("validate required field") {
    val pr = createAPropertyType(requiredUnique)
    val pn = createAPropertyType(nonRequiredNonUnique)

    val data = mutable.HashMap[String, AnyRef]()

    // case 1: required, but not present: failure
    pr.validate(data.toMap).unsatisfiedFailure should be(true)

    // case 2: not required, not present: success
    pn.validate(data.toMap).satisfiedContinue should be(true)

    data.put("name", "value")
    // case 3: required, present: success
    pr.validate(data.toMap).satisfiedContinue should be(true)

    // case 4: not required, present: success
    pn.validate(data.toMap).satisfiedContinue should be(true)
  }

  // Add this back when unique fields are implemented
//  test("validate unique field") {
//    val pr = createAPropertyType(requiredUnique)
//
//    val data = mutable.HashMap[String, AnyRef]("name" -> "value")
//    pr.validate(data) match {
//      case OpSuccess => // good
//      case a => fail("Should have gotten success, not: " + a)
//    }
//  }

  test("validate property value") {

  }

  private def createAPropertyType(node: Node): PropertyType = {
    val result = PropertyType.create(logger, null, node)
    result.waitForSingle() match {
      case Some(p) => p
      case None => fail("Should have gotten a PropertyType")
    }
  }
}
