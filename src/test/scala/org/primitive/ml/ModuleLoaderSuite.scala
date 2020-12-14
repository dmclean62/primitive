/*
 * Copyright (C) 2020, Donald McLean. All rights reserved.
 *
 * This program and the accompanying materials are licensed under
 * the terms of the GNU General Public License version 3.0
 * as published by the Free Software Foundation.
 */
package org.primitive.ml

import net.liftweb.actor.LiftActor
import org.scalatest.{BeforeAndAfter, Sequential}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.xml.Node

class ModuleLoaderSuite extends Sequential (
  new CantLoadInvalidManifest,
  new LoadSimpleTestModule,
  new LoadModulesWithFileReferences,
  new HandleInvalidModule,
  new ActorModule
)

/**
  * Test the functionality of the ModuleLoader object.
  */
class ModuleLoaderTest extends AnyFunSuite with BeforeAndAfter with Matchers {
  before {
    ModuleLoader.shutdown()
    Thread.sleep(100)
  }

  /**
    * Clean up the ModuleLoader after each test.
    */
  after {
    ModuleLoader.shutdown()
    Thread.sleep(50)
  }
}

class CantLoadInvalidManifest extends ModuleLoaderTest {
  test("can't load invalid manifest") {
    System.err.println("Testing.")
    val result = ModuleLoader.initialize("invalidManifest.xml")
    result.unsatisfiedFailure should be(true)
  }
}

class LoadSimpleTestModule extends ModuleLoaderTest {
  /**
    * Positive test that ModuleLoader can load a simple test module.
    */
  test("load simple test module") {
    val url = getClass.getResource("moduleLoaderTestManifest.xml")
    url should not be null
    System.err.println("url = " + url)
    ModuleLoader.initialize(url)

    val defaultContext = ModuleLoader.getDefaultContext
    defaultContext should not be null

    defaultContext.getModuleCount should be (3)
  }
}

class LoadModulesWithFileReferences extends ModuleLoaderTest {
  test("load modules with file references") {
    ModuleLoader.initialize("mlFromFileTM.xml")

    val defaultContext = ModuleLoader.getDefaultContext
    defaultContext should not be null

    defaultContext.getModuleCount should be (3)
  }
}

class HandleInvalidModule extends ModuleLoaderTest {
  /**
    * Test correct handling of problems in the module definition file.
    */
  test("handle invalid module definition") {
    val result = ModuleLoader.initialize("moduleLoaderErrorTestManifest.xml")
    result.unsatisfiedFailure should be(true)

    val defaultContext = ModuleLoader.getDefaultContext
    defaultContext should not be null

    defaultContext.getModuleCount should be (2)
  }
}

class ActorModule extends ModuleLoaderTest {
  test("basic actor module") {
    val result = ModuleLoader.initialize("basicActorModuleTM.xml")
    result.satisfiedContinue should be(true)

    val defaultContext = ModuleLoader.getDefaultContext
    defaultContext should not be null

    defaultContext.getModuleCount should be (3)
  }
}

//TODO: write tests showing correct handling of valid and invalid multiple manifest conditions

/**
 * This is a simple BasicActorModule that creates a test actor
 */
class TestActorModule extends BasicActorModule {
  override def createActor(): LiftActor = new TestServiceActor()

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

class TestServiceActor extends LiftActor {
  override protected def messageHandler: PartialFunction[Any, Unit] = {
    case (_: Context, _: Node) => reply(OpResult.simpleSuccess())
    case _ => reply(OpResult.failure("Improper initialization."))
  }
}