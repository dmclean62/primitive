/*
 * Copyright (C) 2020, Donald McLean. All rights reserved.
 *
 * This program and the accompanying materials are licensed under
 * the terms of the GNU General Public License version 3.0
 * as published by the Free Software Foundation.
 */
package org.primitive.db

import org.apache.commons.lang3.RandomStringUtils
import org.primitive.NeedsLogging
import org.primitive.ml.{Context, ModuleLoader, OpResult}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfter, Sequential}

import scala.collection.mutable

class NeoConnectorSuite extends Sequential(
  new NCSSetUp,
  new IsConnected,
  new CreateSpace,
  new NodeDefinition,
  new UniversalIndex,
  new GlobalCreate,
  new GlobalFindByLabel,
  new GlobalDeleteByIndex,
  new CreateNodeInASpace,
  new DeleteEntireNodeSpace,
  new NCSTearDown
)

//noinspection TypeAnnotation
object NeoConnectorTest {
  var defaultContext: Context = _

  val campaignName = "Blah test"
  val campaignOwner = "Some Person"
  val TEST_SPACE: String = RandomStringUtils.randomAlphanumeric(20)

  val SPACE_KEY = "test space"
  val RECORD_INDEX_KEY = "record Index"

  val properties = mutable.HashMap[String, Any]()

  def campaignData() = List[(String, AnyRef)]("name" -> campaignName, "owner" -> campaignOwner)
}

trait NeoConnectorTest extends AnyFunSuite with Matchers with NeedsLogging with CypherDSL {
  def initialize(manifestName: String): Unit = {
    val url = getClass.getResource(manifestName)
    convertToAnyShouldWrapper(url) should not.be(null)
    ModuleLoader.initialize(url)
    NeoConnectorTest.defaultContext = ModuleLoader.getDefaultContext
  }

  protected def getGateway: CypherGateway = {
    NeoConnectorTest.defaultContext.findService[CypherGateway] match {
      case None => fail("Should have gotten connector.")
      case Some(g: CypherGateway) => g
    }
  }

  protected def getLabel(gateway: CypherGateway): NodeLabel = {
    val result: OpResult[NodeLabel] = gateway request label("TravellerCampaign")

    logger.info("[GlobalCreate.test] got: {}", result)
    val travellerCampaign: NodeLabel = result.waitForSingle() match {
      case Some(result) => result
      case None => fail("Expected node label")
    }

    logger.info("[GlobalCreate.test] node label: {}", travellerCampaign)
    travellerCampaign
  }
}

class NCSSetUp extends NeoConnectorTest with BeforeAndAfter {
  before {
    ModuleLoader.shutdown()
    Thread.sleep(100)
  }

  test("set up") {
    initialize("neoConnectorTM.xml")
  }
}

class IsConnected extends NeoConnectorTest {
  test("is connected") {
    initializeLogger(NeoConnectorTest.defaultContext)

    val gateway: CypherGateway = getGateway
    (gateway test isConnected).waitForSingle(logger) match {
      case Some(response) =>
        logger.info("[IsConnected.test] got response: {}", response)
        response should be(true)
      case None => fail("Should have gotten a single Boolean value")
    }
  }
}

class CreateSpace extends NeoConnectorTest {
  test("create space") {
    initializeLogger(NeoConnectorTest.defaultContext)

    val gateway: CypherGateway = getGateway
    (gateway CREATE nodeSpace label NeoConnectorTest.TEST_SPACE).waitForSingle(logger) match {
      case Some(space) => // we are good, carry case _: scala.None.type =>
        NeoConnectorTest.properties.put(NeoConnectorTest.TEST_SPACE, space)
      case None => fail("Should have gotten a node space.")
    }
  }
}

class NodeDefinition extends NeoConnectorTest {
  test("node definition") {
    initializeLogger(NeoConnectorTest.defaultContext)
    val gateway: CypherGateway = getGateway
    val result = gateway request label("TravellerCampaign")
    logger.info("[NodeDefinition.test] got: {}", result)
    val definition = result.waitForSingle() match {
      case Some(result) =>
        logger.trace("[NodeDefinition.test] node type: {}", result)
        result
      case None => fail("Should have gotten NodeLabel")
    }

    val data = mutable.HashMap[String, AnyRef]()
    var validateResult = definition.validate(data.toMap)
    validateResult.continue match {
      case Some(false) =>
        val messages = validateResult.getMessages.get
        convertToStringShouldWrapper(messages.head) should be("Missing required value: name")
      case a => fail("Expected failure Some(false), got " + a)
    }

    data.put("name", "Spinward out of control")
    validateResult = definition.validate(data.toMap)
    validateResult.continue match {
      case Some(false) =>
        val messages = validateResult.getMessages.get
        convertToStringShouldWrapper(messages.head) should be("Missing required value: owner")
      case a => fail(message = "Expected failure Some(false), got " + a)
    }

    data.put("owner", "dmclean")
    validateResult = definition.validate(data.toMap)
    validateResult.continue match {
      case Some(true) => // we're good
      case a => fail(message = "Expected success Some(true), got " + a)
    }
  }
}

class UniversalIndex extends NeoConnectorTest {
  test("universal index") {
    initializeLogger(NeoConnectorTest.defaultContext)
    logger.info("[UniversalIndex.test] begin test.")

    val gateway = getGateway

    val future = gateway request nextIndex
    logger.info("[UniversalIndex.test] got: {}", future)
    future.waitForSingle() match {
      case Some(value) =>
        if (value <= 0) fail("Invalid index.")
        logger.info("[UniversalIndex.test] got value: {}", value)
        value
      case None => fail("Should have gotten a value.")
    }
  }
}

/**
 * test creation of a node that is part of the global space (does not
 * belong to a node space)
 */
class GlobalCreate extends NeoConnectorTest {
  test("simple global create") {
    initializeLogger(NeoConnectorTest.defaultContext)
    logger.info("[GlobalCreate.test] begin test.")

    val gateway: CypherGateway = getGateway
    val travellerCampaign: NodeLabel = getLabel(gateway)

    val testProperties = NeoConnectorTest.campaignData()
    val createResult = gateway CREATE label(travellerCampaign) data testProperties
    //properties testProperties
    logger.info("[GlobalCreate.test] create result: {}", createResult)
    createResult.waitForSingle() match {
      case Some(v) =>
        logger.info("[GlobalCreate.test] retrieved node: {}", v)
        NeoConnectorTest.properties.put(NeoConnectorTest.RECORD_INDEX_KEY, v.getIndex)
      case None => fail("Should have gotten value (create failed?)")
    }
    logger.info("[GlobalCreate.test] end test.")
  }
}

class GlobalFindByLabel extends NeoConnectorTest {
  test("global find by label") {
    initializeLogger(NeoConnectorTest.defaultContext)
    logger.info("[GlobalFindByLabel.test] begin test.")

    val gateway: CypherGateway = getGateway
    val travellerCampaign: NodeLabel = getLabel(gateway)

    val matchResult = gateway MATCH label(travellerCampaign)
    logger.info("[GlobalFindByLabel.test] matchResult: {}", matchResult)

    matchResult.waitForSingle(logger, 30000) match {
      case Some(v) =>
        logger.info("[GlobalFindByLabel.test] got: {}", v)
        NeoConnectorTest.properties.get(NeoConnectorTest.RECORD_INDEX_KEY) match {
          case Some(value) => value should be(v.getIndex)
          case None => fail("Index of created node should be in test properties.")
        }
      case None => fail("Should have gotten value (match failed?)")
    }

    logger.info("[GlobalFindByLabel.test] end test.")
  }
}

class GlobalFindByIndex extends NeoConnectorTest {
  test("global find by index") {
    initializeLogger(NeoConnectorTest.defaultContext)
    logger.info("[GlobalFindByLIndex.test] begin test.")

    val gateway: CypherGateway = getGateway

    val index: Long = NeoConnectorTest.properties.get(NeoConnectorTest.RECORD_INDEX_KEY) match {
      case Some(value) => value.asInstanceOf[Long]
      case None => fail("Index of created node should be in test properties.")
    }

    val matchResult = gateway MATCH withIndex(index)
    logger.info("[GlobalFindByLabel.test] matchResult: {}", matchResult)

    matchResult.waitForSingle(logger, 30000) match {
      case Some(v) =>
        logger.info("[GlobalFindByLabel.test] got: {}", v)
        NeoConnectorTest.properties.get(NeoConnectorTest.RECORD_INDEX_KEY) match {
          case Some(value) => value should be(v.getIndex)
          case None => fail("Index of created node should be in test properties.")
        }
      case None => fail("Should have gotten value (match failed?)")
    }

    logger.info("[GlobalFindByLabel.test] end test.")
  }
}

class GlobalDeleteByIndex extends NeoConnectorTest {
  test("global delete by Index") {
    initializeLogger(NeoConnectorTest.defaultContext)
    logger.info("[GlobalDeleteByIndex.test] begin test.")

    val gateway: CypherGateway = getGateway

    val index: Long = NeoConnectorTest.properties.get(NeoConnectorTest.RECORD_INDEX_KEY) match {
      case Some(value) => value.asInstanceOf[Long]
      case None => fail("Index of created node should be in test properties.")
    }

    val deleteResult = gateway DELETE withIndex(index)

    deleteResult.waitFor()
    logger.info("[GlobalDeleteByIndex.test] deleteResult: {}", deleteResult)

    deleteResult.getMessages match {
      case Some(messages) =>
        logger.info("[GlobalDeleteByIndex.test] messages: {}", messages)
        messages should contain("Nodes deleted: 1")
      case None => fail("Should have gotten messages.")
    }
  }
}

/**
 * test creation of a node that is part of a node space
 */
class CreateNodeInASpace extends NeoConnectorTest {
  test("simple node space create") {
    initializeLogger(NeoConnectorTest.defaultContext)
    logger.info("[CreateNodeInASpace.test] begin test.")

    val gateway: CypherGateway = getGateway

    val testSpace = NeoConnectorTest.properties.get(NeoConnectorTest.TEST_SPACE) match {
      case Some(s: NodeSpace) => s
      case None => fail("Prior test should have stored the node space")
    }

    val travellerCampaign: NodeLabel = getLabel(gateway)

    val testProperties = NeoConnectorTest.campaignData()
    val createResult = gateway IN testSpace CREATE label(travellerCampaign) data testProperties
    //properties testProperties
    logger.info("[CreateNodeInASpace.test] create result: {}", createResult)
    createResult.waitForSingle() match {
      case Some(v) =>
        logger.info("[CreateNodeInASpace.test] retrieved node: {}", v)
        NeoConnectorTest.properties.put(NeoConnectorTest.RECORD_INDEX_KEY, v.getIndex)
      case None => fail("Should have gotten value (create failed?)")
    }
    logger.info("[CreateNodeInASpace.test] end test.")
  }
}

class DeleteEntireNodeSpace extends NeoConnectorTest {
  test("delete entire node space") {
    initializeLogger(NeoConnectorTest.defaultContext)
    logger.info("[DeleteEntireNodeSpace.test] begin test.")

    val gateway: CypherGateway = getGateway

    val testSpace = NeoConnectorTest.properties.get(NeoConnectorTest.TEST_SPACE) match {
      case Some(s: NodeSpace) => s
      case None => fail("Prior test should have stored the node space")
    }

    val deleteResult = gateway DELETE testSpace

    deleteResult.waitFor()
    logger.info("[DeleteEntireNodeSpace.test] deleteResult: {}", deleteResult)

    deleteResult.getMessages match {
      case Some(messages) =>
        logger.info("[DeleteEntireNodeSpace.test] messages: {}", messages)
        messages should contain("Space deleted, nodes deleted: 1")
      case None => fail("Should have gotten messages.")
    }
  }
}

class NCSTearDown extends NeoConnectorTest with BeforeAndAfter {
  test("tear down") {
    initializeLogger(NeoConnectorTest.defaultContext)
  }

  after {
    logger.info("[NeoConnectorTest.after] cleaning up.")
    ModuleLoader.shutdown()
    logger_=(null)
    NeoConnectorTest.defaultContext_=(null)
    Thread.sleep(100)
  }
}