/*
 * Copyright (C) 2020, Donald McLean. All rights reserved.
 *
 * This program and the accompanying materials are licensed under
 * the terms of the GNU General Public License version 3.0
 * as published by the Free Software Foundation.
 */
package org.primitive.db

import net.liftweb.actor.LiftActor
import org.neo4j.driver._
import org.primitive.ml._
import org.primitive.{NeedsLogging, ThreadTransactionMarker}
import org.slf4j.Logger

import scala.collection.mutable
import scala.xml.Node

class NeoConnectorModule extends ImportableBasicModule[CypherGateway] {
  override def createT(): CypherGateway = new CypherGateway

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

class CypherGateway extends PServiceStub {
  var actor: NeoConnector = _

  override def start(context: Context, data: Node): OpResult[Unit] = {
    actor = new NeoConnector()
    actor !! ((context, data))
    OpResult.simpleSuccess()
  }

  override def stop(context: Context): Unit = {
    actor ! STOP_ACTOR_MODULE
    actor = null
  }

  def runs[T](e: ExecutableRequest[T]): OpResult[T] = {
    val result = e.createFuture()
    actor ! e
    result
  }
}

/**
 * Defines things that are used for internal service nodes and properties
 */
trait NeoConnectorInternal {
  val OPEN = "\u0391\u0391internal"
  val SEPERATOR = "\u039B\u039B"
  val CLOSE = "\u039E\u039E"
  def internalLabel(prefix: String, suffix: String): String = {
    OPEN + prefix + SEPERATOR + suffix + CLOSE
  }
}

/**
 * Connects to a Neo4j server and performs queries/transactions against that DB
 */
class NeoConnector extends LiftActor with NeedsLogging with ThreadTransactionMarker {

  private var driver: Driver = _
  private val internal = new ConnectorInternal()

  private def start(context: Context, data: Node): Unit = {
    initializeLogger(context)
    NodeLabel.initializeLogger(context)

    logger.debug("[NeoConnector.start] got:\n{}", data)

    var url: String = null
    var authTokens: AuthToken = null
    var nodeTypeDefinitions: Node = null

    data.child.foreach(n => {
      logger.debug("[NeoConnector.start] child: {}", n)
      n.label match {
        case "credentials" =>
          for (userName <- n.attribute("user");
               password <- n.attribute("password")) {
            authTokens = AuthTokens.basic(userName.toString(), password.toString())
          }
        case "url" => url = n.text
        case "typeDefinitions" => nodeTypeDefinitions = n
        case _ => logger.trace("[NeoConnector.start] ignore")
      }
    })

    if (url != null && authTokens != null) {
      driver = GraphDatabase.driver(url, authTokens)
      reply(internal.initializeTypes(nodeTypeDefinitions, logger))
    } else {
      reply(OpResult.failure("Did not get connection info."))
    }
  }

  private def stop(): Unit = {
    println("[stop] enter")
    driver.close()
    internal.clear()
    logger = null
  }

  override protected def messageHandler: PartialFunction[Any, Unit] = {
    case (context: Context, data: Node) => start(context, data)
    case STOP_ACTOR_MODULE => stop()
    case e: ExecutableCypherRequest[_] => executeCypherRequest(e)
    case e: ExecutableNeoRequest[_] => executeNeoRequest(e)
    case e: ExecutableLabelRequest[_] => executeLabelRequest(e)
  }

  private def executeCypherRequest(e: ExecutableCypherRequest[_]): Unit = {
    if (logger == null) return
    val key = beginTrans(logger, "handle ExecutableCypherRequest")
    var result: OpResult[_] = null

    try {
      val session = driver.session()

      val transactionType = e.getType
      transactionType.runTransaction(internal, session, logger, e)
//      e.runRequest(internal, logger, session)
    } catch {
      case exception: Exception =>
        logger.error("[NeoConnector.executeCypherRequest] ", exception)
        e.fail(exception)
    }

    endTrans(logger, key)
    reply(result)
  }

  private def executeLabelRequest(e: ExecutableLabelRequest[_]): Unit = {
    if (logger == null) return
    val key = beginTrans(logger, "handle ExecutableLabelRequest")
    var result: OpResult[_] = null

    try {
      e.runRequest(logger, internal)
    } catch {
      case exception: Exception =>
        logger.error("[NeoConnector.executeCypherRequest] ", exception)
        e.fail(exception)
    }

    endTrans(logger, key)
  }

  private def executeNeoRequest(e: ExecutableNeoRequest[_]): Unit = {
    if (logger == null) return
    val key = beginTrans(logger, "handle ExecutableNeoRequest")
    var result: OpResult[_] = null

    try {
      e.runRequest(logger, driver)
    } catch {
      case exception: Exception =>
        logger.error("[NeoConnector.executeNeoRequest] ", exception)
        e.fail(exception)
    }

    endTrans(logger, key)
    reply(result)
  }
}

/**
 * Used by requests to do things
 */
protected class ConnectorInternal {
  private var nodeTypes: mutable.HashMap[String, NodeLabel] = _

  def initializeTypes(node: Node, logger: Logger): OpResult[Unit] = {
    nodeTypes = mutable.HashMap[String, NodeLabel]()

    node.child.foreach(n => {
      logger.info("[NeoConnector.initializeTypes] next is: {}", n.label)
      if (n.label.equals("type")) {
        val createResult = NodeLabel.create(n, nodeTypes)
        if (createResult.unsatisfiedFailure) return OpResult.failure(createResult)
      }
    })

    logger.info("[NeoConnector.initializeTypes] complete.")
    OpResult.simpleSuccess()
  }

  def clear(): Unit = {
    nodeTypes.clear()
    nodeTypes = null
  }

  def getLabel(labelName: String): OpResult[NodeLabel] = {
    if (nodeTypes == null) OpResult.failure("Node type not defined: " + labelName)
    else nodeTypes.get(labelName) match {
      case None => OpResult.failure("Node type not defined: " + labelName)
      case Some(l) => OpResult.successWithDatum(l)
    }
  }

  def getLabel(logger: Logger, labelName: String, future: OpResult[NodeLabel]): Unit = {
    if (nodeTypes == null) {
      logger.warn("[NeoConnector.fetchNodeType] no type names defined.")
      future.fail("Node type not defined: " + labelName)
    }
    else nodeTypes.get(labelName) match {
      case None =>
        logger.warn("[NeoConnector.fetchNodeType] type name {} not defined.")
        future.fail("Node type not defined: " + labelName)
      case Some(node) => future.setResult(node)
    }
  }
}

trait NodeSpaceStatus

object NSUndefined extends NodeSpaceStatus
object NSActive extends NodeSpaceStatus
object NSRemoved extends NodeSpaceStatus
object NSPlaceholder extends NodeSpaceStatus

/**
 * Represents Nodes that are part of a grouping for a particular use
 * @param name
 * @param id
 */
abstract class NodeSpace(val g: CypherGateway, val name: String) {
  var id: Long = _
}

object NodeSpace extends NeoConnectorInternal {
  val SPACE_ID_PROPERTY = internalLabel("property", "spaceID")

  private val spaces = mutable.HashMap[String, NodeSpace]()

  def apply(): NodeSpace = null

  def apply(g: CypherGateway, name: String): NodeSpace = {
    spaces.get(name) match {
      case Some(space) => return space
      case None => // continue
    }

    val result = new INodeSpace(g, name, -1L)
    result.status = NSPlaceholder
    spaces.put(name, result)

    return result
  }

  def apply(logger: Logger, g: CypherGateway, n: types.Node): NodeSpace = {
    val name = n.get("name").asString()
    logger.info("[NodeSpace.apply] got: {}", name)

    spaces.get(name) match {
      case Some(space) => return space
      case None => // continue
    }

    val id = n.get(NodeIndex.INDEX_PROPERTY).asLong()
    logger.info("[NodeSpace.apply] got: {}", id)

    val result = new INodeSpace(g, name, id)
    result.status = NSActive
    spaces.put(name, result)
    result
  }

  private class INodeSpace(g: CypherGateway, name: String) extends NodeSpace(g, name) {
    var status: NodeSpaceStatus = NSUndefined

    def this(g: CypherGateway, name: String, id: Long) {
      this(g, name)
      this.id = id
    }
  }
}

case class GetNodeType(name: String)
