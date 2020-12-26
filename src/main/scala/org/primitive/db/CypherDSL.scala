/*
 * Copyright (C) 2020, Donald McLean. All rights reserved.
 *
 * This program and the accompanying materials are licensed under
 * the terms of the GNU General Public License version 3.0
 * as published by the Free Software Foundation.
 */
package org.primitive.db

import java.{lang, util}

import org.neo4j.driver.{Driver, Record, Transaction, types}
import org.primitive.ml.OpResult
import org.slf4j.Logger

import scala.jdk.CollectionConverters.CollectionHasAsScala

trait CypherDSL {
  /**
   * Implicitly converts a <code>CypherGateway</code> to a <code>CypherWrapper</code>,
   * to enable DSL methods to be invokable on that object.
   */
  implicit def convertToCypherWrapper(g: CypherGateway): CypherWrapper = new CypherWrapper(g, None)

  implicit def convertToCypherWrapper(n: NodeSpace): CypherWrapper = new CypherWrapper(n.g, Option(n))

  val label = new LabelToken
  val isConnected = new IsConnectedToken
  val nextIndex = new NextIndexToken
  val withIndex = new IndexToken
  val nodeSpace = new NodeSpaceToken
}

//noinspection ScalaUnusedSymbol
class CypherWrapper(g: CypherGateway, space: Option[NodeSpace]) extends NeedsClauseNames {
  def test(c: IsConnectedToken): OpResult[Boolean] = g.runs[Boolean](new IsConnectedRequest())

  def request(l: LabelTokenWithLabelName): OpResult[NodeLabel] = g.runs[NodeLabel](new LabelRequest(g, l.labelName))

  def request(n: NextIndexToken): OpResult[java.lang.Long] = g.runs(NodeIndex)

  /*
   *  DSL commands relating to node spaces
   */
  def CREATE(o: NodeSpaceToken): CreateNodeSpaceOp = new CreateNodeSpaceOp(g)

  def IN(s: NodeSpace): InNodeSpaceOp = new InNodeSpaceOp(g, s)
  def IN(name: String): InNodeSpaceOp = new InNodeSpaceOp(g, NodeSpace(g, name))

  /*
   * Regular DSL commands (for node space versions see InNodeSpaceOp)
   */
  def CREATE(l: LabelTokenWithLabel): CreateNodeOp = new CreateNodeOp(g, None, l.label)

  def MATCH(l: LabelTokenWithLabel): OpResult[NeoNode] = g.runs(new MatchLabel(None, l.label))
  def MATCH(i: IndexValueToken): OpResult[NeoNode] = g.runs(new MatchIndex(space, RETURN_CLAUSE, i.index, ReadTransaction))

  def DELETE(i: IndexValueToken): OpResult[NeoNode] = g.runs(new MatchIndex(space, DELETE_CLAUSE, i.index, WriteTransaction))
  def DELETE(s: NodeSpace): OpResult[Unit] = g.runs(new DeleteNodeSpace(s))
}

class LabelToken {
  def apply(labelName: String): LabelTokenWithLabelName = new LabelTokenWithLabelName(labelName)
  def apply(label: NodeLabel): LabelTokenWithLabel = new LabelTokenWithLabel(label)
}

class IndexToken {
  def apply(indexValue: Long): IndexValueToken = new IndexValueToken(indexValue)
}

/**
 * Implements the token "nodeSpace" in the DSL
 */
class NodeSpaceToken

class IsConnectedToken

class IsConnectedRequest extends ExecutableNeoRequest[Boolean] {
  override def runRequest(logger: Logger, driver: Driver): Unit = {
    if (logger == null) {
      future.fail("System is shut down?")
      return
    }

    val key = beginTrans(logger, "IsConnectedRequest.runRequest")
    var response: Boolean = false
    try {
      driver.verifyConnectivity()
      response = true
    } catch {
      case exception: Exception =>
        logger.error("[IsConnectedRequest.runRequest] verifying connection", exception)
        fail(exception)
    }

    future.setResult(response)
    logger.info("[NeoConnector.checkIsConnected] got: {}", response)
    endTrans(logger, key)
  }
}

class LabelTokenWithLabelName(val labelName: String)
class LabelTokenWithLabel(val label: NodeLabel)
class NextIndexToken
class IndexValueToken(val index: Long)

class CreateNodeOp(g: CypherGateway, s: Option[NodeSpace], l: NodeLabel) {
  def data(d: List[(String, AnyRef)]): OpResult[NeoNode] = {
    val data = s match {
      case None => d
      case Some(ns) => (NodeSpace.SPACE_ID_PROPERTY, java.lang.Long.valueOf(ns.id)) :: d
    }

    g.runs(new CreateNode(l, data))
  }
}

class CreateNodeSpaceOp(g: CypherGateway) {
  def label(name: String): OpResult[NodeSpace] = g.runs(new CreateNodeSpace(g, name))
}

class InNodeSpaceOp(g: CypherGateway, space: NodeSpace) extends NeedsClauseNames {
  /*
   * Node space versions, (for regular DSL commands see CypherWrapper))
   */
  def CREATE(l: LabelTokenWithLabel): CreateNodeOp = new CreateNodeOp(g, Some(space), l.label)

  def MATCH(l: LabelTokenWithLabel): OpResult[NeoNode] = g.runs(new MatchLabel(Some(space), l.label))
  def MATCH(i: IndexValueToken): OpResult[NeoNode] = g.runs(new MatchIndex(Some(space), RETURN_CLAUSE, i.index, ReadTransaction))

  def DELETE(i: IndexValueToken): OpResult[NeoNode] = g.runs(new MatchIndex(Some(space), DELETE_CLAUSE, i.index, WriteTransaction))
}

class NeedLabelNameForRequest(g: CypherGateway) {
  def apply(labelName: String): OpResult[NodeLabel] = {
    g.runs(new LabelRequest(g, labelName))
  }
}

class LabelRequest(g: CypherGateway, labelName: String) extends ExecutableLabelRequest[NodeLabel] {

  override def runRequest(logger: Logger, internal: ConnectorInternal): Unit = {
    if (logger == null) {
      future.fail("We are shut down")
      return
    }

    val key = beginTrans(logger, "fetchNodeType." + labelName)

    internal.getLabel(logger, labelName, future)
    endTrans(logger, key)
  }
}

object Create {
  def label(nodeType: NodeLabel): CreateNode = new CreateNode(nodeType, Nil)
}

/**
 * Creates a node space, a virtual node container
 * @param label name to be given to the node space
 */
class CreateNodeSpace(g: CypherGateway, label: String)
  extends ExecutableCypherRequest[NodeSpace]
    with NeoConnectorInternal {
  val SPACE_NODE_LABEL: String = internalLabel("node", "nodeSpacePrimary")

  private val queryString = "MATCH (n :" + SPACE_NODE_LABEL + "{ name: $name})\n" +
    "RETURN n"

  override def getType: TransactionType = WriteTransaction

  override def runRequest(internal: ConnectorInternal, logger: Logger, transaction: Transaction): Unit = {
    logger.warn("[CreateNodeSpace.runRequest] enter, label: {}", label)
    val parameters = List[(String, AnyRef)](("name", label))
    var resultData: util.List[Record] = null

    resultData = runSimpleQuery(logger, transaction, queryString, parameters)

    if (resultData.isEmpty) {
      logger.info("[CreateNodeSpace.runRequest] No existing space, creating {}", label)
      resultData = doCreateSpace(internal, logger, transaction)
    } else { // return existing space and message
      logger.info("[CreateNodeSpace.runRequest] found space, returning {}")
      future.addMessage("Returning existing space named '" + label + "'.")
    }

    if (resultData.isEmpty) return

    val node: types.Node = resultData.get(0).get("n").asNode()
    val result = NodeSpace(logger, g, node)

    logger.info("[CreateNodeSpace.runRequest] got {}", result)
    future.setResult(result)
  }

  private def doCreateSpace(internal: ConnectorInternal, logger: Logger, transaction: Transaction): util.List[Record] = {
    logger.trace("[CreateNodeSpace.doCreateSpace] enter")

    val index: lang.Long = getIndex(logger, transaction) match {
      case Some(result) => result
      case None => return new util.ArrayList[Record]()
    }

    val pList = List[(String, AnyRef)]((NodeIndex.INDEX_PROPERTY, index), ("name", label))
    logger.info("[Create.runRequest] pList: {}", pList)

    val queryString = "CREATE (n :" + SPACE_NODE_LABEL +
      " {" + NodeIndex.INDEX_PROPERTY + ": $" + NodeIndex.INDEX_PROPERTY +
      ", name: $name})\n" +
      "RETURN n"
    runSimpleQuery(logger, transaction, queryString, pList)
  }
}

/**
 * Runs a simple CREATE, of the form:
 * CREATE (variable:Label1:Label2 { propertyName1: 'property value 1', propertyName2: 'property value 2' })
 *
 * @param nodeType Information about the type of node, including the labels to be applied
 * @param props A list of properties to be set on the node
 */
class CreateNode(nodeType: NodeLabel, props: List[(String, AnyRef)])
  extends ExecutableCypherRequest[NeoNode] {

  def properties(p: List[(String, AnyRef)]): CreateNode = new CreateNode(nodeType, p)

  override def getType: TransactionType = WriteTransaction

  override def runRequest(internal: ConnectorInternal, logger: Logger, transaction: Transaction): Unit = {
    val index: lang.Long = getIndex(logger, transaction) match {
      case Some(result) => result
      case None => return
    }
    logger.warn("[CreateNode.runRequest] got index {}", index)

    val pList = (NodeIndex.INDEX_PROPERTY, index) :: props
    logger.info("[Create.runRequest] pList: {}", pList)

    val queryString: String = createQueryString(logger, CREATE_CLAUSE, RETURN_CLAUSE, List[NodeLabel](nodeType), pList)
    runBaseQuery(logger, transaction, queryString, pList) match {
      case null => future.fail("Exception running query.")
      case result =>
        val rawData = result.list().asScala
        val resultData: OpResult[NeoNode] = rawData.map(doOneRecord(internal, logger)).reduce(OpResult.softMerge[NeoNode])
        logger.info("[MatchLabel.runRequest] result: {}", resultData)

        resultData.singularOption match {
          case Some(node) =>
            logger.info("[CreateNodeRequest.runRequest] got: {}", node)
            future.setResult(node)
          case None => future.fail("Unknown problem getting created node.")
        }
    }
  }
}

class MatchLabel(space: Option[NodeSpace], label: NodeLabel)
  extends ExecutableCypherRequest[NeoNode] {


  override def getType: TransactionType = ReadTransaction

  /**
   * Run the Cypher request and give the result to the future from the parent trait
   *
   * @param logger  for any logging the request wants to do
   * @param transaction a Neo4j read transaction that can be used to perform the request
   */
  override def runRequest(internal: ConnectorInternal, logger: Logger, transaction: Transaction): Unit = {
    val queryString: String = createQueryString(logger, MATCH_CLAUSE, RETURN_CLAUSE, List[NodeLabel](label), Nil)

    runBaseQuery(logger, transaction, queryString, Nil) match {
      case null => future.fail("Exception running query.")
      case result =>
        val rawData = result.list().asScala
        val resultData: OpResult[NeoNode] = rawData.map(doOneRecord(internal, logger)).reduce(OpResult.softMerge[NeoNode])
        logger.info("[MatchLabel.runRequest] result: {}", resultData)
        future.complete(resultData)
        countersAsMessages(future, result)
    }
  }
}

class MatchIndex(space: Option[NodeSpace], actionClause: String, index: Long, transactionType: TransactionType)
  extends ExecutableCypherRequest[NeoNode] {

  override def getType: TransactionType = transactionType

  /**
   * Run the Cypher request and give the result to the future from the parent trait
   *
   * @param logger  for any logging the request wants to do
   * @param transaction a Neo4j session that can be used to perform the request
   */
  override def runRequest(internal: ConnectorInternal, logger: Logger, transaction: Transaction): Unit = {
    logger.info("[MatchIndex.runRequest] begin")
    val l = List[(String, AnyRef)]((NodeIndex.INDEX_PROPERTY, java.lang.Long.valueOf(index)))
    val queryString: String = createQueryString(logger, MATCH_CLAUSE, actionClause, Nil, l)
    runBaseQuery(logger, transaction, queryString, l) match {
      case null => future.fail("Exception running query.")
      case result =>
        val list = result.list()
        if (list.isEmpty) {
          logger.info("[MatchIndex.runRequest] query produced no records.")
          future.success()
        } else {
          val resultData: OpResult[NeoNode] = list.asScala.map(doOneRecord(internal, logger)).reduce(OpResult.softMerge[NeoNode])
          logger.info("[MatchLabel.runRequest] result: {}", resultData)
          future.complete(resultData)
        }
        countersAsMessages(future, result)
    }
    logger.info("[MatchIndex.runRequest] complete.")
  }
}

class DeleteNodeSpace(space: NodeSpace) extends ExecutableCypherRequest[Unit] with RunsQueries[Unit] {
  val QUERY_FAILED = "Exception occurred running query."
  val UNEXPECTED_COUNT = "The query return count numbers that are inconsistent with what was expected."

  override def getType: TransactionType = WriteTransaction

  /**
   * Run the Cypher request and give the result to the future from the parent trait
   *
   * @param logger  for any logging the request wants to do
   * @param transaction a Neo4j write transaction that can be used to perform the request
   */
  override def runRequest(internal: ConnectorInternal, logger: Logger, transaction: Transaction): Unit = {
    var subnodeResult: OpResult[Map[String, Int]] = null
    var spaceResult: OpResult[Unit] = null

    subnodeResult = deleteSubnodes(transaction, logger, future)
    if (subnodeResult.unsatisfiedFailure) return

    spaceResult = deleteSpace(transaction, logger, future)
    if (spaceResult.unsatisfiedFailure) return

    val message = subnodeResult.singularOption match {
      case None => ", results not returned by sub-task" // Shouldn't get this
      case Some(map) => map.map(entry => {
        if (entry._2 > 0) ", " + entry._1 + ": " + entry._2
        else ""
      }).reduce(_ + _)
    }
    future.addMessage("Space deleted" + message)
    future.success()
  }

  /** If the node space contains any nodes, connections, etc, delete them all first
    */
  def deleteSubnodes(transaction: Transaction, logger: Logger, future: OpResult[Unit]): OpResult[Map[String, Int]] = {
    val l = List[(String, AnyRef)]((NodeSpace.SPACE_ID_PROPERTY, java.lang.Long.valueOf(space.id)))
    val queryString: String = createQueryString(logger, MATCH_CLAUSE, DELETE_CLAUSE, Nil, l)

    runBaseQuery(logger, transaction, queryString, l) match {
      case null =>
        future.fail(QUERY_FAILED)
        OpResult.failure(QUERY_FAILED)
      case result =>
        val mirror = mirrorList(List(DeletedNodes, DeletedRelationships, IndexesDeleted, RemovedConstraints))
        val values = counterExpectations(result, mirror, Nil).singularOption match {
          case None =>
            future.fail(UNEXPECTED_COUNT)
            return OpResult.failure(UNEXPECTED_COUNT)
          case Some(v) => v
        }
        OpResult.successWithDatum(values)
    }
  }

  /** Delete the node space itself, verifying that only one node was deleted
    */
  def deleteSpace(transaction: Transaction, logger: Logger, future: OpResult[Unit]): OpResult[Unit] = {
    val l = List[(String, AnyRef)]((NodeIndex.INDEX_PROPERTY, java.lang.Long.valueOf(space.id)))
    val queryString: String = createQueryString(logger, MATCH_CLAUSE, DELETE_CLAUSE, Nil, l)

    runBaseQuery(logger, transaction, queryString, l) match {
      case null =>
        future.fail(QUERY_FAILED)
        OpResult.failure(QUERY_FAILED)
      case result =>
        val mirror = mirrorList(List(DeletedNodes))
        val opResult = counterExpectations(result, mirror, List(DeletedNodes))
        if (opResult.unsatisfiedFailure) {
          future.fail(UNEXPECTED_COUNT)
          OpResult.failure(UNEXPECTED_COUNT)
        } else {
          val values: Map[String, Int] = opResult.singularOption.get // it isn't a failure, so 'get' is safe
          if (values(DeletedNodes.getName) == 1) {
            OpResult.simpleSuccess()
          } else {
            future.fail(UNEXPECTED_COUNT)
            OpResult.failure(UNEXPECTED_COUNT)
          }
        }
    }
  }
}

/**
 * Manages a singleton node in the DB that is used to assign unique indexes to nodes
 * TODO: what about connections?
 */
object NodeIndex extends ExecutableCypherRequest[java.lang.Long] with NeoConnectorInternal {
  val INDEX_PROPERTY: String = internalLabel("property", "Index")
  val INDEX_NODE_LABEL: String = internalLabel("node", "singletonIndex")

  override def getType: TransactionType = WriteTransaction

  /**
    * Run the Cypher request and give the result to the future from the parent trait
    *
    * @param logger  for any logging the request wants to do
    * @param transaction a Neo4j write transaction that can be used to perform the request
    */
  override def runRequest(internal: ConnectorInternal, logger: Logger, transaction: Transaction): Unit = {
    runRequest(logger, transaction)
  }

  /**
   * Runs a MATCH looking for the node, finds it and gets the value or calls create
   *
   * @param logger a Logger for debugging
   * @param transaction the DB session for running queries
   * @return
   */
  def runRequest(logger: Logger, transaction: Transaction): Unit = {
    val queryString = "MATCH (n :" + INDEX_NODE_LABEL + ")\n" +
      "SET n.value = n.value + 1\n" +
      "RETURN n.value"

    val resultData: util.List[Record] = runSimpleQuery(logger, transaction, queryString)

    val index: Long = if (resultData.isEmpty) {
      createMasterIndex(logger, transaction)
    } else {
      resultData.get(0).get("n.value").asLong(-1)
    }

    future.setResult(index)
    logger.info("[NodeIndex.runRequest] indexNode: {}", index)
  }

  def createMasterIndex(logger: Logger, transaction: Transaction): Long = {
    logger.info("[NodeIndex.createMasterIndex] enter")
    val queryString = "CREATE (n :" + INDEX_NODE_LABEL + " { value: 1})\n" +
      "RETURN n.value"
    val resultData: util.List[Record] = runSimpleQuery(logger, transaction, queryString)

    resultData.get(0).get("n.value").asLong(-1)
  }
}