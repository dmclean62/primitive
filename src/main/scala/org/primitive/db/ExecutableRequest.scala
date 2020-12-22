/*
 * Copyright (C) 2020, Donald McLean. All rights reserved.
 *
 * This program and the accompanying materials are licensed under
 * the terms of the GNU General Public License version 3.0
 * as published by the Free Software Foundation.
 */
package org.primitive.db

import java.util

import org.neo4j.driver.summary.SummaryCounters
import org.neo4j.driver.{Driver, Record, Result, Session, Transaction}
import org.primitive.ThreadTransactionMarker
import org.primitive.db.NodeLabel.logger
import org.primitive.ml.OpResult
import org.slf4j.Logger

import scala.collection.mutable

trait ExecutableRequest[T] {
  protected var future: OpResult[T] = _

  /**
   * Ask the request to create a future object of the correct type
   *
   * @return a future with result type T
   */
  def createFuture(): OpResult[T] = {
    future = OpResult.future()
    future
  }

  /**
   * Used to notify the request that an exception occurred during execution
   */
  def fail(e: Exception): Unit = {
    logger.error("[ExecutableRequest.fail] ", e)
    future.fail(e)
  }

  protected def getIndex(logger: Logger, transaction: Transaction): Option[java.lang.Long] = {
    val f = NodeIndex.createFuture()
    NodeIndex.runRequest(logger, transaction)
    val index = f.singularOption match {
      case Some(i) => i
      case None =>
        future.fail("Got invalid index response")
        return None
    }
    Some(index)
  }
}

trait ExecutableLabelRequest[T] extends ExecutableRequest[T] with ThreadTransactionMarker {
  /**
   * Run the label request and give the result to the future from the parent trait
   * @param logger for any logging the request wants to do
   * @param internal private data used by request objects
   */
  def runRequest(logger: Logger, internal: ConnectorInternal): Unit
}

trait ExecutableNeoRequest[T] extends ExecutableRequest[T] with ThreadTransactionMarker {
  /**
   * Run the Neo4j request and give the result to the future from the parent trait
   * @param logger for any logging the request wants to do
   * @param driver a Neo4j driver that can be used to perform the request
   */
  def runRequest(logger: Logger, driver: Driver): Unit
}

trait NeedsClauseNames {
  val MATCH_CLAUSE = "MATCH"
  val CREATE_CLAUSE = "CREATE"
  val RETURN_CLAUSE = "RETURN"
  val DELETE_CLAUSE = "DELETE"
}

trait RunsQueries[T] {
  protected def createQueryString(logger: Logger, primaryClause: String, actionClause: String, labels: List[NodeLabel], pList: List[(String, AnyRef)]): String = {
    val labelPart = if (labels.isEmpty) "" else labels.map(_.getLabelString).reduce(_ + _)
    logger.info("[CreateNodeRequest.runRequest] labelPart: {}", labelPart)

    val propertyPart = if (pList.isEmpty) "" else "{" + pList.map(t => t._1 + ": $" + t._1).reduce(_ + ", " + _) + "}"
    logger.info("[CreateNodeRequest.runRequest] propertyPart: {}", propertyPart)

    val queryString = primaryClause + " (n" + labelPart + " " + propertyPart + ")\n" +
      actionClause + " n"
    queryString
  }

  protected def runSimpleQuery(logger: Logger, tx: Transaction, queryString: String, props: List[(String, AnyRef)] = Nil): util.List[Record] = {
    val result: Result = runBaseQuery(logger, tx, queryString, props)
    logger.warn("[ExecutableCypherRequest.runSimpleQuery] result: {}", result)

    val resultData: util.List[Record] = result.list()
    logger.info("[ExecutableCypherRequest.runSimpleQuery] resultData: {}", resultData)
    resultData
  }

  protected def runBaseQuery(logger: Logger, tx: Transaction, queryString: String, props: List[(String, AnyRef)]): Result = {
    logger.info("[ExecutableCypherRequest.runBaseQuery] enter, running: {}", queryString)

    try {
      val parameters = new util.HashMap[String, AnyRef]()
      props.foreach(t => parameters.put(t._1, t._2))

      val result = tx.run(queryString, parameters)
      logger.info("[ExecutableCypherRequest.runBaseQuery] result: {}", result)
      result
    } catch {
      case e: Exception =>
        logger.error("[ExecutableCypherRequest.runBaseQuery] ", e)
        null
    }
  }

  protected def doOneRecord(internal: ConnectorInternal, logger: Logger)(r: Record): OpResult[NeoNode] = {
    logger.info("[MatchLabel.doOneRecord] next: {}", r)
    val n = r.get("n").asNode()
    NeoNode(internal, logger, n)
  }

  /**
   * Performs operations using information from a Neo4J SummaryCounters object
   */
  abstract class CounterHandler(name: String) {
    def getName: String = name

    /**
      * Gets the value of the counter handled by this class
      * @param counters - the result of the query
      * @return The value of count for this class
      */
    def getValue(counters: SummaryCounters): Int

    /**
     * For a non-zero counter, renders a text message and adds it to the list of
     * messages for the given future
      *
      * @param counters SummaryCounters object with counter information
     * @param future OpResult to which results will be reported
     */
    def renderMessage(counters: SummaryCounters, future: OpResult[T]): Unit

    /**
     * Indicates if the value of this counter equals the expected value
     */
    def verifyCount(counters: SummaryCounters, expected: Int): Boolean

    protected def renderCounter(future: OpResult[T], value: Int, root: String): Unit = {
      if (value > 0) {
        val message = root + value
        logger.info("[ExecutableCypherRequest.renderCounter] adding to [{}] message: {}", java.lang.Integer.valueOf(future.hashCode()), message, "")
        future.addMessage(message)
      }
    }
  }

  object CreatedNodes extends CounterHandler("nodes created") {
    /**
      * Gets the value of the counter handled by this class
      *
      * @param counters - the result of the query
      * @return The value of count for this class
      */
    override def getValue(counters: SummaryCounters): Int = counters.nodesCreated()

    override def renderMessage(counters: SummaryCounters, future: OpResult[T]): Unit = {
      renderCounter(future, counters.nodesCreated(), "Nodes created: ")
    }

    override def verifyCount(counters: SummaryCounters, expected: Int): Boolean = counters.nodesCreated() == expected
  }

  object DeletedNodes extends CounterHandler("nodes deleted") {
    /**
      * Gets the value of the counter handled by this class
      *
      * @param counters - the result of the query
      * @return The value of count for this class
      */
    override def getValue(counters: SummaryCounters): Int = counters.nodesDeleted()

    override def renderMessage(counters: SummaryCounters, future: OpResult[T]): Unit = {
      renderCounter(future, counters.nodesDeleted(), "Nodes deleted: ")
    }
    override def verifyCount(counters: SummaryCounters, expected: Int): Boolean = counters.nodesDeleted() == expected
  }

  object CreatedRelationships extends CounterHandler("relationships created") {
    /**
      * Gets the value of the counter handled by this class
      *
      * @param counters - the result of the query
      * @return The value of count for this class
      */
    override def getValue(counters: SummaryCounters): Int = counters.relationshipsCreated()

    override def renderMessage(counters: SummaryCounters, future: OpResult[T]): Unit = {
      renderCounter(future, counters.relationshipsCreated(), "Relationships created: ")
    }
    override def verifyCount(counters: SummaryCounters, expected: Int): Boolean = counters.relationshipsCreated() == expected
  }

  object DeletedRelationships extends CounterHandler("relationships deleted") {
    /**
      * Gets the value of the counter handled by this class
      *
      * @param counters - the result of the query
      * @return The value of count for this class
      */
    override def getValue(counters: SummaryCounters): Int = counters.relationshipsDeleted()

    override def renderMessage(counters: SummaryCounters, future: OpResult[T]): Unit = {
      renderCounter(future, counters.relationshipsDeleted(), "Relationships deleted: ")
    }
    override def verifyCount(counters: SummaryCounters, expected: Int): Boolean = counters.relationshipsDeleted() == expected
  }

  object AddedConstraints extends CounterHandler("constraints added") {
    /**
      * Gets the value of the counter handled by this class
      *
      * @param counters - the result of the query
      * @return The value of count for this class
      */
    override def getValue(counters: SummaryCounters): Int = counters.constraintsAdded()

    override def renderMessage(counters: SummaryCounters, future: OpResult[T]): Unit = {
      renderCounter(future, counters.constraintsAdded(), "Constraints added: ")
    }
    override def verifyCount(counters: SummaryCounters, expected: Int): Boolean = counters.constraintsAdded() == expected
  }

  object RemovedConstraints extends CounterHandler("constraints removed") {
    /**
      * Gets the value of the counter handled by this class
      *
      * @param counters - the result of the query
      * @return The value of count for this class
      */
    override def getValue(counters: SummaryCounters): Int = counters.constraintsRemoved()

    override def renderMessage(counters: SummaryCounters, future: OpResult[T]): Unit = {
      renderCounter(future, counters.constraintsRemoved(), "Constraints removed: ")
    }
    override def verifyCount(counters: SummaryCounters, expected: Int): Boolean = counters.constraintsRemoved() == expected
  }

  object AddedIndexes extends CounterHandler("indexes added") {
    /**
      * Gets the value of the counter handled by this class
      *
      * @param counters - the result of the query
      * @return The value of count for this class
      */
    override def getValue(counters: SummaryCounters): Int = counters.indexesAdded()

    override def renderMessage(counters: SummaryCounters, future: OpResult[T]): Unit = {
      renderCounter(future, counters.indexesAdded(), "Indexes added: ")
    }
    override def verifyCount(counters: SummaryCounters, expected: Int): Boolean = counters.indexesAdded() == expected
  }

  object IndexesDeleted extends CounterHandler("indexes removed") {
    /**
      * Gets the value of the counter handled by this class
      *
      * @param counters - the result of the query
      * @return The value of count for this class
      */
    override def getValue(counters: SummaryCounters): Int = counters.indexesRemoved()

    override def renderMessage(counters: SummaryCounters, future: OpResult[T]): Unit = {
      renderCounter(future, counters.indexesRemoved(), "Indexes removed: ")
    }
    override def verifyCount(counters: SummaryCounters, expected: Int): Boolean = counters.indexesRemoved() == expected
  }

  object PropertiesSet extends CounterHandler("properties set") {
    /**
      * Gets the value of the counter handled by this class
      *
      * @param counters - the result of the query
      * @return The value of count for this class
      */
    override def getValue(counters: SummaryCounters): Int = counters.propertiesSet()

    override def renderMessage(counters: SummaryCounters, future: OpResult[T]): Unit = {
      renderCounter(future, counters.propertiesSet(), "Properties set: ")
    }
    override def verifyCount(counters: SummaryCounters, expected: Int): Boolean = counters.propertiesSet() == expected
  }

  private val counterHandlers = Array[CounterHandler](
    CreatedNodes, DeletedNodes, CreatedRelationships, DeletedRelationships,
    AddedConstraints, RemovedConstraints, AddedIndexes, IndexesDeleted, PropertiesSet)

  protected def countersAsMessages(future: OpResult[T], result: Result): Unit = {
    logger.info("[ExecutableCypherRequest.countersAsMessages] rendering non-zero counters as messages.")
    val counters: SummaryCounters = result.consume().counters()
    counterHandlers.foreach(handler => handler.renderMessage(counters, future))
  }

  protected def counterExpectations(result: Result, zeros: List[CounterHandler], nonZeros: List[CounterHandler]): OpResult[Map[String, Int]] = {
    logger.info("[ExecutableCypherRequest.counterExpectations] checking against expectations.")
    val eitherSet = mutable.HashSet[CounterHandler]()
    eitherSet.addAll(counterHandlers)
    // we're going to use a mutable map to build the immutable result we return
    val builder = mutable.HashMap[String, Int]()

    val counters: SummaryCounters = result.consume().counters()
    zeros.foreach(next => {
      eitherSet.remove(next)
      if (! next.verifyCount(counters, 0)) return OpResult.failure("Unexpected non-zero in " + next.getName)
    })

    nonZeros.foreach(next => {
      eitherSet.remove(next)
      if (next.verifyCount(counters, 0)) return OpResult.failure("Unexpected zero in " + next.getName)
      builder.put(next.getName, next.getValue(counters))
    })

    eitherSet.foreach(next => builder.put(next.getName, next.getValue(counters)))
    OpResult.successWithDatum(builder.toMap)
  }

  protected def mirrorList(opposite: List[CounterHandler]): List[CounterHandler] = {
    val builder = mutable.HashSet[CounterHandler]().addAll(counterHandlers)
    opposite.foreach(next => builder.remove(next))
    builder.toList
  }
}

trait TransactionType {
  def runTransaction[T](internal: ConnectorInternal, session: Session, logger: Logger, request: ExecutableCypherRequest[T])
}

object ReadTransaction extends TransactionType {
  override def runTransaction[T](internal: ConnectorInternal, session: Session, logger: Logger, request: ExecutableCypherRequest[T]): Unit =
    session.readTransaction((tx: Transaction) => request.runRequest(internal, logger, tx))
}

object WriteTransaction extends TransactionType {
  override def runTransaction[T](internal: ConnectorInternal, session: Session, logger: Logger, request: ExecutableCypherRequest[T]): Unit =
    session.writeTransaction((tx: Transaction) => request.runRequest(internal, logger, tx))
}

trait ExecutableCypherRequest[T] extends ExecutableRequest[T] with ThreadTransactionMarker
  with NeedsClauseNames with RunsQueries[T] {

  def getType: TransactionType

  /**
   * Run the Cypher request and give the result to the future from the parent trait
   * @param logger for any logging the request wants to do
   * @param transaction a Neo4j transaction that can be used to perform the request
   */
  def runRequest(internal: ConnectorInternal, logger: Logger, transaction: Transaction): Unit

  def dumpCounters(logger: Logger, counter: SummaryCounters): String = {
    logger.info("[ExecutableCypherRequest.dumpCounters] constraintsAdded: {}", counter.constraintsAdded())
    logger.info("[ExecutableCypherRequest.dumpCounters] constraintsRemoved: {}", counter.constraintsRemoved())
    logger.info("[ExecutableCypherRequest.dumpCounters] containsSystemUpdates: {}", counter.containsSystemUpdates())
    logger.info("[ExecutableCypherRequest.dumpCounters] containsUpdates: {}", counter.containsUpdates())
    logger.info("[ExecutableCypherRequest.dumpCounters] indexesAdded: {}", counter.indexesAdded())
    logger.info("[ExecutableCypherRequest.dumpCounters] indexesRemoved: {}", counter.indexesRemoved())
    logger.info("[ExecutableCypherRequest.dumpCounters] nodesCreated: {}", counter.nodesCreated())
    logger.info("[ExecutableCypherRequest.dumpCounters] nodesDeleted: {}", counter.nodesDeleted())
    logger.info("[ExecutableCypherRequest.dumpCounters] propertiesSet: {}", counter.propertiesSet())
    logger.info("[ExecutableCypherRequest.dumpCounters] relationshipsCreated: {}", counter.relationshipsCreated())
    logger.info("[ExecutableCypherRequest.dumpCounters] relationshipsDeleted: {}", counter.relationshipsDeleted())
    logger.info("[ExecutableCypherRequest.dumpCounters] systemUpdates: {}", counter.systemUpdates())

    val sb = new StringBuilder()
    if (counter.containsUpdates()) {
      addCounter(sb, counter.nodesCreated(), "Nodes created: ")
      addCounter(sb, counter.nodesDeleted(), "Nodes deleted: ")
      addCounter(sb, counter.relationshipsCreated(), "Relationships created: ")
      addCounter(sb, counter.relationshipsDeleted(), "Relationships deleted: ")
      addCounter(sb, counter.constraintsAdded(), "Constraints created: ")
      addCounter(sb, counter.constraintsRemoved(), "Constraints deleted: ")
      addCounter(sb, counter.indexesAdded(), "Indexes created: ")
      addCounter(sb, counter.indexesRemoved(), "Indexes deleted: ")
      addCounter(sb, counter.propertiesSet(), "Properties set: ")
    }
    sb.toString()
  }

  private def addCounter(sb: StringBuilder, value: Int, label: String) = {
    if (value > 0) {
      if (sb.nonEmpty) sb.append(", ")
      sb.append(label).append(value)
    }
  }
}
