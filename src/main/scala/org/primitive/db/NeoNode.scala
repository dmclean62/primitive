/*
 * Copyright (C) 2020, Donald McLean. All rights reserved.
 *
 * This program and the accompanying materials are licensed under
 * the terms of the GNU General Public License version 3.0
 * as published by the Free Software Foundation.
 */
package org.primitive.db

import org.neo4j.driver.Value
import org.neo4j.driver.internal.value.NullValue
import org.neo4j.driver.types.Node
import org.primitive.ml.OpResult
import org.slf4j.Logger

import scala.collection.mutable
import scala.jdk.CollectionConverters._

/**
 * A node object loaded from a Neo4j database. It is a temporary, in memory, version of what's
 * in the database
 * TODO: There will need to be a mechanism to ensure that the DB contents aren't modified out from under the object.
 */
class NeoNode(index: Long, labels: List[NodeLabel], properties: Map[String, PropertyValue]) {
  def getIndex: Long = index
  override def toString: String = "NeoNode(" + index + ", labels: " + labels + ", properties: " +
    properties + ")"
}

object NeoNode {
  /**
   * During the construction of a node with data from the DB, we're going to ignore any
   * labels or properties we don't recognize. They could be from other apps that use the
   * same database.
   * @param internal a data object that is used by objects working inside the actor context
   * @param logger for logging
   * @param n the raw data from the database
   * @return Will normally be a single result
   */
  def apply(internal: ConnectorInternal, logger: Logger, n: Node): OpResult[NeoNode] = {

    val index = n.get(NodeIndex.INDEX_PROPERTY).asLong()
    logger.info("[NeoNode.apply] index: {}", index)

    val rawLabels = n.labels().asScala
    val labels: List[NodeLabel] = rawLabels.flatMap(oneLabel(internal, logger)).toList
    logger.info("[NeoNode.apply] mapped labels: {}", labels)
    // TODO: with nodes that have a "type hierarchy" we need to normalize the labels

    val propertyTypes = propertyMap(labels)
    val getResult = mapProperties(logger, propertyTypes, n)
    getResult.singularOption match {
      case None => OpResult.failure(getResult)
      case Some(properties) => OpResult.successWithDatum(new NeoNode(index, labels, properties))
    }
  }

  private def mapProperties(logger: Logger, propertyTypes: Map[String, PropertyType], n: Node): OpResult[Map[String, PropertyValue]] = {
    val keys = propertyTypes.keys
    logger.info("[NeoNode.apply] keys: {}", keys)
    val properties = keys.flatMap(k => {
      val propertyType = propertyTypes(k) // since the keys came from propertyTypes, this is safe
      val value: Value = n.get(k) match {
        case n: NullValue =>
          if (propertyType.required) return OpResult.failure("Missing required value")
          else n
        case v: Value => v
        case _ => return OpResult.failure("Impossible case")
      }

      val result = oneProperty(logger, propertyType, value)
      logger.info("[NeoNode.mapProperties] got: {}", result)
      result
    }).toMap

    logger.info("[NeoNode.apply] properties: {}", properties)
    OpResult.successWithDatum(properties)
  }

  /**
   * Use internal to map labels from the database to application class labels
   *
   * @param internal data object use by tasks working inside the actor context
   * @param logger for logging
   * @param label a label to be mapped
   * @return optional application label class
   */
  def oneLabel(internal: ConnectorInternal, logger: Logger)(label: String): Option[NodeLabel] = {
    logger.info("[NeoNode.oneLabel] label: {}", label)
    internal.getLabel(label).singularOption
  }

  def propertyMap(labels: List[NodeLabel]): Map[String, PropertyType] = {
    val properties = new mutable.HashMap[String, PropertyType]()
    labels.foreach(p => p.properties.foreach(property => properties.put(property.name, property)))

    properties.toMap
  }

  def oneProperty(logger: Logger, propertyType: PropertyType, value: Value): Option[(String, PropertyValue)] = {
    logger.info("[NeoNode.oneProperty] processing: {}", propertyType)
    logger.info("[NeoNode.oneProperty] value is {}", value)

    value match {
      case _: NullValue => None
      case _ => Some((propertyType.name, propertyType.createProperty(value)))
    }
  }
}