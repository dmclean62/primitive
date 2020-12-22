/*
 * Copyright (C) 2020, Donald McLean. All rights reserved.
 *
 * This program and the accompanying materials are licensed under
 * the terms of the GNU General Public License version 3.0
 * as published by the Free Software Foundation.
 */
package org.primitive.db

import org.primitive.NeedsLogging
import org.primitive.ml.OpResult
import org.slf4j.Logger

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.xml.Node

object NodeLabel extends NeedsLogging {
  /*
  *       <type name="TravellerCampaign">
              <property name="name" type="String" required="true" unique="true"/>
              <property name="owner" type="String" required="true"/>
          </type>
*/
  def create(node: Node, nodeTypes: mutable.HashMap[String, NodeLabel]): OpResult[Nothing] = {
    val name = node.attribute("name") match {
      case None => return OpResult.failure("Name not defined for type")
      case Some(n) =>
        val nt = n.toString()
        nodeTypes.get(nt) match {
          case None => // we're good
          case Some(_) => return OpResult.failure("Duplicate type name: " + nt)
        }
        nt
    }
    logger.trace("[NodeType.create] name: {}", name)

    val t = new NodeLabel(logger)
    t.name = name

    val propBuffer = new ListBuffer[PropertyType]()
    node.child.foreach(c => {
      logger.trace("[NodeType.create] child: {}", c)
      if (c.label.equals("property")) {
        val createResult = PropertyType.create(logger, t, c)
        createResult.singularOption match {
          case None => return OpResult.failure(createResult)
          case Some(p) => propBuffer.addOne(p)
        }
      }
    })

    t.properties = propBuffer.toList
    nodeTypes.put(name, t)
    OpResult.simpleSuccess()
  }
}

class NodeLabel(logger: Logger) {
  var name: String = _
  var parentTypes: List[NodeLabel] = Nil
  var properties: List[PropertyType] = Nil

  override def toString: String = {
    val parentString = if (parentTypes.isEmpty) "" else ": " + parentTypes.toString()
    "NodeLabel(" + name  + parentString + ")"
  }

  def getLabelString: String = {
    logger.debug("[NodeType.getLabelString] name: {}, parentTypes: {}, properties: {}", name, parentTypes, properties)
    val suffix = if (parentTypes.nonEmpty) {
      parentTypes.map(" :" + _.name).reduce(_ + _)
    } else ""
    " :" + name + suffix
  }

  def validate(data: Map[String, AnyRef]): OpResult[Unit] = {
    if (properties.isEmpty) return OpResult.failure("No properties defined.")

    var result: OpResult[Unit] = OpResult.simpleSuccess()
    properties.foreach(p => {
      val t = p.validate(data)
      logger.debug("[NodeType.validate] validate: {}, got {}", p, t, "")
      result = result.merge(t)
    })
    result
  }
}