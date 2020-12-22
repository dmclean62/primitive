/*
 * Copyright (C) 2020, Donald McLean. All rights reserved.
 *
 * This program and the accompanying materials are licensed under
 * the terms of the GNU General Public License version 3.0
 * as published by the Free Software Foundation.
 */
package org.primitive.db

import org.neo4j.driver.Value
import org.primitive.ml.OpResult
import org.slf4j.Logger

import scala.collection.mutable
import scala.xml.Node

/**
 * Describes the specific type of a node property of a specific NodeLabel, so that
 * the program knows how to manage instances of that property in instances of nodes
 * with that label.
 */
class PropertyType {
  var logger: Logger = _
  var parent: NodeLabel = _
  var name: String = _
  var required: Boolean = _
  var unique: Boolean = _
  var definition: PropertyTypeDefinition = _

  def this(logger: Logger, parent: NodeLabel, name: String, definition: PropertyTypeDefinition, required: Boolean, unique: Boolean) {
    this()

    this.logger = logger
    this.parent = parent
    this.name = name
    this.definition = definition
    this.required = required
    this.unique = unique
  }

  override def toString: String = "PropertyType(" + name + ": " + definition + ")"

  def createProperty(value: Value): PropertyValue = definition.createProperty(this, value)

  def validate(data: Map[String, AnyRef]): OpResult[Unit] = {
    var value: AnyRef = null
    if (required) {
      data.get(name) match {
        case None => return OpResult.failure("Missing required value: " + name)
        case Some(v) => value = v
      }
    }

    if (unique) {
      //TODO: implement this
    }

    definition.validate(value)
  }
}

object PropertyType {
  private val directory = mutable.HashMap[String, PropertyTypeDefinition](
    "String" -> StringDefinition
  )

  def create(logger: Logger, parent: NodeLabel, node: Node): OpResult[PropertyType] = {
    val name = node.attribute("name") match {
      case None =>
        logger.warn("[PropertyType.create] name not supplied.")
        return OpResult.failure("Name not supplied")
      case Some(n) => n.toString()
    }

    val propertyType = node.attribute("type") match {
      case None =>
        logger.warn("[PropertyType.create] type attribute not supplied.")
        return OpResult.failure("Type attribute not supplied")
      case Some(t) =>
        val typeName = t.toString()
        directory.get(typeName) match {
          case None =>
            logger.warn("[PropertyType.create] type attribute not found in catalog.")
            return OpResult.failure("Unknown typ attribute")
          case Some(ptd) => ptd
        }
    }

    val required = parseOptionalBoolean(node, "required")
    val unique = parseOptionalBoolean(node, "unique")

    if (unique && !required) {
      logger.warn("[PropertyType.create] unique properties must be required.")
      return OpResult.failure("Unique properties must be require")
    }

    OpResult.successWithDatum(new PropertyType(logger, parent, name, propertyType, required, unique))
  }

  private def parseOptionalBoolean(node: Node, attributeName: String) = {
    node.attribute(attributeName) match {
      case None => false
      case Some(r) => r.toString() == "true"
    }
  }
}

trait PropertyTypeDefinition {
  def createProperty(propertyType: PropertyType, value: Value): PropertyValue

  def validate(value: AnyRef): OpResult[Unit]
}

object StringDefinition extends PropertyTypeDefinition {
  override def toString: String = "String property"

  override def createProperty(propertyType: PropertyType, value: Value): PropertyValue = new StringValue(propertyType, value.asString())

  override def validate(value: AnyRef): OpResult[Unit] = OpResult.simpleSuccess() // anything can be converted to string
}

abstract class PropertyValue(myType: PropertyType) {

}

class StringValue(myType: PropertyType, value: String) extends PropertyValue(myType) {
  override def toString: String = "StringValue(" + myType + ", '" + value + "')"
}