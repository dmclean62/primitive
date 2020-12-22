/*
 * Copyright (C) 2020, Donald McLean. All rights reserved.
 *
 * This program and the accompanying materials are licensed under
 * the terms of the GNU General Public License version 3.0
 * as published by the Free Software Foundation.
 */
package org.primitive.message

import org.primitive.ml.Context
import org.slf4j.Logger

import scala.collection.mutable
import scala.xml.Node

/**
  * A generic message class with a name and named properties.
  */
class Message(val name: String, properties: Map[String, Any]) {
  var context: Context = _
  var state: MessageState = MSConstructed

  /**
    * Query an message to see if it is in a particular state
    * @param s - the state of interest
    * @return true if the message is in that state
    */
  def is(s: MessageState): Boolean = state == s

  def update(s: MessageState): Unit = {
    if (state != MSComplete) state = s
  }

  /**
    * Retrieves the value of a named property
    *
    * @param name - the key under which the property is stored
    * @return Object - the value attached to the given name
    */
  def getProperty(name: String): Option[Any] = {
    properties.get(name)
  }

  /**
    * Check to see if an message contains a property
    * @param name - the name of the property of interest
    * @return Boolean - true if the property is defined for this message
    */
  def hasProperty(name: String): Boolean = properties.contains(name)

  /**
    * Retrieves the value of a named property, but only if it is a String
    *
    * @param name - the key under which the property is stored
    * @return String - the String value attached to the given name
    */
  def getStringProperty(name: String): String = {
    properties.get(name) match {
      case None => null
      case Some(s: String) => s
      case _ => null
    }
  }

  /** Copies the properties of this message into another map.
    *
    * @param data - the destination map for the contents of properties.
    */
  def copyProperties(data: mutable.HashMap[String, Any]): Unit = {
    data ++= properties
  }

  def dump(logger: Logger): Unit = {
    logger.debug("[Message.dump] message properties for '{}' in context: {}", name, context, "")
    properties.foreach {
      case (k: String, null) => logger.debug("[Message.dump] {} is null.", k)
      case (k: String, v: Any) => logger.debug("[Message.dump] {}: {}", k, v)
    }
  }
}

package object messages{
  type MessageCondition = Message => Boolean
}

case class MessageState(name: String)

object MSConstructed extends MessageState("constructed")
object MSPending extends MessageState("pending")
object MSComplete extends MessageState("complete")

class MessageBuilder(name: String) {
  val properties = new mutable.HashMap[String, Any]()

  def build(): Message = {
    new Message(name, properties.toMap)
  }

  /**
    * Sets the value of a property
    *
    * @param name - the key under which the property is stored.
    * @param value - the value object attached to a property
    */
  def setProperty(name: String, value: Any): Unit = { properties.put(name, value) }

  /** Adds a collection of properties to this message.
    *
    * @param data - a map containing properties to be added
    */
  def addProperties[A <: Any](data: mutable.HashMap[String, A]): Unit = { properties ++= data }
}

object XMLMessageConstructor {
  def createOneMessage(data: Node): Message = {
    val messageName = data.attribute("name").head.toString()
    val result = new MessageBuilder(messageName)

    data.child.foreach((n: Node) => {
      if (n.label == "property") {
        val name = n.attribute("name").head.toString()
        val value = n.attribute("value").head.toString()
        result.setProperty(name, value)
      }
    })

    result.build()
  }
}