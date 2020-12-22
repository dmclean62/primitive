/*
 * Copyright (C) 2020, Donald McLean. All rights reserved.
 *
 * This program and the accompanying materials are licensed under
 * the terms of the GNU General Public License version 3.0
 * as published by the Free Software Foundation.
 */
package org.primitive.services

import net.liftweb.actor.LiftActor
import org.primitive.message.Message
import org.primitive.message.messages.MessageCondition

/**
  * This is the interface definition for classes that provide application message services.
  */
trait MessageDispatchService {
  def register(name: String, callback: Message => Unit): MessageRegistration

  def registerConditionally(name: String, callback: Message => Unit, condition: (String, Any)): MessageRegistration
  def registerConditionally(name: String, callback: Message => Unit, condition: MessageCondition): MessageRegistration

  def registerActor(name: String, actor: LiftActor): MessageRegistration

  def post(message: Message): Unit
  def delayedPost(message: Message, delay: Int): Unit
}

trait MessageRegistration {
  private var status: MessageRegistrationStatus = MRSPending

  def deregister(): Unit

  def is (s: MessageRegistrationStatus): Boolean = s == status

  def update(s: MessageRegistrationStatus): Unit = {status = s}

  def isAcceptingMessages: Boolean = status == MRSActive
}

case class MessageRegistrationStatus(name: String)

object MRSPending extends MessageRegistrationStatus("pending")
object MRSActive extends MessageRegistrationStatus("active")
object MRSPaused extends MessageRegistrationStatus("paused")
object MRSTerminating extends MessageRegistrationStatus("terminating")
object MRSTerminal extends MessageRegistrationStatus("terminal")
