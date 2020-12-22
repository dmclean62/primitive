/*
 * Copyright (C) 2020, Donald McLean. All rights reserved.
 *
 * This program and the accompanying materials are licensed under
 * the terms of the GNU General Public License version 3.0
 * as published by the Free Software Foundation.
 */
package org.primitive

import org.slf4j.Logger

/** Mixin for logging "transaction" processing in actors. Adding a begin/end marker
  * makes it easier to identify any logging that takes place in the transaction,
  * since everything on that thread between the markers is guaranteed to be part
  * of the transaction.
  */
trait ThreadTransactionMarker {

  def beginTrans(logger: Logger, name: String = "event"): EventSignature = {
    val key = new EventSignature(name, scala.math.random)
    logger.info(key.beginString)
    key
  }

  def endTrans(logger: Logger, key: EventSignature): Unit = {
    logger.info(key.endString)
  }
}

class EventSignature(val name: String, val key: Double) {
  val beginTransMarker = ".begin<["
  val endTransMarker = ".end<["
  val markerTerm = "]>"

  //noinspection TypeAnnotation
  val time = System.currentTimeMillis()

  def beginString: String = name + beginTransMarker + key + markerTerm

  def endString: String = {
    val elapsed = System.currentTimeMillis() - time
    val msg = {
      if (elapsed > 100) " elapsed[*]: "
      else " elapsed: "
    }

    name + endTransMarker + key + markerTerm + msg + elapsed
  }
}
