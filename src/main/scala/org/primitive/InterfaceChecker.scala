/*
 * Copyright (C) 2020, Donald McLean. All rights reserved.
 *
 * This program and the accompanying materials are licensed under
 * the terms of the GNU General Public License version 3.0
 * as published by the Free Software Foundation.
 */
package org.primitive

trait InterfaceChecker {
  def implementsInterface(target: Class[_], someInterface: Class[_]): Boolean = {
    val i: Array[Class[_]] = target.getInterfaces
    i.foreach((c: Class[_]) => if (c == someInterface) return true)

    val s = target.getSuperclass

    if (s == null) false
    else implementsInterface(s, someInterface)
  }
}
