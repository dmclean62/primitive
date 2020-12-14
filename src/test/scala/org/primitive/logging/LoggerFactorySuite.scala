/*
 * Copyright (C) 2020, Donald McLean. All rights reserved.
 *
 * This program and the accompanying materials are licensed under
 * the terms of the GNU General Public License version 3.0
 * as published by the Free Software Foundation.
 */
package org.primitive.logging

import java.io.{File, FileReader}

import ch.qos.logback.core.FileAppender
import org.primitive.NeedsLogging
import org.primitive.ml.{Context, ModuleLoader}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfter, Sequential}

class LoggerFactorySuite extends Sequential (
  new LFBasicCase,
  new LFHostOne,
  new LFHostTwo,
  new LFHostFallthrough,
  new LFTypeOne,
  new LFTypeTwo,
  new LFTypeFallthrough
)

//noinspection TypeAnnotation
object LoggerFactoryTest extends Matchers {
  var defaultContext: Context = _

  val LOG_MESSAGE = "[LFSetup.test] doing some logging."

  private var dropBox: TestableFileAppender[_] = _

  def drop(testObject: TestableFileAppender[_]): Unit = {
    if (dropBox != null) fail("Drop box already full")

    dropBox = testObject
  }

  def clear(): Unit = {
    if (dropBox == null) fail("Can't clear null.")

    dropBox = null
  }

  def get(): TestableFileAppender[_] = {
    if (dropBox == null) fail("Drop box is empty.")

    dropBox
  }
}

trait LoggerFactoryTest extends AnyFunSuite with Matchers with NeedsLogging with BeforeAndAfter {
  before {
    // ensure that anything done in a previous test is cleaned up
    ModuleLoader.shutdown()
    Thread.sleep(100)
  }

  after {
    ModuleLoader.shutdown()
    Thread.sleep(100)
  }

  def initialize(manifestName: String): Unit = {
    val url = getClass.getResource(manifestName)
    convertToAnyShouldWrapper(url) should not.be(null)
    ModuleLoader.initialize(url)
    LoggerFactoryTest.defaultContext = ModuleLoader.getDefaultContext
    initializeLogger(LoggerFactoryTest.defaultContext)
  }

  protected def waitForFileToWrite(f: File): Unit = {
    var count = 0
    while (f.length() < 500) {
      Thread.sleep(100)

      count += 1
      if (count > 100) fail("Output should have been written to file.")
    }
  }

  protected def verifyLogFileWrite(fileName: String): Unit = {
    logger.error(LoggerFactoryTest.LOG_MESSAGE)
    val f: File = new File(fileName)

    waitForFileToWrite(f)
    logger = null

    val reader = new FileReader(f)
    val buffer = new Array[Char](f.length().toInt)
    /* val read = */ reader.read(buffer)
    String.valueOf(buffer).contains(LoggerFactoryTest.LOG_MESSAGE) should be(true)
  }
}

class LFBasicCase extends LoggerFactoryTest {
  test("set up") {
    initialize("loggerFactoryTM01.xml")

    val appender = LoggerFactoryTest.get()
    val fileName = appender.getFileName
    // check that we're writing to the correct file name
    fileName.contains("LFS.001") should be(true)

    logger.error(LoggerFactoryTest.LOG_MESSAGE)
    val f: File = new File(fileName)

    waitForFileToWrite(f)
    logger = null

    val reader = new FileReader(f)
    val buffer = new Array[Char](f.length().toInt)
    /* val read = */ reader.read(buffer)
    String.valueOf(buffer).contains(LoggerFactoryTest.LOG_MESSAGE) should be(true)
  }
}

class LFHostOne extends LoggerFactoryTest {
  test("case one") {
    System.setProperty("simulate.hostName", "one")

    initialize("loggerFactoryTM02.xml")

    val appender = LoggerFactoryTest.get()
    val fileName = appender.getFileName
    // check that we're writing to the correct file name
    fileName.contains("LFS.002.one") should be(true)

    verifyLogFileWrite(fileName)
  }
}

class LFHostTwo extends LoggerFactoryTest {
  test("case two") {
    System.setProperty("simulate.hostName", "two")

    initialize("loggerFactoryTM02.xml")

    val appender = LoggerFactoryTest.get()
    val fileName = appender.getFileName
    // check that we're writing to the correct file name
    fileName.contains("LFS.002.two") should be(true)

    verifyLogFileWrite(fileName)
  }
}

class LFHostFallthrough extends LoggerFactoryTest {
  test("case fallthrough") {
    System.setProperty("simulate.hostName", "fallthrough")

    initialize("loggerFactoryTM02.xml")

    val appender = LoggerFactoryTest.get()
    val fileName = appender.getFileName
    // check that we're writing to the correct file name
    fileName.contains("LFS.002.fallthrough") should be(true)

    verifyLogFileWrite(fileName)
  }
}

class LFTypeOne extends LoggerFactoryTest {
  test("case one") {
    System.setProperty("logType", "one")

    initialize("loggerFactoryTM03.xml")

    val appender = LoggerFactoryTest.get()
    val fileName = appender.getFileName
    // check that we're writing to the correct file name
    fileName.contains("LFS.003.one") should be(true)

    verifyLogFileWrite(fileName)
  }
}

class LFTypeTwo extends LoggerFactoryTest {
  test("case two") {
    System.setProperty("logType", "two")

    initialize("loggerFactoryTM03.xml")

    val appender = LoggerFactoryTest.get()
    val fileName = appender.getFileName
    // check that we're writing to the correct file name
    fileName.contains("LFS.003.two") should be(true)

    verifyLogFileWrite(fileName)
  }
}

class LFTypeFallthrough extends LoggerFactoryTest {
  test("case fallthrough") {
    System.clearProperty("logType")

    initialize("loggerFactoryTM03.xml")

    val appender = LoggerFactoryTest.get()
    val fileName = appender.getFileName
    // check that we're writing to the correct file name
    fileName.contains("LFS.003.fallthrough") should be(true)

    verifyLogFileWrite(fileName)
  }
}

class TestableFileAppender[E] extends FileAppender[E] {
  LoggerFactoryTest.drop(this)

  def getFileName: String = fileName

  override def stop(): Unit = {
    super.stop()

    LoggerFactoryTest.clear()
  }
}