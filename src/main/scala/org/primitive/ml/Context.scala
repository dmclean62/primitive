/*
 * Copyright (C) 2020, Donald McLean. All rights reserved.
 *
 * This program and the accompanying materials are licensed under
 * the terms of the GNU General Public License version 3.0
 * as published by the Free Software Foundation.
 */
package org.primitive.ml

import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.xml.Node
import java.util.Date

import org.primitive.services.LoggerFactory

import scala.reflect.ClassTag

/**
  *  Provides a container for services that are being provided by various modules.
  *
  *  The default context can be had from the ModuleLoader object.
  *
  *  Services are accessed by the class of the trait that they implement. For example,
  *  logging services implement LoggerFactory so to get access the service, call
  *  getService[LoggerFactory]() to find the service that was defined in
  *  the ModuleLoader configuration file.
  */
//noinspection LoopVariableNotUpdated,DuplicatedCode
class Context(val name: String) {
  // default logger which we will hopefully replace
  private var logger: Logger = LoggerFactory.getLogger(getClass.getName)
  private var modules: mutable.HashMap[String, Module] = new mutable.HashMap[String, Module]
  private var shutdownList: List[Module] = Nil
  private var importList: List[Module] = Nil

  private val byObject: mutable.HashSet[AnyRef] = mutable.HashSet.empty
  private val bySource = new mutable.HashMap[String, mutable.HashSet[AnyRef]]

  private var myDependencies: List[Context] = _
  private var myDependents: List[Context] = _

  private var watchers: List[ContextWatcher] = List[ContextWatcher]()

  var loadCompleteCallbacks: List[()=>Unit] = _

  /**
    * Used in testing to verify that the correct number of modules have been loaded.
    */
  def getModuleCount: Int = {
    logger.debug("[Context.getModuleCount] modules: {}", modules)
    modules.size
  }

  /**
    * Use by the ModuleLoader to pass a module definition from the XML configuration file.
    */
  def load(moduleDefinition: Node): OpResult[Unit] = {
    var result: OpResult[Unit] = OpResult.simpleSuccess()

    val moduleName = (moduleDefinition \ "@name").toString()
    val className = (moduleDefinition \ "@class").toString()

    try {
      logger.debug("[Context.load] loading: " + className)
      val rawClass = Class.forName(className)
      val moduleClass = rawClass.asSubclass(classOf[Module])
      val constructor = moduleClass.getConstructor()
      val instance = constructor.newInstance()
      result = instance.start(this, moduleDefinition)
      logger.debug("[Context.load] got result: {}", result)

      if (result.satisfiedContinue) {
        Thread.sleep(50) // give module a chance to initialize a bit

        shutdownList ::= instance
        if (modules.contains(moduleName)) {
          return OpResult.failure("Duplicate module name.")
        }

        modules.put(moduleName, instance)
        doCallbacks()

        logger.debug("[Context.load] loaded {}", moduleName)
      }
    }
    catch {
      case e: Exception =>
        println("Failed to initialize module. Error:")
        e.printStackTrace()
        result = OpResult.failure(e)
    }

    result
  }

  /** An import is where a module created in one context is added (by context name and module name) to
    * another context. Not all modules support importing and the call to "addContext" may throw an exception.
    */
  def resolveImport(data: Node): OpResult[Unit] = {
    val contextName = getAttribute(data, "context")
    val name = getAttribute(data, "name")
    val module: Module = {
      val context = ModuleLoader.getContext(contextName) match {
        case None => return OpResult.failure("Referenced context" + contextName + " not found.")
        case Some(c) => c
      }
      context.modules.get(name) match {
        case None => return OpResult.failure("Referenced context " + contextName +
          " does not contain module " + name);
        case Some(m) => m
      }
    }

    module.addContext(this)
    importList ::= module
    modules.put(name, module)
    OpResult.simpleSuccess()
  }

  def getAttribute(data: Node, name: String): String = {
    data.attribute(name) match {
      case Some(v) => v.toString()
      case None => throw new IllegalArgumentException("Failed to specify attribute " + name + " in import.")
    }
  }

  /**
    * LoadComplete callbacks make it possible for module objects to access services that are added later in the manifest.
    *
    * @param callback - a function to be called when the regular module and context loading are complete.
    */
  def addLoadCompleteCallback(callback: () => Unit): Unit ={
    val list = List[() => Unit](callback)
    if (loadCompleteCallbacks == null) loadCompleteCallbacks = list
    else loadCompleteCallbacks = loadCompleteCallbacks ++ list
  }

  /**
    * Used by modules at application startup to announce services that they are providing.
    */
  def addService(service: Object): Unit ={
    logger.trace("[Context.addService] [{}]: adding {}", name, service.getClass.getName)
    byObject += service

    service match {
      case lf: org.primitive.services.LoggerFactory =>
        logger = lf.getLogger(getClass.getName)
      case _ =>
    }

    logger.trace("[Context.addService] services: {}", byObject)
  }

  /**
    * Used by modules at application startup to announce services that they are providing.
    */
  def addService(source: String, service: Object): Unit ={
    logger.trace("[Context.addService] [{}]: adding {}", name, service.getClass.getName)
    byObject += service
    bySource.get(source) match {
      case None =>
        val sourceSet = new mutable.HashSet[AnyRef]()
        sourceSet.add(service)
        bySource.put(source, sourceSet)
      case Some(sourceSet) => sourceSet.add(service)
    }

    logger.trace("[Context.addService] services: {}", byObject)
  }

  /**
    * Adds a context to the list of dependencies (things we depend on).
    */
  //noinspection DuplicatedCode
  def addDependency(fLContext: Context): Unit ={
    myDependencies match {
      case null => myDependencies = List[Context](fLContext)
      case _ => myDependencies = myDependencies :+ fLContext
    }
    fLContext.addDependent(this)
  }

  /**
    * Adds a context to the list of dependent contexts (contexts that depend on us)
    */
  def addDependent(fLContext: Context): Unit ={
    myDependents match {
      case null => myDependents = List[Context](fLContext)
      case _ => myDependents = myDependents :+ fLContext
    }
  }

  def hasDependents: Boolean = myDependents != null

  def removeDependency(context: Context): Unit ={
    myDependencies = myDependencies.filterNot((c: Context) => c == context)
    if (myDependencies.isEmpty) myDependencies = null

    context.removeDependent(this)
  }

  def removeDependent(context: Context): Unit ={
    myDependents = myDependents.filterNot((c: Context) => c == context)
    if (myDependents.isEmpty) myDependents = null
  }

  /**
    * Used by modules at application shutdown to terminate services.
    */
  def removeService(service: Object): Unit = {
    byObject -= service
  }

  /**
    * Used by modules at application shutdown to terminate services.
    */
  def removeService(source: String, service: Object): Any = {
    byObject -= service

    bySource.get(source) match {
      case None => //shouldn't happen
      case Some(sourceSet) =>
        sourceSet.remove(service)
        if (sourceSet.isEmpty) bySource.remove(source)
    }
  }

  def addWatcher(watcher: ContextWatcher): Unit ={
    watchers = watchers :+ watcher
  }

  def removeWatcher(watcher: ContextWatcher): Unit ={
    watchers = watchers.filter(_ != watcher)
  }

  def getWatchers: List[ContextWatcher] = watchers
  def watchCount: Int = watchers.size

  /**
    * Called by the ModuleLoader at application shutdown so that loaded modules can be stopped.
    */
  def shutdown(): Unit ={
    synchronized {
      logger.trace("Shutting down {}", name)
      if (modules == null) throw new RuntimeException("Attempt to shutdown context that was already shutdown.")
      if (hasDependents) throw new RuntimeException("Attempt to shutdown context with loaded dependent contexts.")

      releaseImports()
      shutdownModules()

      modules.clear()
      if (byObject.nonEmpty) {
        logger.trace("Improper cleanup: {} services still registered.", byObject.size)
        byObject.foreach(s => logger.trace(s.toString))
      }
      modules = null

      while (myDependencies != null) {
        val next = myDependencies.head
        removeDependency(next)
      }

      logger.trace("[Context.shutdown] exit: {}", name)
    }
  }

  private def shutdownModules(): Unit = {
    while (shutdownList.nonEmpty) {
      val next = shutdownList.head
      shutdownList = shutdownList.tail
      next.stop(this)
    }
  }

  private def releaseImports(): Unit = {
    while (importList.nonEmpty) {
      val next = importList.head
      importList = importList.tail
      next.removeContext(this)
    }
  }

  /** Search for a given service by the class/trait the service implements/extends.
    *
    * @return Option[A] 'Some' of the given type, or 'None' if no service object extending that type is registered.
    */
  def findService[A: ClassTag]: Option[A] = {
    val time = new Date()
    logger.trace("[Context.findService] {} ({}): {}", name, time.toString, byObject)
    byObject.collectFirst {
      case a: A => a
    }
  }

  /** Search for a given service by the class/trait the service implements/extends.
    *
    * @return Option[A] 'Some' of the given type, or 'None' if no service object extending that type is registered.
    */
  def findService[A: ClassTag](source: String): Option[A] = {
    val time = new Date()
    logger.trace("[Context.findService] {} ({}): {}", name, time.toString, byObject)
    bySource.get(source) match {
      case None => None
      case Some(s) => s.collectFirst {
        case a: A => a
      }
    }
  }

  def findRequiredService[A : ClassTag]: A =
    byObject.collectFirst {
      case a: A => a
    }.getOrElse(throw new IllegalStateException(s"Missing required service: ${implicitly[ClassTag[A]]}"))

  def findRequiredService[A : ClassTag](source: String): A = {
    val set = bySource.get(source) match {
      case None => throw new IllegalStateException(s"Missing required service: ${implicitly[ClassTag[A]]}")
      case Some(s) => s
    }
    set.collectFirst {
      case a: A => a
    }.getOrElse(throw new IllegalStateException(s"Missing required service: ${implicitly[ClassTag[A]]}"))
  }

  private def doCallbacks(): Unit ={
    if (loadCompleteCallbacks != null) loadCompleteCallbacks.foreach((f: () => Unit) => f())
    loadCompleteCallbacks = null
  }
}

trait ContextWatcher {
  def contextShuttingDown(context: Context): Unit
}
