/*
 * Copyright (C) 2020, Donald McLean. All rights reserved.
 *
 * This program and the accompanying materials are licensed under
 * the terms of the GNU General Public License version 3.0
 * as published by the Free Software Foundation.
 */
package org.primitive.ml

import java.io.{File, FileReader}
import java.net.URL

import org.primitive.NeedsLogging
import org.primitive.message.{Message, MessageBuilder}
import org.primitive.services.{MessageDispatchService, MessageRegistration}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.xml.{Elem, Node, XML}

/**
  * Two forms of the initialize method are provided - one taking a URL to the XML file and one taking the name
  * of the XML file as a resource (relative to this class).
  *
  * The XML file should have a root tag of "manifest". Inside the manifest block there should be a "module"
  * tag for each module with "name" and "class" attributes. Inside each module block, include module specific
  * initialization data.
  *
  * Note: initialize may be called more than once to load multiple configuration files.
  */
//noinspection DuplicatedCode
object ModuleLoader {
  trait ModuleLoaderState

  object MLSNotInitialized extends ModuleLoaderState
  object MLSInitializing extends ModuleLoaderState
  object MLSRunning extends ModuleLoaderState
  object MLSShuttingDown extends ModuleLoaderState
  object MLSShutDown extends ModuleLoaderState

  private var state: ModuleLoaderState = MLSNotInitialized

  private var defaultContext: Context = _
  private var contextDirectory: mutable.HashMap[String, Context] = _

  private var manifest: Elem = _
  private var shutdownList: List[Context] = Nil

  private var loadCompleteCallbacks: List[()=>Unit] = Nil
  private var preShutdownCallbacks: List[()=>Unit] = Nil

  private var contextUnloadPlans: List[ContextShutdownPlan] = Nil

  private var eventDispatch: MessageDispatchService = _

  private var logger: Logger = _

  val DEFAULT = "main"

  val CONTEXT_UNLOAD_PENDING = "contextUnloadPending"
  val TRY_UNLOAD = "tryUnload"

  val CONTEXT_LOADED = "contextLoaded"
  val CONTEXT_UNLOADED = "contextUnloaded"

  /**
    * Loads a manifest file, creating the module objects and starting them so that they can set up and
    * register their services.
    *
    * @param resourceName - name of a class loader accessible resource containing an XML "manifest",
    * relative to the path of the ModuleLoader class
    */
  def initialize(resourceName: String): OpResult[Unit] = {
    state = MLSInitializing
    logger = LoggerFactory.getLogger(getClass.getName)

    createMainContext()

    loadManifest(resourceName)
    val result = doMainInitialize()

    state = MLSRunning
    result
  }

  private def doMainInitialize(): OpResult[Unit] = {
    if (manifest != null) {
      val result = readManifest()
      doCallbacks()
      eventDispatch = defaultContext.findRequiredService[MessageDispatchService]
      result
    } else {
      OpResult.failure("Failed to load manifest")
    }
  }

  /**
    * Loads a manifest file, creating the context and module objects and starting them so that they can set up and
    * register their services.
    *
    * @param resource - URL to an XML "manifest"
    */
  def initialize(resource: URL): OpResult[Unit] = {
    state = MLSInitializing
    logger = LoggerFactory.getLogger(getClass.getName)

    createMainContext()

    loadManifest(resource)
    val result = doMainInitialize()

    state = MLSRunning
    result
  }

  /**
    * Normally called at program termination to stop all the contexts and modules,
    * allowing them to clean up. Can also be used at the end of a unit test to clean
    * up test setups.
    */
  def shutdown(): Unit = {
    if (state != MLSRunning) return // there is only one state from which we can shut down

    state = MLSShuttingDown

    logger.info("[ModuleLoader.shutdown] enter.")
    doPreShutdownCallbacks()

    logger.info("[ModuleLoader.shutdown] shutting down services.")
    shutdownList.foreach(_.shutdown())
    shutdownList = Nil

    if (contextDirectory != null) {
      contextDirectory.clear()
      contextDirectory = null
    }

    manifest = null
    defaultContext = null
    logger = null

    state = MLSShutDown
  }

  /**
    * Accessor for the default context.
    *
    * @return EFLContext - the default context
    */
  def getDefaultContext: Context = defaultContext

  /**
    * Access for getting a context by name
    *
    * @param name - the name of a context that we would like to access
    * @return context with that name (if found)
    */
  def getContext(name: String): Option[Context] = contextDirectory.get(name)

  /**
    * Accessor for a list of all contexts.
    * @return List[EFLContext] containing all currently loaded contexts.
    */
  def getContexts: List[Context] = contextDirectory.values.toList

  /**
    * Allows an external agent to load a context by supplying an XML definition.
    * @param data XML definition of a context to be loaded as a child of the main context.
    */
  def loadContext(data: Node): Unit = {
    readContext(defaultContext, data)
  }

  /**
    * Unloading a context on-demand is fraught with peril, so unloading is "scheduled" for some future time. We
    * precede the actual unloading with an "unload pending" event, and hopefully anyone dependent upon the
    * context will have cleaned up the dependency before we actually unload it.
    */
  def scheduleUnloadContext(fLContext: Context, preferredTime: Int = 10000): Unit = {
    if (fLContext.hasDependents) throw new RuntimeException("Can't unload a module with dependencies.")

    val plan = new ContextShutdownPlan(fLContext, preferredTime)
    contextUnloadPlans +:= plan

    val eventDispatch = fLContext.findRequiredService[MessageDispatchService]
    val builder = new MessageBuilder(CONTEXT_UNLOAD_PENDING)
    builder.setProperty("plan", plan)
    eventDispatch.post(builder.build())
  }

//  /**
//    * Attempt to unload the context specified by some ContextShutdownPlan.
//    * @param plan An object specifying the context to be unloaded.
//    * @return true if the context was successfully unloaded.
//    */
//  def tryUnload(plan: ContextShutdownPlan): Boolean = {
//    log("[ModuleLoader.tryUnload] enter.")
//
//    try {
//      if (!contextUnloadPlans.contains(plan)) throw new RuntimeException("Attempt to try an unsanctioned unload.")
//
//      plan.context.shutdown()
//
//      shutdownList = shutdownList.filter((c: Context) => c != plan.context)
//
//      contextDirectory.remove(plan.context.name)
//      contextUnloadPlans = contextUnloadPlans.filterNot(_ == plan)
//      propagateContextEvent(plan.context, unloadEN)
//    }
//    catch {
//      case t: Throwable =>
//        t.printStackTrace()
//        return false
//    }
//
//    log("[ModuleLoader.tryUnload] exit.")
//    true
//  }

  /**
    * LoadComplete callbacks make it possible for module objects to access services that are added later in the manifest.
    *
    * @param callback - a function to be called when the regular module and context loading are complete.
    */
  def addLoadCompleteCallback(callback: () => Unit): Unit = {
    loadCompleteCallbacks +:= callback
  }

  /**
    * PreShutdown callbacks make it possible for module object to disengage from services that are cleaned up earlier
    * in the cleanup stack.
    *
    * @param callback - a function to be called just before beginning the regular module and context shutdown process.
    */
  def addPreShutdownCallback(callback: () => Unit): Unit = {
    preShutdownCallbacks +:= callback
  }

  private def createMainContext(): Unit = {
    if (defaultContext == null) {
      logger.info("[ModuleLoader.createMainContext] creating a main/default context.")
      defaultContext = new Context(DEFAULT)
      contextDirectory = mutable.HashMap[String, Context](DEFAULT -> defaultContext)
    }
  }

  private def loadManifest(resourceName: String): Unit = {
    logger.info("[ModuleLoader.loadManifest] attempt to load: {}", resourceName)
    val resourceURL = getClass.getResource(resourceName)
    if (resourceURL == null) {
      logger.error("[ModuleLoader.initialize] invalid resource name: {}", resourceName)
      return
    }

    loadManifest(resourceURL)
  }

  private def loadManifest(resourceURL: URL): Unit = {
    val stream = resourceURL.openStream
    manifest = XML.load(stream)
    logger.info("[ModuleLoader.loadManifest] manifest contents:\n{}",manifest.toString)
  }

  private def readManifest(): OpResult[Unit] = {
    logger.trace("[ModuleLoader.readManifest] length: {}", manifest.child.length)

    if (shutdownList.isEmpty) shutdownList +:= defaultContext

    var result: OpResult[Unit] = OpResult.simpleSuccess()
    manifest.child.foreach((n: Node) => {
      val next = readNode(defaultContext, n)
      result = result merge next
    })

    result
  }

  private def readNode(root: Context, n: Node): OpResult[Unit] = {
    logger.info("[ModuleLoader.readNode] context is: {}, node is: {}", root, n.label)

    n.label match {
      case "reference" => readFile(root, n)
      case "module" => readModule(root, n)
      case "context" => readContext(root, n)
      case "import" => root.resolveImport(n)
      case _ =>
        logger.info("[ModuleLoader.readNode] doing nothing for {}" + n.label)
        OpResult.simpleSuccess()
    }
  }

  private def readFile(context: Context, data: Node): OpResult[Unit] = {
    logger.info("[ModuleLoader.readFile] enter: {}", data)

    var result: OpResult[Unit] = OpResult.simpleSuccess()

    try {
      data.attribute("path").foreach(n => {
        val source = new File(n.toString())
        logger.info("[ModuleLoader.readFile] path is: {}", source.getAbsolutePath)

        if (source.canRead) {
          val fileReader = new FileReader(source)
          val refData = XML.load(fileReader)
          logger.info("[ModuleLoader.readFile] got: {}", refData)
          result = readNode(context, refData)
        }
        else {
          logger.warn("[ModuleLoader.readFile] can't read referenced source file, source exists: {}", source.exists())
          result = OpResult.failure("Unable to read referenced initialization file.")
        }
      })
    } catch {
      case e: Exception =>
        logger.error("[ModuleLoader.readFile] got exception.", e)
        result = OpResult.failure(e)
    }

    result
  }

  private def readModule(context: Context, data: Node): OpResult[Unit] = {
    logger.info("[ModuleLoader.readModule] enter.")

    context.load(data)
  }

  //noinspection ScalaUnusedSymbol
  private def readContext(root: Context, data: Node): OpResult[Unit] = {
    logger.info("[ModuleLoader.readContext] enter.")

    val name = data.attribute("name").head.toString()
    logger.info("[ModuleLoader.readContext] name is: {}", name)
    val result = new Context(name)

    if (contextDirectory.contains(name)) throw new IllegalArgumentException("Duplicate context name: " + name)
    contextDirectory.put(name, result)
    propagateContextEvent(result, CONTEXT_LOADED)
    shutdownList +:= result

    logger.info("[ModuleLoader.readContext] created context: {}", result.name)

    var returnValue: OpResult[Unit] = OpResult.simpleSuccess()
    data.child.foreach((n: Node) => {
      val next = readNode(result, n)
      returnValue = returnValue merge next
    })

    returnValue
  }

  private def doCallbacks(): Unit = {
    loadCompleteCallbacks.foreach((f: () => Unit) => f())
    loadCompleteCallbacks = Nil
  }

  private def doPreShutdownCallbacks(): Unit = {
    preShutdownCallbacks.foreach((f: () => Unit) => f())
    preShutdownCallbacks = Nil
  }

  private def propagateContextEvent(context: Context, eventName: String): Unit = {
    if (eventDispatch == null){
      logger.error("[ModuleLoader.propagateContextEvent] can't propagate event.")
      return
    } // still initializing or already shutting down

    val builder = new MessageBuilder(eventName)
    builder.setProperty("context", context)
    eventDispatch.post(builder.build())

    logger.info("[ModuleLoader.propagateContextEvent] exit.")
  }

  private class ContextShutdownPlan(val context: Context, preferredTime: Int) extends NeedsLogging {
    val unloadTime: Long = System.currentTimeMillis() + preferredTime
    initializeLogger(context)

    val eventDispatch: MessageDispatchService = context.findService[MessageDispatchService].get

    var unloadER: MessageRegistration = eventDispatch.register(TRY_UNLOAD, tryUnload)
    val builder = new MessageBuilder(TRY_UNLOAD)
    eventDispatch.delayedPost(builder.build(), preferredTime)

    def tryUnload(event: Message): Unit = {
      logger.debug("[ContextShutdownPlan.tryUnload] enter.")

      val now = System.currentTimeMillis()
      if (unloadTime > now) {
        logger.trace("[ContextShutdownPlan.tryUnload] waiting...")
        val difference = (unloadTime - now + 10).toInt
        eventDispatch.delayedPost(event, difference)
        return
      }

      // since the unload will shutdown event handling (if successful), deregister but be prepared to re-register
      unloadER.deregister()
      unloadER = null

//      val t = new UnloadThread
//      t.start()
    }

//    class UnloadThread extends Thread with ThreadTransactionMarker {
//      override def run(): Unit = {
//        val key = beginTrans(logger, "unloadThread")
//
//        Thread.sleep(50)
//        if (! ModuleLoader.tryUnload(ContextShutdownPlan.this)) unloadER = eventDispatch.register(TRY_UNLOAD, tryUnload)
//
//        endTrans(logger, key)
//      }
//    }
  }
}

///**
//  * Thrown by the ModuleLoader when there is an invalid entry in the XML configuration file.
//  */
//class PInitializationFailure(xmlBlock: String, exception: Exception) extends Exception(exception)
//
///**
//  * Thrown when a module is unable to complete its initialization
//  * @param errors List of problems the module ran into
//  */
//class PInitializationException(errors: Array[Exception]) extends Exception("Initialization produced a list of failures")