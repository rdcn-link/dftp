package link.rdcn.operation

import jep._
import link.rdcn.Logging

/**
 * @Author renhao
 * @Description:
 * @Data 2025/7/21 18:30
 * @Modified By:
 */

object JepInterpreterManager extends Logging {
  // ThreadLocal stores each thread's SharedInterpreter instance
  private val threadLocalInterpreter: ThreadLocal[SharedInterpreter] = new ThreadLocal[SharedInterpreter] {
    override def initialValue(): SharedInterpreter = {
      logger.debug(s"Initializing SharedInterpreter for thread: ${Thread.currentThread().getName}")
      new SharedInterpreter()
    }
  }

  // Get the SharedInterpreter instance for the current thread
  def getInterpreter: SharedInterpreter = threadLocalInterpreter.get()

  // Close the SharedInterpreter instance for the current thread.
  // IMPORTANT: This should be called when the thread finishes its lifecycle or no longer needs Jep.
  def closeInterpreterForCurrentThread(): Unit = {
    val interp = threadLocalInterpreter.get()
    if (interp != null) {
      try {
        // --- REMOVED THE isClosed CHECK HERE ---
        interp.close()
        logger.debug(s"SharedInterpreter for thread ${Thread.currentThread().getName} has been closed.")
      } catch {
        case e: JepException if e.getMessage.contains("already closed") =>
          // Catch and log if it's already closed. This might happen if cleanup logic is called multiple times.
          logger.warn(s"Warning: Interpreter for thread ${Thread.currentThread().getName} already closed, ignoring. ${e.getMessage}")
        case e: Exception =>
          logger.error(s"Error closing interpreter for thread ${Thread.currentThread().getName}: ${e.getMessage}")
      } finally {
        threadLocalInterpreter.remove() // Remove from ThreadLocal to prevent memory leaks and ensure new interp on next get()
      }
    }
  }

  Runtime.getRuntime.addShutdownHook(new Thread(() => {
    logger.debug("JVM shutdown hook activated. Attempting to close current thread's Jep interpreter if active.")
    closeInterpreterForCurrentThread()
  }))
}