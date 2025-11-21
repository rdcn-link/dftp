package link.rdcn.dacp.optree

import jep.SubInterpreter
import link.rdcn.operation.TransformOp
import link.rdcn.struct.DataFrame
import link.rdcn.user.{Credentials, UserPrincipal}
import link.rdcn.Logging

import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * @Author renhao
 * @Description:
 * @Data 2025/9/26 16:00
 * @Modified By:
 */
trait FlowExecutionContext extends link.rdcn.operation.ExecutionContext with Logging{

  private[this] val asyncResults = new ConcurrentHashMap[TransformOp, Future[DataFrame]]()
  private[this] val asyncResultsList = new ArrayBuffer[Thread]()

  def registerAsyncResult(transformOp: TransformOp, future: Future[DataFrame],
                          thread: Thread): Unit = {
    asyncResults.put(transformOp, future)
    asyncResultsList.append(thread)
    future onComplete { tryResult =>
      transformOp.asInstanceOf[TransformerNode].release()
      tryResult match {
        case Success(v) =>
        case Failure(exception) =>
          asyncResults.remove(transformOp)
          shutdownAsyncTasks()
          logger.error(exception)
      }
    }
  }

  def getAsyncResult(transformOp: TransformOp): Option[Future[DataFrame]] = {
    Option(asyncResults.get(transformOp))
  }

  def shutdownAsyncTasks(): Unit = asyncResultsList.foreach(_.stop)

  def fairdHome: String

  def pythonHome: String

  def isAsyncEnabled: Boolean = true

  def loadRemoteDataFrame(baseUrl: String, path:String, credentials: Credentials): Option[DataFrame]

  def getSubInterpreter(sitePackagePath: String, whlPath: String): Option[SubInterpreter] =
    Some(JepInterpreterManager.getJepInterpreter(sitePackagePath, whlPath, Some(pythonHome)))

  def getRepositoryClient(): Option[OperatorRepository]
}
