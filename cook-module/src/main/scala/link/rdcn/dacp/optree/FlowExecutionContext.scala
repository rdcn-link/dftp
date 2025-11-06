package link.rdcn.dacp.optree

import jep.SubInterpreter
import link.rdcn.operation.TransformOp
import link.rdcn.struct.DataFrame
import link.rdcn.user.{Credentials, UserPrincipal}

import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.JavaConverters._

/**
 * @Author renhao
 * @Description:
 * @Data 2025/9/26 16:00
 * @Modified By:
 */
trait FlowExecutionContext extends link.rdcn.operation.ExecutionContext {

  private[this] val asyncResults = new ConcurrentHashMap[TransformOp, Future[DataFrame]]()
  private[this] val asyncResultsList =
    new ConcurrentHashMap[TransformOp, ArrayBuffer[Thread]]()

  def registerAsyncResult(transformOp: TransformOp, future: Future[DataFrame],
                          thread: Thread): Unit = {
    asyncResults.put(transformOp, future)
    asyncResultsList.keys().asScala.foreach(key => {
      if(key.asInstanceOf[TransformerNode].contain(transformOp.asInstanceOf[TransformerNode])){
        asyncResultsList.get(key).append(thread)
        future.onComplete{
          case Failure(e) =>
            asyncResults.remove(transformOp)
            throw new Exception(s"TransformOp $transformOp failed", e)
        }
      }else {
        val arr = new ArrayBuffer[Thread]()
        arr.append(thread)
        asyncResultsList.put(transformOp, arr)
        future.onComplete {
          case Success(df) => transformOp.asInstanceOf[TransformerNode].release()
          case Failure(e) =>
            asyncResultsList.remove(transformOp)
            asyncResults.remove(transformOp)
            throw new Exception(s"TransformOp $transformOp failed", e)
        }
      }
    })
  }

  def getAsyncResult(transformOp: TransformOp): Option[Future[DataFrame]] = {
    Option(asyncResults.get(transformOp))
  }

  def getAsyncThreads(transformOp: TransformOp): Option[ArrayBuffer[Thread]] = {
    Option(asyncResultsList.get(transformOp))
  }

  def fairdHome: String

  def pythonHome: String

  def isAsyncEnabled: Boolean = false

  def loadRemoteDataFrame(baseUrl: String, path:String, credentials: Credentials): Option[DataFrame]

  def getSubInterpreter(sitePackagePath: String, whlPath: String): Option[SubInterpreter] =
    Some(JepInterpreterManager.getJepInterpreter(sitePackagePath, whlPath, Some(pythonHome)))

  def getRepositoryClient(): Option[OperatorRepository]
}
