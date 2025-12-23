package link.rdcn.dacp.cook

import link.rdcn.Logging
import link.rdcn.client.{DftpClient, UrlValidator}
import link.rdcn.dacp.cook.JobStatus.{COMPLETE, RUNNING}
import link.rdcn.dacp.optree._
import link.rdcn.dacp.recipe.ExecutionResult
import link.rdcn.message.DftpTicket
import link.rdcn.operation.TransformOp
import link.rdcn.server._
import link.rdcn.server.exception.{DataFrameAccessDeniedException, DataFrameNotFoundException}
import link.rdcn.server.module._
import link.rdcn.struct.{ClosableIterator, DataFrame, DefaultDataFrame, Row}
import link.rdcn.user.{Credentials, UserPrincipal}
import link.rdcn.util.CodecUtils
import org.apache.arrow.flight.Ticket
import org.json.JSONObject

import java.nio.charset.StandardCharsets
import java.time.format.DateTimeFormatter
import java.time.{ZoneId, ZonedDateTime}
import java.util.concurrent.atomic.AtomicLong
import scala.collection.concurrent.TrieMap

/**
 * @Author renhao
 * @Description:
 * @Data 2025/10/31 10:59
 * @Modified By:
 */
trait DacpJobStreamRequest extends DftpGetStreamRequest {
  def getJobId: String
  def getDataFrameName: String
}

trait DacpCookStreamRequest extends DftpGetStreamRequest {
  def getTransformTree: TransformOp
}

class DacpCookModule() extends DftpModule with Logging {

  private implicit var serverContext: ServerContext = _
  private val dataFrameHolder = new Workers[DataFrameProviderService]

  private val formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
  private val recipeCounter = new AtomicLong(0L)
  private val jobResultCache = TrieMap.empty[String, ExecutionResult]
  private val jobTransformOpsCache = TrieMap.empty[String, Seq[TransformOp]]

  override def init(anchor: Anchor, serverContext: ServerContext): Unit = {
    this.serverContext = serverContext

    def flowExecutionContext(userPrincipal: UserPrincipal): FlowExecutionContext = new FlowExecutionContext {

      override def fairdHome: String = serverContext.getDftpHome().getOrElse("./")

      override def pythonHome: String = sys.env
        .getOrElse("PYTHON_HOME", throw new Exception("PYTHON_HOME environment variable is not set"))

      override def isAsyncEnabled(wrapper: TransformFunctionWrapper): Boolean = wrapper match {
        case r: RepositoryOperator => r.transformFunctionWrapper match {
          case _: FifoFileRepositoryBundle => true
          case _  => false
        }
        case _ => false
      }

      override def loadSourceDataFrame(dataFrameNameUrl: String): Option[DataFrame] = {
        Some(dataFrameHolder.work(new TaskRunner[DataFrameProviderService, DataFrame]() {

          override def acceptedBy(worker: DataFrameProviderService): Boolean = worker.accepts(dataFrameNameUrl)

          override def executeWith(worker: DataFrameProviderService): DataFrame = worker.getDataFrame(dataFrameNameUrl, userPrincipal)(serverContext)

          override def handleFailure(): DataFrame = throw new DataFrameNotFoundException(dataFrameNameUrl)
        }
        ))
      }

      //TODO Repository config
      override def getRepositoryClient(): Option[OperatorRepository] = Some(new RepositoryClient("http://10.0.89.39", 8090))

      override def loadRemoteDataFrame(baseUrl: String, transformOp: TransformOp, credentials: Credentials): Option[DataFrame] = {
        val urlInfo = UrlValidator.extractBase(baseUrl)
          .getOrElse(throw new IllegalArgumentException(s"Invalid URL format $baseUrl"))
        val client = new Client(urlInfo._2, urlInfo._3)
        client.login(credentials)
        Some(client.getRemoteDataFrame(transformOp.toJsonString))
      }

      private class Client(host: String, port: Int, useTLS: Boolean = false) extends DftpClient(host, port, useTLS) {
        def getRemoteDataFrame(transformOpStr: String): DataFrame = {
          val schemaAndIter = getStream(new Ticket(CookTicket(transformOpStr).encodeTicket()))
          val stream = schemaAndIter._2.map(seq => Row.fromSeq(seq))
          DefaultDataFrame(schemaAndIter._1, stream)
        }
      }
      private case class CookTicket(ticketContent: String) extends DftpTicket {
        override val typeId: Byte = 3
      }
    }

    anchor.hook(new EventHandler {
      override def accepts(event: CrossModuleEvent): Boolean = {
        event match {
          case r: CollectParseRequestMethodEvent => true
          case r: CollectGetStreamMethodEvent => true
          case r: CollectActionMethodEvent => true
          case _ => false
        }
      }

      override def doHandleEvent(event: CrossModuleEvent): Unit = {
        event match {
          case r: CollectActionMethodEvent =>
            r.collect(new ActionMethod {

              override def accepts(request: DftpActionRequest): Boolean = {
                request.getActionName() match {
                  case "submit" => true
                  case "getJobStatus" => true
                  case "getJobExecuteResult" => true
                  case "getJobExecuteProcess" => true
                  case _ => false
                }
              }

              override def doAction(request: DftpActionRequest, response: DftpActionResponse): Unit = {
                val paramsMap = request.getParameterAsMap()
                request.getActionName() match {
                  case "submit" =>
                    val jobId = getRecipeId()
                    val flowJson = paramsMap.getOrElse("flowJson", response.sendError(400, "flow json empty"))
                    val transformOps: Seq[TransformOp] = TransformTree.fromFlowdJsonString(flowJson.toString)
                    val ctx = flowExecutionContext(request.getUserPrincipal())
                    val dfs = transformOps.map(_.execute(ctx))
                    val executeResult = new ExecutionResult {
                      override def single(): DataFrame = dfs.head

                      override def get(name: String): DataFrame = dfs(name.toInt - 1)

                      override def map(): Map[String, DataFrame] = dfs.zipWithIndex.map {
                        case (dataFrame, id) => (id.toString, dataFrame)
                      }.toMap
                    }
                    jobResultCache.put(jobId, executeResult)
                    jobTransformOpsCache.put(jobId, transformOps)
                    response.sendData(CodecUtils.encodeString(jobId))
                  case "getJobStatus" =>
                    val jobId = paramsMap.getOrElse("jobId", response.sendError(400, "empty request require jobId"))
                    val executionResult = jobResultCache.get(jobId.toString)
                    if(executionResult.nonEmpty){
                      val df = executionResult.get.map().values.toList.find(df => df.mapIterator(_.hasNext))
                      if(df.nonEmpty) response.sendData(CodecUtils.encodeString(RUNNING.name))
                      else response.sendData(CodecUtils.encodeString(COMPLETE.name))
                    }else response.sendError(404, s"job $jobId not exist")
                  case "getJobExecuteResult" =>
                    val jobId = paramsMap.getOrElse("jobId", response.sendError(400, "empty request require jobId"))
                    val executionResult = jobResultCache.get(jobId.toString)
                    if(executionResult.isEmpty) response.sendError(404, s"job $jobId not exist")
                    else response.sendData(executionResult.get.map().keys.toList.map((_, jobId)).toMap)
                  case "getJobExecuteProcess" =>
                    val jobId = paramsMap.getOrElse("jobId", response.sendError(400, "empty request require jobId"))
                    val transformOps = jobTransformOpsCache.get(jobId.toString)
                    if(transformOps.isEmpty) response.sendError(404, s"job $jobId not exist")
                    else{
                      val processSeq: Seq[Double] = transformOps.get.map(_.executionProgress).filter(_.nonEmpty).map(_.get)
                      if(processSeq.isEmpty) response.sendError(400, s"Unable to calculate ${transformOps.mkString(",")} progress")
                      else {
                        val process = processSeq.sum/processSeq.length
                        response.sendData(CodecUtils.encodeString(process.toString))
                      }
                    }
                  case _ => response.sendError(400, "")
                }
              }
            })

          case r: CollectParseRequestMethodEvent => r.collect(
            new ParseRequestMethod() {
              val COOK_TICKET: Byte = 3
              val JOB_TICKET: Byte = 4

              override def accepts(token: Array[Byte]): Boolean = {
                val typeId = token(0)
                typeId match {
                  case COOK_TICKET => true
                  case JOB_TICKET => true
                  case _ => false
                }
              }

              override def parse(bytes: Array[Byte], principal: UserPrincipal): DftpGetStreamRequest = {
                val buffer = java.nio.ByteBuffer.wrap(bytes)
                val typeId: Byte = buffer.get()
                val len = buffer.getInt()
                val b = new Array[Byte](len)
                buffer.get(b)
                val ticketContent = new String(b, StandardCharsets.UTF_8)

                typeId match {
                  case JOB_TICKET =>
                    val jo = new JSONObject(ticketContent)
                    new DacpJobStreamRequest {
                      override def getJobId: String = jo.getString("jobId")

                      override def getDataFrameName: String = jo.getString("dataFrameName")

                      override def getUserPrincipal(): UserPrincipal = principal
                    }

                  case COOK_TICKET =>
                    val transformOp = TransformTree.fromJsonString(ticketContent)
                    new DacpCookStreamRequest {
                      override def getUserPrincipal(): UserPrincipal = principal

                      override def getTransformTree: TransformOp = transformOp
                    }
                }
              }
            })

          case r: CollectGetStreamMethodEvent =>
            r.collect(
              new GetStreamMethod() {

                override def accepts(request: DftpGetStreamRequest): Boolean = {
                  request match {
                    case _: DacpCookStreamRequest => true
                    case _: DacpJobStreamRequest => true
                    case _ => false
                  }
                }

                override def doGetStream(request: DftpGetStreamRequest, response: DftpGetStreamResponse): Unit = {
                  request match {
                    case request: DacpCookStreamRequest => {
                      val transformTree = request.getTransformTree
                      val userPrincipal = request.getUserPrincipal()
                      try {
                        val result = transformTree.execute(flowExecutionContext(userPrincipal))
                        response.sendDataFrame(result)
                      } catch {
                        case e: DataFrameAccessDeniedException => response.sendError(403, e.getMessage)
                          throw e
                        case e: DataFrameNotFoundException => response.sendError(404, e.getMessage)
                          throw e
                        case e: Exception => response.sendError(500, e.getMessage)
                          throw e
                      }
                    }
                    case r: DacpJobStreamRequest =>
                      try{
                        val jobId = r.getJobId
                        val executionResult = jobResultCache.get(r.getJobId)
                        if(executionResult.isEmpty) response.sendError(404, s"job $jobId not exist")
                        else {
                          val df = executionResult.get.map().get(r.getDataFrameName)
                          if(df.isEmpty) response.sendError(404, s"not found dataFrame ${r.getDataFrameName} in job $jobId")
                          response.sendDataFrame(df.get)
                        }
                      }catch {
                        case e: DataFrameAccessDeniedException => response.sendError(403, e.getMessage)
                          throw e
                        case e: DataFrameNotFoundException => response.sendError(404, e.getMessage)
                          throw e
                        //TODO Add status code on the client
                        case e: Exception => response.sendError(500, e.getMessage)
                      }
                  }
                }
              })

          case _ =>
        }
      }
    })

    anchor.hook(new EventSource {
      override def init(eventHub: EventHub): Unit = {
        eventHub.fireEvent(CollectDataFrameProviderEvent(dataFrameHolder))
      }
    })
  }

  private def getRecipeId(): String = {
    val timestamp = ZonedDateTime.now(ZoneId.of("UTC")).format(formatter)
    val seq = recipeCounter.incrementAndGet()
    s"job-${timestamp}-${seq}"
  }

  override def destroy(): Unit = {}
}
