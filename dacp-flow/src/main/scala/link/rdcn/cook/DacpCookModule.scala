package link.rdcn.cook

import link.rdcn.Logging
import link.rdcn.operation.TransformOp
import link.rdcn.optree.{FlowExecutionContext, OperatorRepository, RepositoryClient, TransformTree}
import link.rdcn.server.module.BaseDftpDataSource
import link.rdcn.server.{Anchor, CrossModuleEvent, DftpGetStreamRequest, DftpGetStreamResponse, DftpModule, EventHandleService, GetStreamHandler, GetStreamRequestParser, ServerContext}
import link.rdcn.struct.DataFrame
import link.rdcn.user.{Credentials, UserPrincipal}

import java.nio.charset.StandardCharsets

/**
 * @Author renhao
 * @Description:
 * @Data 2025/10/29 16:00
 * @Modified By:
 */
trait DacpCookStreamRequest extends DftpGetStreamRequest{
  def getTransformTree: TransformOp
}

class DacpCookModule extends DftpModule with Logging{

  private var serverContext: ServerContext = _
  private var dataSource: BaseDftpDataSource = _

  private val eventHandleService: EventHandleService = new EventHandleService {
    override def accepts(event: CrossModuleEvent): Boolean =
      event match {
        case _: BaseDftpDataSource => true
        case _ => false
      }

    override def doHandleEvent(event: CrossModuleEvent): Unit =
      dataSource = event.asInstanceOf[BaseDftpDataSource]
  }

  private val getStreamRequestParseService = new GetStreamRequestParser {
    val COOK_TICKET: Byte = 3

    override def accepts(token: Array[Byte]): Boolean = {
      val typeId = token(0)
      typeId == COOK_TICKET
    }

    override def parse(bytes: Array[Byte], principal: UserPrincipal): DftpGetStreamRequest = {
      val buffer = java.nio.ByteBuffer.wrap(bytes)
      val typeId: Byte = buffer.get()
      val len = buffer.getInt()
      val b = new Array[Byte](len)
      buffer.get(b)
      val ticketContent = new String(b, StandardCharsets.UTF_8)
      typeId match {
        case COOK_TICKET =>
          val transformOp = TransformTree.fromJsonString(ticketContent)
          new DacpCookStreamRequest {
            override def getUserPrincipal(): UserPrincipal = principal

            override def getTransformTree: TransformOp = transformOp
          }
        case _ => null
      }
    }
  }

  private val getMethodService: GetStreamHandler = new GetStreamHandler {

    override def accepts(request: DftpGetStreamRequest): Boolean = {
      request match {
        case _: DacpCookStreamRequest => true
        case _ => false
      }
    }

    override def doGetStream(request: DftpGetStreamRequest, response: DftpGetStreamResponse): Unit = {
      request match {
        case request: DacpCookStreamRequest => {
          val transformTree = request.getTransformTree
          val userPrincipal = request.getUserPrincipal()
          //TODO check permission
          response.sendDataFrame(transformTree.execute(flowExecutionContext))
        }
        case _ => response.sendError(500, s"illegal DftpGetStreamRequest: $request")
      }
    }
  }

  private val flowExecutionContext: FlowExecutionContext = new FlowExecutionContext {

    override def fairdHome: String = serverContext.getDftpHome().getOrElse("./")

    override def pythonHome: String = sys.env
      .getOrElse("PYTHON_HOME", throw new Exception("PYTHON_HOME environment variable is not set"))

    override def loadSourceDataFrame(dataFrameNameUrl: String): Option[DataFrame] = {
      try {
        Some(dataSource.getDataFrame(dataFrameNameUrl))
      } catch {
        case e: Exception =>
          logger.error(e)
          None
      }
    }

    //TODO Repository config
    override def getRepositoryClient(): Option[OperatorRepository] = Some(new RepositoryClient("10.0.89.38", 8088))
   //TODO UnionServer
    override def loadRemoteDataFrame(baseUrl: String, path: String, credentials: Credentials): Option[DataFrame] = ???
  }

  override def init(anchor: Anchor, serverContext: ServerContext): Unit = {
    this.serverContext = serverContext

    anchor.hook(getStreamRequestParseService)
    anchor.hook(eventHandleService)
    anchor.hook(getMethodService)
  }

  override def destroy(): Unit = {}
}