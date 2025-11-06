package link.rdcn.dacp.cook

import link.rdcn.Logging
import link.rdcn.dacp.optree.{FlowExecutionContext, OperatorRepository, RepositoryClient, TransformTree}
import link.rdcn.dacp.user.{PermissionService, RequirePermissionServiceEvent}
import link.rdcn.operation.TransformOp
import link.rdcn.server.module.{DataFrameProviderService, ObjectHolder, RequireDataFrameProviderEvent, RequireGetStreamHandlerEvent, RequireGetStreamRequestParserEvent}
import link.rdcn.server._
import link.rdcn.server.exception.{DataFrameAccessDeniedException, DataFrameNotFoundException}
import link.rdcn.struct.DataFrame
import link.rdcn.user.{Credentials, UserPrincipal}

import java.nio.charset.StandardCharsets

/**
 * @Author renhao
 * @Description:
 * @Data 2025/10/31 10:59
 * @Modified By:
 */
trait DacpCookStreamRequest extends DftpGetStreamRequest{
  def getTransformTree: TransformOp
}

class DacpCookModule() extends DftpModule with Logging {

  private implicit var serverContext: ServerContext = _
  private val dataFrameHolder = new ObjectHolder[DataFrameProviderService]
  private val permissionHolder = new ObjectHolder[PermissionService]

  override def init(anchor: Anchor, serverContext: ServerContext): Unit = {
    this.serverContext = serverContext
    anchor.hook(new EventHandler {
      override def accepts(event: CrossModuleEvent): Boolean = {
        event match {
          case r: RequireGetStreamRequestParserEvent => true
          case r: RequireGetStreamHandlerEvent => true
          case _ => false
        }
      }

      override def doHandleEvent(event: CrossModuleEvent): Unit = {
        event match {
          case r: RequireGetStreamRequestParserEvent => r.holder.set {
            old =>
              new GetStreamRequestParser {
                val COOK_TICKET: Byte = 3

                override def accepts(token: Array[Byte]): Boolean = {
                  val typeId = token(0)
                  typeId == COOK_TICKET
                }

                override def parse(bytes: Array[Byte], principal: UserPrincipal): DftpGetStreamRequest = {
                  val buffer = java.nio.ByteBuffer.wrap(bytes)
                  val typeId: Byte = buffer.get()

                  typeId match {
                    case COOK_TICKET =>
                      val len = buffer.getInt()
                      val b = new Array[Byte](len)
                      buffer.get(b)
                      val ticketContent = new String(b, StandardCharsets.UTF_8)
                      val transformOp = TransformTree.fromJsonString(ticketContent)
                      new DacpCookStreamRequest {
                        override def getUserPrincipal(): UserPrincipal = principal

                        override def getTransformTree: TransformOp = transformOp
                      }

                    case _ => old.parse(bytes, principal)
                  }
                }
              }
          }

          case r: RequireGetStreamHandlerEvent => r.holder.set {
            old =>
              new GetStreamHandler {

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
                      val flowExecutionContext: FlowExecutionContext = new FlowExecutionContext {

                        override def fairdHome: String = serverContext.getDftpHome().getOrElse("./")

                        override def pythonHome: String = sys.env
                          .getOrElse("PYTHON_HOME", throw new Exception("PYTHON_HOME environment variable is not set"))

                        override def loadSourceDataFrame(dataFrameNameUrl: String): Option[DataFrame] = {
                          try {
                            if(permissionHolder.invoke(!_.checkPermission(userPrincipal, dataFrameNameUrl), false)){
                              throw new DataFrameAccessDeniedException(dataFrameNameUrl)
                            }
                            Some(dataFrameHolder.invoke(_.getDataFrame(dataFrameNameUrl, userPrincipal)(serverContext),
                              throw new DataFrameNotFoundException(dataFrameNameUrl)))
                          } catch {
                            case e: DataFrameAccessDeniedException => response.sendError(403, e.getMessage)
                              throw e
                            case e: DataFrameNotFoundException => response.sendError(404, e.getMessage)
                             throw e
                            case e: Exception => response.sendError(500, e.getMessage)
                              throw e
                          }
                        }

                        //TODO Repository config
                        override def getRepositoryClient(): Option[OperatorRepository] = Some(new RepositoryClient("10.0.89.38", 8088))

                        //TODO UnionServer
                        override def loadRemoteDataFrame(baseUrl: String, path: String, credentials: Credentials): Option[DataFrame] = ???
                      }
                      response.sendDataFrame(transformTree.execute(flowExecutionContext))
                    }

                    case _ => old.doGetStream(request, response)
                  }
                }
              }
          }

          case _ =>
        }
      }
    })

    anchor.hook(new EventSource {
      override def init(eventHub: EventHub): Unit = {
        eventHub.fireEvent(RequireDataFrameProviderEvent(dataFrameHolder))
        eventHub.fireEvent(RequirePermissionServiceEvent(permissionHolder))
      }
    })
  }

  override def destroy(): Unit = {}
}
