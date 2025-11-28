package link.rdcn.dacp.proxy

import link.rdcn.client.{DacpClient, UrlValidator}
import link.rdcn.dacp.cook.{DacpCookModule, DacpCookStreamRequest}
import link.rdcn.operation.TransformOp
import link.rdcn.dacp.optree.TransformTree
import link.rdcn.server.module._
import link.rdcn.server._
import link.rdcn.struct.{BlobRegistry, DefaultDataFrame, Row, StructType}
import link.rdcn.user.{AuthenticationMethod, Credentials, UserPrincipal}
import link.rdcn.util.DataUtils

import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentHashMap

/**
 * @Author renhao
 * @Description:
 * @Data 2025/11/5 11:31
 * @Modified By:
 */
class ClientManage(val targetServerUrl: String) {
  private val clientCache = new ConcurrentHashMap[Credentials, DacpClient]()
  def getInternalClient(credentials: Credentials): DacpClient = {
    if(clientCache.contains(credentials)) clientCache.get(credentials)
    else {
      val client = DacpClient.connect(targetServerUrl, credentials)
      clientCache.put(credentials, client)
      client
    }
  }
}

class PutStreamProxyModule(clientManage: ClientManage) extends DftpModule {
  private val putMethodService = new PutStreamMethod {
    override def accepts(request: DftpPutStreamRequest): Boolean = true

    override def doPutStream(request: DftpPutStreamRequest, response: DftpPutStreamResponse): Unit = {
      val internalClient = clientManage.getInternalClient(request.getUserPrincipal()
        .asInstanceOf[ProxyUserPrincipal].credentials)
      try{
        val resultBytes = internalClient.put(request.getDataFrame())
        response.sendData(resultBytes)
      }catch {
        case e: Exception => response.sendError(500, e.getMessage)
      }
    }
  }

  override def init(anchor: Anchor, serverContext: ServerContext): Unit = {
    anchor.hook(new EventHandler {
      override def accepts(event: CrossModuleEvent): Boolean =
        event.isInstanceOf[CollectPutStreamMethodEvent]

      override def doHandleEvent(event: CrossModuleEvent): Unit = {
        event match {
          case r: CollectPutStreamMethodEvent => r.collect(putMethodService)
          case _ =>
        }
      }
    })
  }

  override def destroy(): Unit = {}
}

class ActionProxyModule(clientManage: ClientManage) extends DftpModule {

  private val actionMethodService = new ActionMethod {
    override def accepts(request: DftpActionRequest): Boolean = true

    override def doAction(request: DftpActionRequest, response: DftpActionResponse): Unit = {
      val internalClient = clientManage.getInternalClient(request.getUserPrincipal()
        .asInstanceOf[ProxyUserPrincipal].credentials)
      request.getActionName() match {
        case name if name == "/getTargetServerUrl" =>
          response.sendData(clientManage.targetServerUrl.getBytes("UTF-8"))
        case _ =>
          try{
            val resultBytes: Array[Byte] =
              internalClient.doAction(request.getActionName(), request.getParameterAsMap())
            response.sendData(resultBytes)
          }catch {
            case e: Exception => response.sendError(500, e.getMessage)
          }
      }
    }
  }

  override def init(anchor: Anchor, serverContext: ServerContext): Unit = {
    anchor.hook(new EventHandler {
      override def accepts(event: CrossModuleEvent): Boolean = {
        event.isInstanceOf[CollectActionMethodEvent]
      }

      override def doHandleEvent(event: CrossModuleEvent): Unit = {
        event match {
          case r: CollectActionMethodEvent => r.collect(actionMethodService)
          case _ =>
        }
      }
    })
  }

  override def destroy(): Unit = {}
}

class StreamProxyModule(clientManage: ClientManage) extends DftpModule {
  override def init(anchor: Anchor, serverContext: ServerContext): Unit = {
    anchor.hook(new EventHandler {
      override def accepts(event: CrossModuleEvent): Boolean =
        event match {
          case _: CollectParseRequestMethodEvent => true
          case _: CollectGetStreamMethodEvent => true
          case _ => false
        }

      override def doHandleEvent(event: CrossModuleEvent): Unit = {
        event match {
          case r: CollectParseRequestMethodEvent => {
            r.collect(new ParseRequestMethod {
              val BLOB_TICKET: Byte = 1
              val URL_GET_TICKET: Byte = 2
              val COOK_TICKET: Byte = 3

              override def accepts(token: Array[Byte]): Boolean = true

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
                  case BLOB_TICKET => {
                    new DacpGetBlobStreamRequest {
                      override def getBlobId(): String = ticketContent

                      override def getUserPrincipal(): UserPrincipal = principal
                    }
                  }

                  case URL_GET_TICKET => {
                    val transformOp = TransformOp.fromJsonString(ticketContent)
                    val dataFrameUrl = transformOp.sourceUrlList.head
                    val urlValidator = UrlValidator(serverContext.getProtocolScheme)
                    val urlAndPath = urlValidator.validate(dataFrameUrl) match {
                      case Right(v) => (dataFrameUrl, v._3)
                      case Left(message) =>
                        (s"${serverContext.getProtocolScheme}://${serverContext.getHost}:${serverContext.getPort}${dataFrameUrl}", dataFrameUrl)
                    }

                    new DftpGetPathStreamRequest {
                      override def getRequestPath(): String = urlAndPath._2

                      override def getRequestURL(): String = urlAndPath._1

                      override def getUserPrincipal(): UserPrincipal = principal

                      override def getTransformOp(): TransformOp = transformOp
                    }
                  }
                  case _ => throw new Exception(s"Illegal ticket ID $typeId")
                }
              }
            })
          }
          case r: CollectGetStreamMethodEvent => {
            r.collect(new GetStreamMethod {
              override def accepts(request: DftpGetStreamRequest): Boolean = true

              override def doGetStream(request: DftpGetStreamRequest, response: DftpGetStreamResponse): Unit = {
                val internalClient = clientManage.getInternalClient(request.getUserPrincipal().asInstanceOf[ProxyUserPrincipal].credentials)
                request match {
                  case r: DacpCookStreamRequest => {
                    try{
                      val df = internalClient.executeTransformTree(r.getTransformTree)
                      response.sendDataFrame(df)
                    }catch {
                      //TODO Add status code on the client
                      case e: Exception => response.sendError(500, e.getMessage)
                    }
                  }
                  case r: DftpGetPathStreamRequest => {
                    try {
                      val df = internalClient.executeTransformTree(r.getTransformOp())
                      response.sendDataFrame(df)
                    } catch {
                      case e: Exception => response.sendError(500, e.getMessage)
                    }
                  }
                  case r: DacpGetBlobStreamRequest => {
                    val blobId = r.getBlobId()
                    val blob = BlobRegistry.getBlob(blobId)
                    if (blob.isEmpty) {
                      response.sendError(404, s"blob ${blobId} resource closed")
                    } else {
                      blob.get.offerStream(inputStream => {
                        val stream: Iterator[Row] = DataUtils.chunkedIterator(inputStream)
                          .map(bytes => Row.fromSeq(Seq(bytes)))
                        val schema = StructType.blobStreamStructType
                        response.sendDataFrame(DefaultDataFrame(schema, stream))
                      })
                    }
                  }
                  case _ => response.sendError(500, s"illegal DftpGetStreamRequest except DacpGetStreamRequest but get $request")
                }
              }
            })
          }
          case _ =>
        }
      }
    })
  }

  override def destroy(): Unit = {}
}

class AuthProxyModule extends DftpModule {
  override def init(anchor: Anchor, serverContext: ServerContext): Unit =
    anchor.hook(new EventHandler {
      override def accepts(event: CrossModuleEvent): Boolean =
        event.isInstanceOf[CollectAuthenticationMethodEvent]

      override def doHandleEvent(event: CrossModuleEvent): Unit = {
        event match {
          case require: CollectAuthenticationMethodEvent =>
            require.collect(new AuthenticationMethod {

              override def accepts(credentials: Credentials): Boolean = true

              override def authenticate(credentials: Credentials): UserPrincipal =
                ProxyUserPrincipal(credentials)
            })
          case _ =>
        }
      }
    })

  override def destroy(): Unit = {}
}

case class ProxyUserPrincipal(credentials: Credentials) extends UserPrincipal
