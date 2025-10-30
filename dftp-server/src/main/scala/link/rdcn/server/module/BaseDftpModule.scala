package link.rdcn.server.module

import link.rdcn.client.UrlValidator
import link.rdcn.operation.TransformOp
import link.rdcn.server._
import link.rdcn.user.UserPrincipal

import java.nio.charset.StandardCharsets

class BaseDftpModule extends DftpModule {

  override def init(anchor: Anchor, serverContext: ServerContext): Unit = {
    //by default parsing BLOB_TICKET & URL_GET_TICKET
    anchor.hook(new EventHandler {
      override def accepts(event: CrossModuleEvent): Boolean =
        event.isInstanceOf[RequireGetMethodParser]

      override def doHandleEvent(event: CrossModuleEvent): Unit = {
        event match {
          case require: RequireGetMethodParser =>
            require.add(new GetStreamRequestParseService {
              val BLOB_TICKET: Byte = 1
              val URL_GET_TICKET: Byte = 2

              override def accepts(token: Array[Byte]): Boolean = {
                val typeId = token(0)
                typeId == BLOB_TICKET || typeId == URL_GET_TICKET
              }

              override def parse(bytes: Array[Byte], principal: UserPrincipal): DftpGetStreamRequest = {
                val buffer = java.nio.ByteBuffer.wrap(bytes)
                val typeId: Byte = buffer.get()
                val len = buffer.getInt()
                val b = new Array[Byte](len)
                buffer.get(b)
                val ticketContent = new String(b, StandardCharsets.UTF_8)
                typeId match {
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

                  case _ => null
                }
              }
            })
        }
      }
    })
  }

  override def destroy(): Unit = {
  }
}

