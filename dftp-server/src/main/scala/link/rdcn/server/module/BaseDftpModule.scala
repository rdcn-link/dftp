package link.rdcn.server.module

import link.rdcn.client.UrlValidator
import link.rdcn.operation.{ExecutionContext, TransformOp}
import link.rdcn.server._
import link.rdcn.struct.{BlobRegistry, DataFrame, DefaultDataFrame, Row, StructType}
import link.rdcn.user.UserPrincipal
import link.rdcn.util.DataUtils

import java.nio.charset.StandardCharsets
import scala.collection.mutable.ArrayBuffer

class BaseDftpModule extends DftpModule {

  private val dataFrameProviderHub = new CompositeDataFrameProvider
  private var serverContext: ServerContext = _

  private val getStreamHandler = new GetStreamHandler {
    override def accepts(request: DftpGetStreamRequest): Boolean = {
      request match {
        case _: DacpGetBlobStreamRequest => true
        case _: DftpGetPathStreamRequest => true
        case _ => false
      }
    }

    override def doGetStream(request: DftpGetStreamRequest, response: DftpGetStreamResponse): Unit = {
      request match {
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
        case r: DftpGetPathStreamRequest => {
          val dataFrame = r.getTransformOp().execute(new ExecutionContext {
            override def loadSourceDataFrame(dataFrameNameUrl: String): Option[DataFrame] = {
              try {
                Some(dataFrameProviderHub.getDataFrame(dataFrameNameUrl, r.getUserPrincipal())(serverContext))
              }catch {
                case e: IllegalAccessException => response.sendError(403, e.getMessage)
                  throw e
                case e: Exception => response.sendError(500, e.getMessage)
                  throw e
              }

            }
          })
          response.sendDataFrame(dataFrame)
        }
        case _ =>
          response.sendError(500, s"illegal DftpGetStreamRequest except DacpGetBlobStreamRequest but get $request")
      }
    }
  }

  private val eventHandlerGetStream = new EventHandler {

    override def accepts(event: CrossModuleEvent): Boolean =
      event.isInstanceOf[RequireGetStreamHandlerEvent]

    override def doHandleEvent(event: CrossModuleEvent): Unit = {
      event match {
        case require: RequireGetStreamHandlerEvent  =>
          require.add(getStreamHandler)
        case _ =>
      }
    }
  }

  override def init(anchor: Anchor, serverContext: ServerContext): Unit = {
    this.serverContext = serverContext

    //by default parsing BLOB_TICKET & URL_GET_TICKET
    anchor.hook(new EventHandler {
      override def accepts(event: CrossModuleEvent): Boolean =
        event.isInstanceOf[RequireGetStreamRequestParserEvent]

      override def doHandleEvent(event: CrossModuleEvent): Unit = {
        event match {
          case require: RequireGetStreamRequestParserEvent =>
            require.add(new GetStreamRequestParser {
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
    anchor.hook(eventHandlerGetStream)
    anchor.hook(new EventSource {
      override def init(eventHub: EventHub): Unit =
        eventHub.fireEvent(RequireDataFrameProviderEvent(dataFrameProviderHub))
    })
  }

  override def destroy(): Unit = {
  }
}

