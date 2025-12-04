package link.rdcn.server.module

import link.rdcn.client.UrlValidator
import link.rdcn.operation.{ExecutionContext, TransformOp}
import link.rdcn.server._
import link.rdcn.server.exception.{DataFrameAccessDeniedException, DataFrameNotFoundException}
import link.rdcn.struct.{BlobRegistry, DataFrame, DefaultDataFrame, Row, StructType}
import link.rdcn.user.UserPrincipal
import link.rdcn.util.DataUtils

import java.nio.charset.StandardCharsets

class BaseDftpModule extends DftpModule {

  //TODO: should all data frame providers be registered?
  private val dataFrameHolder = new Workers[DataFrameProviderService]
  private implicit var serverContext: ServerContext = _
  private val eventHandlerGetStream = new EventHandler {

    override def accepts(event: CrossModuleEvent): Boolean =
      event.isInstanceOf[CollectGetStreamMethodEvent]

    override def doHandleEvent(event: CrossModuleEvent): Unit = {
      event match {
        case require: CollectGetStreamMethodEvent =>

          //DacpGetBlobStreamRequest
          require.collect(
            new GetStreamMethod {
              override def accepts(request: DftpGetStreamRequest): Boolean = {
                request.isInstanceOf[DacpGetBlobStreamRequest]
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
                }
              }
            })

          //DftpGetPathStreamRequest
          require.collect(
            new GetStreamMethod {
              override def accepts(request: DftpGetStreamRequest): Boolean = {
                request.isInstanceOf[DftpGetPathStreamRequest]
              }

              override def doGetStream(request: DftpGetStreamRequest, response: DftpGetStreamResponse): Unit = {
                request match {
                  case r: DftpGetPathStreamRequest =>
                    try {
                      val dataFrame = r.getTransformOp().execute(new ExecutionContext {
                        override def loadSourceDataFrame(dataFrameNameUrl: String): Option[DataFrame] = {
                          Some(dataFrameHolder.work(new TaskRunner[DataFrameProviderService, DataFrame] {

                            override def acceptedBy(worker: DataFrameProviderService): Boolean = worker.accepts(dataFrameNameUrl)

                            override def executeWith(worker: DataFrameProviderService): DataFrame = worker.getDataFrame(dataFrameNameUrl, r.getUserPrincipal())

                            override def handleFailure(): DataFrame = throw new DataFrameNotFoundException(dataFrameNameUrl)
                          }))
                        }
                      })
                      response.sendDataFrame(dataFrame)
                    } catch {
                      case e: DataFrameAccessDeniedException => response.sendError(403, e.getMessage)
                        throw e
                      case e: DataFrameNotFoundException =>
                        response.sendError(404, e.getMessage)
                        throw e
                      case e: Exception => response.sendError(500, e.getMessage)
                        throw e
                    }
                }
              }
            }
          )
        case _ =>
      }
    }
  }

  override def init(anchor: Anchor, serverContext: ServerContext): Unit = {
    this.serverContext = serverContext

    //by default parsing BLOB_TICKET & URL_GET_TICKET
    anchor.hook(new EventHandler {
      override def accepts(event: CrossModuleEvent): Boolean =
        event.isInstanceOf[CollectParseRequestMethodEvent]

      override def doHandleEvent(event: CrossModuleEvent): Unit = {
        event match {
          case require: CollectParseRequestMethodEvent =>
            require.collect(new ParseRequestMethod {
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
        eventHub.fireEvent(CollectDataFrameProviderEvent(dataFrameHolder))
    })
  }

  override def destroy(): Unit = {
  }
}

trait DataFrameProviderService {
  def accepts(dataFrameUrl: String): Boolean

  def getDataFrame(dataFrameUrl: String, userPrincipal: UserPrincipal)(implicit ctx: ServerContext): DataFrame
}

case class CollectDataFrameProviderEvent(holder: Workers[DataFrameProviderService]) extends CrossModuleEvent

