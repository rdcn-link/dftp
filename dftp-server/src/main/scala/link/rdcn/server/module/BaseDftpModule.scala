package link.rdcn.server.module

import link.rdcn.client.UrlValidator
import link.rdcn.operation.{ExecutionContext, TransformOp}
import link.rdcn.server.{ActionMethodService, Anchor, CrossModuleEvent, DacpGetBlobStreamRequest, DftpActionRequest, DftpActionResponse, DftpGetPathStreamRequest, DftpGetStreamRequest, DftpGetStreamResponse, DftpModule, DftpPutStreamRequest, DftpPutStreamResponse, DftpRequest, DftpResponse, EventHandleService, EventSourceService, GetMethodService, GetStreamRequestParseService, LogService, PutMethodService, ServerContext}
import link.rdcn.struct.{BlobRegistry, DataFrame, DefaultDataFrame, Row, StructType}
import link.rdcn.user.UserPrincipal
import link.rdcn.util.DataUtils

import java.nio.charset.StandardCharsets

trait BaseDftpDataSource extends CrossModuleEvent{
  def getDataFrame(dataFrameName: String): DataFrame

  def action(actionName: String, parameter: Array[Byte]): Array[Byte]

  def put(dataFrame: DataFrame): Array[Byte]
}


class BaseDftpModule extends DftpModule {

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

  private val getStreamRequestParseService = new GetStreamRequestParseService {
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
  }

  private val getMethodService: GetMethodService = new GetMethodService {
    override def accepts(request: DftpGetStreamRequest): Boolean = {
      request match {
        case _: DftpGetPathStreamRequest => true
        case _: DacpGetBlobStreamRequest => true
        case _ => false
      }
    }

    override def doGetStream(request: DftpGetStreamRequest, response: DftpGetStreamResponse): Unit = {
      request match {
        case r: DftpGetPathStreamRequest => {
          val dataFrame = r.getTransformOp().execute(new ExecutionContext {
            override def loadSourceDataFrame(dataFrameNameUrl: String): Option[DataFrame] =
              Some(dataSource.getDataFrame(dataFrameNameUrl))
          })
          response.sendDataFrame(dataFrame)
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
        case _ => response.sendError(500, s"illegal DftpGetStreamRequest: $request")
      }
    }
  }

  private val actionMethodService = new ActionMethodService {

    override def accepts(request: DftpActionRequest): Boolean = true

    override def doAction(request: DftpActionRequest, response: DftpActionResponse): Unit = {
      try{
        val actionName = request.getActionName()
        val parameter = request.getParameter()
        response.sendData(dataSource.action(actionName, parameter))
      }catch {
        case e: Exception =>
          response.sendError(500, e.getMessage)
          throw e
      }

    }
  }

  private val putMethodService = new PutMethodService {
    //接收所有put请求，如果sendError modules会交给其他Service处理
    override def accepts(request: DftpPutStreamRequest): Boolean = true

    override def doPutStream(request: DftpPutStreamRequest, response: DftpPutStreamResponse): Unit = {
      try{
        response.sendData(dataSource.put(request.getDataFrame()))
      }catch {
        case e: Exception =>
          response.sendError(500, e.getMessage)
          throw e
      }
    }
  }

  override def init(anchor: Anchor, serverContext: ServerContext): Unit = {
    //TODO by default using a file based access logger
//    anchor.hook(new LogService {
//      //read config file, get log file path...
//      override def accepts(request: DftpRequest): Boolean = ???
//
//      override def doLog(request: DftpRequest, response: DftpResponse): Unit = ???
//    })

    this.serverContext = serverContext

    //by default parsing BLOB_TICKET & URL_GET_TICKET
    anchor.hook(getStreamRequestParseService)
    anchor.hook(eventHandleService)
    anchor.hook(getMethodService)
    anchor.hook(actionMethodService)
    anchor.hook(putMethodService)
  }

  override def destroy(): Unit = {
  }
}

