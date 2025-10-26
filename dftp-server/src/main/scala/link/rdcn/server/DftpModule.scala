package link.rdcn.server

import link.rdcn.Logging
import link.rdcn.client.UrlValidator
import link.rdcn.operation.TransformOp
import link.rdcn.struct.DataFrame
import link.rdcn.user.{AuthenticationService, Credentials, UserPrincipal}

import java.nio.charset.StandardCharsets
import scala.collection.mutable.ArrayBuffer

/**
 * @Author bluejoe2008
 * @Description:
 * @Data 2025/10/25 11:20
 * @Modified By:
 */
trait DftpModule {
  def init(anchor: Anchor, serverContext: ServerContext): Unit
  def destroy(): Unit
}

trait Anchor {
  def hook(service: GetRequestParseService): Unit
  def hook(service: AuthenticationService): Unit
  def hook(service: ActionMethodService): Unit
  //FIXME: register data sources? for reuse of getDocument()
  def hook(service: GetMethodService): Unit
  def hook(service: PutMethodService): Unit
  def hook(service: LogService): Unit
}

trait ActionMethodService {
  def accepts(request: DftpActionRequest): Boolean
  def doAction(request: DftpActionRequest, response: DftpActionResponse): Unit
}

trait GetRequestParseService {
  def accepts(token: Array[Byte]): Boolean
  def parse(token: Array[Byte], principal: UserPrincipal): DftpGetStreamRequest
}

trait LogService {
  def accepts(request: DftpRequest): Boolean
  def doLog(request: DftpRequest, response: DftpResponse): Unit
}

trait GetMethodService {
  def accepts(request: DftpGetStreamRequest): Boolean
  def doGet(request: DftpGetStreamRequest, response: DftpGetResponse): Unit
}

trait PutMethodService {
  def accepts(request: DftpPutRequest): Boolean
  def doPut(request: DftpPutRequest, response: DftpPutResponse): Unit
}

class Modules(serverContext: ServerContext) extends Logging {
  val modules = ArrayBuffer[DftpModule]()

  private val authMethod = new AuthenticationService {
    val services = ArrayBuffer[AuthenticationService]()

    def add(service: AuthenticationService): Unit = services += service

    override def authenticate(credentials: Credentials): UserPrincipal = {
      services.find(_.accepts(credentials)).map(_.authenticate(credentials)).find(_ != null).head
    }

    override def accepts(credentials: Credentials): Boolean = services.exists(_.accepts(credentials))
  }

  private val parseMethod = new GetRequestParseService {
    val services = ArrayBuffer[GetRequestParseService](new DftpGetRequestParseService(serverContext))

    override def accepts(token: Array[Byte]): Boolean = services.exists(_.accepts(token))

    override def parse(token: Array[Byte], principal: UserPrincipal): DftpGetStreamRequest = {
      services.find(_.accepts(token)).map(_.parse(token, principal)).find(_ != null).head
    }

    def add(service: GetRequestParseService): Unit = services += service
  }

  private val getMethod = new GetMethodService {
    val services = ArrayBuffer[GetMethodService]()

    def add(service: GetMethodService): Unit = services += service

    override def accepts(request: DftpGetStreamRequest): Boolean = services.exists(_.accepts(request))

    override def doGet(request: DftpGetStreamRequest, response: DftpGetResponse): Unit = {
      services.filter(_.accepts(request)).find(x => {
        var bingo = false
        val responseObserved = new DftpGetResponse {
          override def sendDataFrame(data: DataFrame): Unit = {
            response.sendDataFrame(data)
            bingo = true
          }

          override def sendError(errorCode: Int, message: String): Unit = response.sendError(errorCode, message)
        }
        x.doGet(request, responseObserved)
        bingo
      })
    }
  }

  private val actionMethod = new ActionMethodService {
    val services = ArrayBuffer[ActionMethodService]()

    def add(service: ActionMethodService): Unit = services += service

    override def accepts(request: DftpActionRequest): Boolean = services.exists(_.accepts(request))

    override def doAction(request: DftpActionRequest, response: DftpActionResponse): Unit = {
      services.filter(_.accepts(request)).find(x => {
        var bingo = false
        val responseObserved = new DftpActionResponse {
          override def sendData(data: Array[Byte]): Unit = {
            response.sendData(data)
            bingo = true
          }

          override def sendError(errorCode: Int, message: String): Unit = response.sendError(errorCode, message)
        }
        x.doAction(request, responseObserved)
        bingo
      })
    }
  }

  private val putMethod = new PutMethodService {
    val services = ArrayBuffer[PutMethodService]()

    def add(service: PutMethodService): Unit = services += service

    override def accepts(request: DftpPutRequest): Boolean = services.exists(_.accepts(request))

    override def doPut(request: DftpPutRequest, response: DftpPutResponse): Unit = {
      services.filter(_.accepts(request)).find(x => {
        var bingo = false
        val responseObserved = new DftpPutResponse {
          override def sendData(data: Array[Byte]): Unit = {
            response.sendData(data)
            bingo = true
          }

          override def sendError(errorCode: Int, message: String): Unit = response.sendError(errorCode, message)
        }
        x.doPut(request, responseObserved)
        bingo
      })
    }
  }

  private val logMethod = new LogService {
    val services = ArrayBuffer[LogService]()

    def add(service: LogService): Unit = services += service

    override def accepts(request: DftpRequest): Boolean = services.exists(_.accepts(request))

    //enabling all loggers
    override def doLog(request: DftpRequest, response: DftpResponse): Unit = {
      services.filter(_.accepts(request)).foreach(_.doLog(request, response))
    }
  }

  def addModule(module: DftpModule) {
    modules += module
  }

  val anchor = new Anchor {
    override def hook(service: AuthenticationService): Unit = {
      authMethod.add(service)
    }

    override def hook(service: ActionMethodService): Unit = {
      actionMethod.add(service)
    }

    override def hook(service: GetMethodService): Unit = {
      getMethod.add(service)
    }

    override def hook(service: PutMethodService): Unit = {
      putMethod.add(service)
    }

    override def hook(service: LogService): Unit = {
      logMethod.add(service)
    }

    override def hook(service: GetRequestParseService): Unit = {
      parseMethod.add(service)
    }
  }

  def init(): Unit = {
    modules.foreach(x => {
      x.init(anchor, serverContext)
      logger.info(s"loaded module: $x")
    })
  }

  def destroy(): Unit = {
    modules.foreach(x => {
      x.destroy()
    })
  }

  def parseGetStreamRequest(token: Array[Byte], principal: UserPrincipal): DftpGetStreamRequest = {
    parseMethod.parse(token, principal)
  }

  def doGet(request: DftpGetStreamRequest, response: DftpGetResponse): Unit = {
    getMethod.doGet(request, response)
  }

  def doPut(request: DftpPutRequest, response: DftpPutResponse): Unit = {
    putMethod.doPut(request, response)
  }

  def doAction(request: DftpActionRequest, response: DftpActionResponse): Unit = {
    actionMethod.doAction(request, response)
  }

  def doLog(request: DftpRequest, response: DftpResponse): Unit = {
    logMethod.doLog(request, response)
  }

  def authenticate(credentials: Credentials): UserPrincipal =
    authMethod.authenticate(credentials)
}

class DftpGetRequestParseService(serverContext: ServerContext) extends GetRequestParseService {
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
          case Left(message) => (s"${serverContext.getProtocolScheme}://${serverContext.getHost}:${serverContext.getPort}${dataFrameUrl}", dataFrameUrl)
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