package link.rdcn.server.module

import link.rdcn.server.{ActionHandler, Anchor, CrossModuleEvent, DftpActionRequest, DftpActionResponse, DftpGetStreamRequest, DftpGetStreamResponse, DftpModule, DftpPutStreamRequest, DftpPutStreamResponse, DftpRequest, DftpResponse, EventHub, EventSource, GetStreamHandler, GetStreamRequestParser, AccessLogger, PutStreamHandler, ServerContext}
import link.rdcn.struct.DataFrame
import link.rdcn.user.{AuthenticationService, Credentials, UserPrincipal}

import scala.collection.mutable.ArrayBuffer

class KernelModule extends DftpModule {
  private val authStub = new CompositeAuthenticator
  private val parseStub = new CompositeGetStreamRequestParser
  private val getStub = new CompositeGetStreamHandler
  private val actionStub = new CompositeActionHandler
  private val putStub = new CompositePutStreamHandler
  private val logStub = new CompositeAccessLogger

  def parseGetStreamRequest(token: Array[Byte], principal: UserPrincipal): DftpGetStreamRequest = {
    parseStub.parse(token, principal)
  }

  def getStream(request: DftpGetStreamRequest, response: DftpGetStreamResponse): Unit = {
    getStub.doGetStream(request, response)
  }

  def putStream(request: DftpPutStreamRequest, response: DftpPutStreamResponse): Unit = {
    putStub.doPutStream(request, response)
  }

  def doAction(request: DftpActionRequest, response: DftpActionResponse): Unit = {
    actionStub.doAction(request, response)
  }

  def logAccess(request: DftpRequest, response: DftpResponse): Unit = {
    logStub.doLog(request, response)
  }

  def authenticate(credentials: Credentials): UserPrincipal =
    authStub.authenticate(credentials)

  override def init(anchor: Anchor, serverContext: ServerContext): Unit = {
    anchor.hook(new EventSource {
      override def init(eventHub: EventHub): Unit = {
        eventHub.fireEvent(new RequireAuthenticatorEvent(authStub))
        eventHub.fireEvent(new RequireAccessLoggerEvent(logStub))
        eventHub.fireEvent(new RequireGetStreamRequestParserEvent(parseStub))
        eventHub.fireEvent(new RequireActionHandlerEvent(actionStub))
        eventHub.fireEvent(new RequirePutStreamHandlerEvent(putStub))
        eventHub.fireEvent(new RequireGetStreamHandlerEvent(getStub))
      }
    })
  }

  override def destroy(): Unit = {
  }
}

class RequireAuthenticatorEvent(composite: CompositeAuthenticator) extends CrossModuleEvent {
  def add(service: AuthenticationService) = composite.add(service)
}

class RequireAccessLoggerEvent(composite: CompositeAccessLogger) extends CrossModuleEvent {
  def add(service: AccessLogger) = composite.add(service)
}

class RequireGetStreamRequestParserEvent(composite: CompositeGetStreamRequestParser) extends CrossModuleEvent {
  def add(service: GetStreamRequestParser) = composite.add(service)
}

class RequireActionHandlerEvent(composite: CompositeActionHandler) extends CrossModuleEvent {
  def add(service: ActionHandler) = composite.add(service)
}

class RequirePutStreamHandlerEvent(composite: CompositePutStreamHandler) extends CrossModuleEvent {
  def add(service: PutStreamHandler) = composite.add(service)
}

class RequireGetStreamHandlerEvent(compositeGetMethodService: CompositeGetStreamHandler) extends CrossModuleEvent {
  def add(service: GetStreamHandler) = compositeGetMethodService.add(service)
}

class CompositeAuthenticator extends AuthenticationService {
  val services = ArrayBuffer[AuthenticationService]()

  def add(service: AuthenticationService): Unit = services += service

  override def authenticate(credentials: Credentials): UserPrincipal = {
    services.find(_.accepts(credentials)).map(_.authenticate(credentials)).find(_ != null).getOrElse(null)
  }

  override def accepts(credentials: Credentials): Boolean = services.exists(_.accepts(credentials))
}

class CompositeGetStreamRequestParser extends GetStreamRequestParser {
  val services = ArrayBuffer[GetStreamRequestParser]()

  override def accepts(token: Array[Byte]): Boolean = services.exists(_.accepts(token))

  override def parse(token: Array[Byte], principal: UserPrincipal): DftpGetStreamRequest = {
    services.find(_.accepts(token)).map(_.parse(token, principal)).find(_ != null).getOrElse(null)
  }

  def add(service: GetStreamRequestParser): Unit = services += service
}

class CompositeGetStreamHandler extends GetStreamHandler {
  val services = ArrayBuffer[GetStreamHandler]()

  def add(service: GetStreamHandler): Unit = services += service

  override def accepts(request: DftpGetStreamRequest): Boolean = services.exists(_.accepts(request))

  override def doGetStream(request: DftpGetStreamRequest, response: DftpGetStreamResponse): Unit = {
    services.filter(_.accepts(request)).find(x => {
      var bingo = false
      val responseObserved = new DftpGetStreamResponse {
        override def sendDataFrame(data: DataFrame): Unit = {
          response.sendDataFrame(data)
          bingo = true
        }

        override def sendError(errorCode: Int, message: String): Unit = response.sendError(errorCode, message)
      }

      x.doGetStream(request, responseObserved)
      bingo
    })
  }
}

class CompositeActionHandler extends ActionHandler {
  val services = ArrayBuffer[ActionHandler]()

  def add(service: ActionHandler): Unit = services += service

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

class CompositePutStreamHandler extends PutStreamHandler {
  val services = ArrayBuffer[PutStreamHandler]()

  def add(service: PutStreamHandler): Unit = services += service

  override def accepts(request: DftpPutStreamRequest): Boolean = services.exists(_.accepts(request))

  override def doPutStream(request: DftpPutStreamRequest, response: DftpPutStreamResponse): Unit = {
    services.filter(_.accepts(request)).find(x => {
      var bingo = false
      val responseObserved = new DftpPutStreamResponse {
        override def sendData(data: Array[Byte]): Unit = {
          response.sendData(data)
          bingo = true
        }

        override def sendError(errorCode: Int, message: String): Unit = response.sendError(errorCode, message)
      }

      x.doPutStream(request, responseObserved)
      bingo
    })
  }
}

class CompositeAccessLogger extends AccessLogger {
  val services = ArrayBuffer[AccessLogger]()

  def add(service: AccessLogger): Unit = services += service

  override def accepts(request: DftpRequest): Boolean = services.exists(_.accepts(request))

  //enabling all loggers
  override def doLog(request: DftpRequest, response: DftpResponse): Unit = {
    services.filter(_.accepts(request)).foreach(_.doLog(request, response))
  }
}