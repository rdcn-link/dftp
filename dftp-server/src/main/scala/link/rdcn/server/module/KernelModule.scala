package link.rdcn.server.module

import link.rdcn.server.{ActionMethodService, Anchor, CrossModuleEvent, DftpActionRequest, DftpActionResponse, DftpGetStreamRequest, DftpGetStreamResponse, DftpModule, DftpPutStreamRequest, DftpPutStreamResponse, DftpRequest, DftpResponse, EventHub, EventSource, GetMethodService, GetStreamRequestParseService, LogService, PutMethodService, ServerContext}
import link.rdcn.struct.DataFrame
import link.rdcn.user.{AuthenticationService, Credentials, UserPrincipal}

import scala.collection.mutable.ArrayBuffer

class KernelModule extends DftpModule {
  private val authMethod = new CompositeAuthenticationService
  private val parseMethod = new CompositeGetStreamRequestParseService
  private val getMethod = new CompositeGetMethodService
  private val actionMethod = new CompositeActionMethodService
  private val putMethod = new CompositePutMethodService
  private val logMethod = new CompositeLogService

  def parseGetStreamRequest(token: Array[Byte], principal: UserPrincipal): DftpGetStreamRequest = {
    parseMethod.parse(token, principal)
  }

  def doGetStream(request: DftpGetStreamRequest, response: DftpGetStreamResponse): Unit = {
    getMethod.doGetStream(request, response)
  }

  def doPutStream(request: DftpPutStreamRequest, response: DftpPutStreamResponse): Unit = {
    putMethod.doPutStream(request, response)
  }

  def doAction(request: DftpActionRequest, response: DftpActionResponse): Unit = {
    actionMethod.doAction(request, response)
  }

  def doLog(request: DftpRequest, response: DftpResponse): Unit = {
    logMethod.doLog(request, response)
  }

  def authenticate(credentials: Credentials): UserPrincipal =
    authMethod.authenticate(credentials)

  override def init(anchor: Anchor, serverContext: ServerContext): Unit = {
    anchor.hook(new EventSource {
      override def init(eventHub: EventHub): Unit = {
        eventHub.fireEvent(new RequireAuthennticator(authMethod))
        eventHub.fireEvent(new RequireAccessLogger(logMethod))
        eventHub.fireEvent(new RequireGetMethodParser(parseMethod))
        eventHub.fireEvent(new RequireActionHandler(actionMethod))
        eventHub.fireEvent(new RequirePutStreamHandler(putMethod))
        eventHub.fireEvent(new RequireGetStreamHandler(getMethod))
      }
    })
  }

  override def destroy(): Unit = {
  }
}

class RequireAuthennticator(composite: CompositeAuthenticationService) extends CrossModuleEvent {
  def add(service: AuthenticationService) = composite.add(service)
}

class RequireAccessLogger(composite: CompositeLogService) extends CrossModuleEvent {
  def add(service: LogService) = composite.add(service)
}

class RequireGetMethodParser(composite: CompositeGetStreamRequestParseService) extends CrossModuleEvent {
  def add(service: GetStreamRequestParseService) = composite.add(service)
}

class RequireActionHandler(composite: CompositeActionMethodService) extends CrossModuleEvent {
  def add(service: ActionMethodService) = composite.add(service)
}

class RequirePutStreamHandler(composite: CompositePutMethodService) extends CrossModuleEvent {
  def add(service: PutMethodService) = composite.add(service)
}

class RequireGetStreamHandler(compositeGetMethodService: CompositeGetMethodService) extends CrossModuleEvent {
  def add(service: GetMethodService) = compositeGetMethodService.add(service)
}

class CompositeAuthenticationService extends AuthenticationService {
  val services = ArrayBuffer[AuthenticationService]()

  def add(service: AuthenticationService): Unit = services += service

  override def authenticate(credentials: Credentials): UserPrincipal = {
    services.find(_.accepts(credentials)).map(_.authenticate(credentials)).find(_ != null).getOrElse(null)
  }

  override def accepts(credentials: Credentials): Boolean = services.exists(_.accepts(credentials))
}

class CompositeGetStreamRequestParseService extends GetStreamRequestParseService {
  val services = ArrayBuffer[GetStreamRequestParseService]()

  override def accepts(token: Array[Byte]): Boolean = services.exists(_.accepts(token))

  override def parse(token: Array[Byte], principal: UserPrincipal): DftpGetStreamRequest = {
    services.find(_.accepts(token)).map(_.parse(token, principal)).find(_ != null).getOrElse(null)
  }

  def add(service: GetStreamRequestParseService): Unit = services += service
}

class CompositeGetMethodService extends GetMethodService {
  val services = ArrayBuffer[GetMethodService]()

  def add(service: GetMethodService): Unit = services += service

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

class CompositeActionMethodService extends ActionMethodService {
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

class CompositePutMethodService extends PutMethodService {
  val services = ArrayBuffer[PutMethodService]()

  def add(service: PutMethodService): Unit = services += service

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

class CompositeLogService extends LogService {
  val services = ArrayBuffer[LogService]()

  def add(service: LogService): Unit = services += service

  override def accepts(request: DftpRequest): Boolean = services.exists(_.accepts(request))

  //enabling all loggers
  override def doLog(request: DftpRequest, response: DftpResponse): Unit = {
    services.filter(_.accepts(request)).foreach(_.doLog(request, response))
  }
}