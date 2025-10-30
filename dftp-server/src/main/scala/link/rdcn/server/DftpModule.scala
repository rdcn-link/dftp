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
  def hook(service: GetStreamRequestParseService): Unit
  def hook(service: AuthenticationService): Unit
  def hook(service: ActionMethodService): Unit
  def hook(service: GetMethodService): Unit
  def hook(service: PutMethodService): Unit
  def hook(service: LogService): Unit
  def hook(service: EventHandleService): Unit
  def hook(service: EventSourceService): Unit
}

trait CrossModuleEvent

trait EventHandleService {
  def accepts(event: CrossModuleEvent): Boolean
  def doHandleEvent(event: CrossModuleEvent): Unit
}

trait EventSourceService {
  def create(): CrossModuleEvent
}

trait ActionMethodService {
  def accepts(request: DftpActionRequest): Boolean
  def doAction(request: DftpActionRequest, response: DftpActionResponse): Unit
}

trait GetStreamRequestParseService {
  def accepts(token: Array[Byte]): Boolean
  def parse(token: Array[Byte], principal: UserPrincipal): DftpGetStreamRequest
}

trait LogService {
  def accepts(request: DftpRequest): Boolean
  def doLog(request: DftpRequest, response: DftpResponse): Unit
}

trait GetMethodService {
  def accepts(request: DftpGetStreamRequest): Boolean
  def doGetStream(request: DftpGetStreamRequest, response: DftpGetStreamResponse): Unit
}

trait PutMethodService {
  def accepts(request: DftpPutStreamRequest): Boolean
  def doPutStream(request: DftpPutStreamRequest, response: DftpPutStreamResponse): Unit
}

class Modules(serverContext: ServerContext) extends Logging {
  private val modules = ArrayBuffer[DftpModule]()

  private val authMethod = new AuthenticationService {
    val services = ArrayBuffer[AuthenticationService]()

    def add(service: AuthenticationService): Unit = services += service

    override def authenticate(credentials: Credentials): UserPrincipal = {
      services.find(_.accepts(credentials)).map(_.authenticate(credentials)).find(_ != null).getOrElse(null)
    }

    override def accepts(credentials: Credentials): Boolean = services.exists(_.accepts(credentials))
  }

  private val parseMethod = new GetStreamRequestParseService {
    val services = ArrayBuffer[GetStreamRequestParseService]()

    override def accepts(token: Array[Byte]): Boolean = services.exists(_.accepts(token))

    override def parse(token: Array[Byte], principal: UserPrincipal): DftpGetStreamRequest = {
      services.find(_.accepts(token)).map(_.parse(token, principal)).find(_ != null).getOrElse(null)
    }

    def add(service: GetStreamRequestParseService): Unit = services += service
  }

  private val getMethod = new GetMethodService {
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

  private val logMethod = new LogService {
    val services = ArrayBuffer[LogService]()

    def add(service: LogService): Unit = services += service

    override def accepts(request: DftpRequest): Boolean = services.exists(_.accepts(request))

    //enabling all loggers
    override def doLog(request: DftpRequest, response: DftpResponse): Unit = {
      services.filter(_.accepts(request)).foreach(_.doLog(request, response))
    }
  }

  def addModule(module: DftpModule): Modules = {
    modules += module
    this
  }

  private val eventHandlers = ArrayBuffer[EventHandleService]()
  private val eventSources = ArrayBuffer[EventSourceService]()

  private val anchor = new Anchor {

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

    override def hook(service: GetStreamRequestParseService): Unit = {
      parseMethod.add(service)
    }

    override def hook(service: EventHandleService): Unit = {
      eventHandlers += service
    }

    override def hook(service: EventSourceService): Unit = {
      eventSources += service
    }
  }

  private def fireEvent(event: CrossModuleEvent): Unit = {
    eventHandlers.filter(_.accepts(event)).foreach(_.doHandleEvent(event))
  }

  def init(): Unit = {
    modules.foreach(x => {
      x.init(anchor, serverContext)
      logger.info(s"loaded module: $x")
    })

    //publish events(CollectDataSourcesEvent, ...)
    eventSources.foreach(source => fireEvent(source.create()))
  }

  def destroy(): Unit = {
    modules.foreach(x => {
      x.destroy()
    })
  }

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
}