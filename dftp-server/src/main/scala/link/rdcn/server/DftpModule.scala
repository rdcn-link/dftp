package link.rdcn.server

import link.rdcn.Logging
import link.rdcn.server.module.KernelModule
import link.rdcn.struct.DataFrame
import link.rdcn.user.{AuthenticationService, Credentials, UserPrincipal}

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

trait EventHub {
  def fireEvent(event: CrossModuleEvent): Unit
}

trait Anchor {
  def hook(service: EventHandler): Unit
  def hook(service: EventSource): Unit
}

trait CrossModuleEvent {
}

trait EventHandler {
  def accepts(event: CrossModuleEvent): Boolean
  def doHandleEvent(event: CrossModuleEvent): Unit
}

trait EventSource {
  def init(eventHub: EventHub): Unit
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

  def addModule(module: DftpModule): Modules = {
    modules += module
    this
  }

  private val eventHandlers = ArrayBuffer[EventHandler]()
  private val eventSources = ArrayBuffer[EventSource]()

  private val anchor = new Anchor {
    override def hook(service: EventHandler): Unit = {
      eventHandlers += service
    }

    override def hook(service: EventSource): Unit = {
      eventSources += service
    }
  }

  private val eventHub = new EventHub {
    override def fireEvent(event: CrossModuleEvent): Unit = eventHandlers.filter(_.accepts(event)).foreach(_.doHandleEvent(event))
  }

  def init(): Unit = {
    //load all configured modules
    modules.foreach(x => {
      x.init(anchor, serverContext)
      logger.info(s"loaded module: $x")
    })

    eventSources.foreach(x => {
      x.init(eventHub)
    })
  }

  def destroy(): Unit = {
    modules.foreach(x => {
      x.destroy()
    })
  }
}