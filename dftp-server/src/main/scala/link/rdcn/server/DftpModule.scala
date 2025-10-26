package link.rdcn.server

import link.rdcn.Logging
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

trait Anchor {
  def hook(service: AuthenticationService): Unit
  def hook(service: ActionMethodService): Unit
  def hook(service: GetMethodService): Unit
  def hook(service: PutMethodService): Unit
  def hook(service: LogService): Unit
}

trait ActionMethodService {
  def accepts(request: DftpActionRequest): Boolean
  def doAction(request: DftpActionRequest, response: DftpActionResponse): Unit
}

trait LogService {
  def accepts(request: DftpRequest): Boolean
  def doLog(request: DftpRequest, response: DftpResponse): Unit
}

trait GetMethodService {
  def accepts(request: DftpGetRequest): Boolean
  def doGet(request: DftpGetRequest, response: DftpGetResponse): Unit
}

trait PutMethodService {
  def accepts(request: DftpPutRequest): Boolean
  def doPut(request: DftpPutRequest, response: DftpPutResponse): Unit
}

class Modules extends Logging {
  val modules = ArrayBuffer[DftpModule]()

  val authMethod = new AuthenticationService {
    val services = ArrayBuffer[AuthenticationService]()

    def add(authenticator: AuthenticationService): Unit = services += authenticator

    override def authenticate(credentials: Credentials): UserPrincipal = {
      services.find(_.accepts(credentials)).map(_.authenticate(credentials)).find(_ != null).head
    }

    override def accepts(credentials: Credentials): Boolean = services.exists(_.accepts(credentials))
  }

  val getMethod = new GetMethodService {
    val services = ArrayBuffer[GetMethodService]()

    def add(service: GetMethodService): Unit = services += service

    override def accepts(request: DftpGetRequest): Boolean = services.exists(_.accepts(request))

    override def doGet(request: DftpGetRequest, response: DftpGetResponse): Unit = {
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

  val actionMethod = new ActionMethodService {
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

  val putMethod = new PutMethodService {
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

  val logMethod = new LogService {
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
  }

  def init(serverContext: ServerContext): Unit = {
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

  def doGet(request: DftpGetRequest, response: DftpGetResponse): Unit = {
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