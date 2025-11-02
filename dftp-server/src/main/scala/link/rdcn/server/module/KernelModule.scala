package link.rdcn.server.module

import link.rdcn.server.{AccessLogger, ActionHandler, Anchor, CrossModuleEvent, DftpActionRequest, DftpActionResponse, DftpGetStreamRequest, DftpGetStreamResponse, DftpModule, DftpPutStreamRequest, DftpPutStreamResponse, DftpRequest, DftpResponse, EventHub, EventSource, GetStreamHandler, GetStreamRequestParser, PutStreamHandler, ServerContext}
import link.rdcn.struct.DataFrame
import link.rdcn.user.{AuthenticationService, Credentials, UserPrincipal, UserPrincipalWithCredentials}

import scala.collection.mutable.ArrayBuffer

class KernelModule extends DftpModule {
  private val authHolder = new ObjectHolder[AuthenticationService]
  private val parseHolder = new ObjectHolder[GetStreamRequestParser]
  private val getHolder = new ObjectHolder[GetStreamHandler]
  private val actionHolder = new ObjectHolder[ActionHandler]
  private val putHolder = new ObjectHolder[PutStreamHandler]
  private val loggerHolder = new ObjectHolder[AccessLogger]

  def parseGetStreamRequest(token: Array[Byte], principal: UserPrincipal): DftpGetStreamRequest = {
    parseHolder.invoke(_.parse(token, principal), {
      throw new Exception(s"value not set: GetStreamRequestParser")
    })
  }

  def getStream(request: DftpGetStreamRequest, response: DftpGetStreamResponse): Unit = {
    getHolder.invoke(_.doGetStream(request, response), {
      response.sendError(404, s"requested resource not found") //FIXME
    })
  }

  def putStream(request: DftpPutStreamRequest, response: DftpPutStreamResponse): Unit = {
    putHolder.invoke(_.doPutStream(request, response), {
      response.sendError(500, s"method not implemented") //FIXME
    })
  }

  def doAction(request: DftpActionRequest, response: DftpActionResponse): Unit = {
    actionHolder.invoke(_.doAction(request, response), {
      response.sendError(404, s"unknown action: ${request.getActionName()}") //FIXME
    })
  }

  def logAccess(request: DftpRequest, response: DftpResponse): Unit = {
    loggerHolder.invoke(_.doLog(request, response), {
      //log nothing
    })
  }

  def authenticate(credentials: Credentials): UserPrincipal = {
    authHolder.invoke(_.authenticate(credentials), {
      UserPrincipalWithCredentials(credentials) //FIXME
    })
  }

  override def init(anchor: Anchor, serverContext: ServerContext): Unit = {
    anchor.hook(new EventSource {
      override def init(eventHub: EventHub): Unit = {
        eventHub.fireEvent(new RequireAuthenticatorEvent(authHolder))
        eventHub.fireEvent(new RequireAccessLoggerEvent(loggerHolder))
        eventHub.fireEvent(new RequireGetStreamRequestParserEvent(parseHolder))
        eventHub.fireEvent(new RequireActionHandlerEvent(actionHolder))
        eventHub.fireEvent(new RequirePutStreamHandlerEvent(putHolder))
        eventHub.fireEvent(new RequireGetStreamHandlerEvent(getHolder))
      }
    })
  }

  override def destroy(): Unit = {
  }
}

class ObjectHolder[T] {
  private var _object: T = _

  def set(v: T): Unit = _object = v

  def invoke[Y](run: (T) => Y, onNull: => Y): Y = {
    if (_object == null) {
      onNull
    }
    else {
      run(_object)
    }
  }

  def set(fn: (T) => T): Unit = {
    _object = fn(_object)
  }
}

class RequireAuthenticatorEvent(val holder: ObjectHolder[AuthenticationService]) extends CrossModuleEvent {
}

class RequireAccessLoggerEvent(val holder: ObjectHolder[AccessLogger]) extends CrossModuleEvent {
}

class RequireGetStreamRequestParserEvent(val holder: ObjectHolder[GetStreamRequestParser]) extends CrossModuleEvent {
}

class RequireActionHandlerEvent(val holder: ObjectHolder[ActionHandler]) extends CrossModuleEvent {

}

class RequirePutStreamHandlerEvent(val holder: ObjectHolder[PutStreamHandler]) extends CrossModuleEvent {

}

class RequireGetStreamHandlerEvent(val holder: ObjectHolder[GetStreamHandler]) extends CrossModuleEvent {

}