package link.rdcn.server.module

import link.rdcn.server.{DftpGetStreamRequest, _}
import link.rdcn.user.{AuthenticationMethod, Credentials, UserPrincipal}

class KernelModule extends DftpModule {
  private val authMethods = new Workers[AuthenticationMethod]
  private val parseMethods = new Workers[ParseRequestMethod]
  private val getMethods = new FilteredGetStreamMethods
  private val actionMethods = new Workers[ActionMethod]
  private val putMethods = new Workers[PutStreamMethod]

  def parseGetStreamRequest(token: Array[Byte], principal: UserPrincipal): DftpGetStreamRequest = {
    parseMethods.work(new TaskRunner[ParseRequestMethod, DftpGetStreamRequest] {

      override def acceptedBy(worker: ParseRequestMethod): Boolean = worker.accepts(token)

      override def executeWith(worker: ParseRequestMethod): DftpGetStreamRequest = worker.parse(token, principal)

      override def handleFailure(): DftpGetStreamRequest = {
        throw new Exception(s"unknown get stream request: $token") //FIXME: user defined exception
      }
    })
  }

  def getStream(request: DftpGetStreamRequest, response: DftpGetStreamResponse): Unit = {
    getMethods.handle(request, response)
  }

  def putStream(request: DftpPutStreamRequest, response: DftpPutStreamResponse): Unit = {
    putMethods.work(new TaskRunner[PutStreamMethod, Unit] {

      override def acceptedBy(worker: PutStreamMethod): Boolean = worker.accepts(request)

      override def executeWith(worker: PutStreamMethod): Unit = worker.doPutStream(request, response)

      override def handleFailure(): Unit = {
        response.sendError(500, s"method not implemented")
      }
    })
  }

  def doAction(request: DftpActionRequest, response: DftpActionResponse): Unit = {
    actionMethods.work(new TaskRunner[ActionMethod, Unit] {

      override def acceptedBy(worker: ActionMethod): Boolean = worker.accepts(request)

      override def executeWith(worker: ActionMethod): Unit = worker.doAction(request, response)

      override def handleFailure(): Unit = {
        response.sendError(404, s"unknown action: ${request.getActionName()}")
      }
    })
  }

  def authenticate(credentials: Credentials): UserPrincipal = {
    authMethods.work(new TaskRunner[AuthenticationMethod, UserPrincipal] {

      override def acceptedBy(worker: AuthenticationMethod): Boolean = worker.accepts(credentials)

      override def executeWith(worker: AuthenticationMethod): UserPrincipal = worker.authenticate(credentials)

      override def handleFailure(): UserPrincipal = {
        throw new Exception(s"unknown authentication request: ${credentials}") //FIXME: user defined exception
      }
    })
  }

  override def init(anchor: Anchor, serverContext: ServerContext): Unit = {
    anchor.hook(new EventSource {
      override def init(eventHub: EventHub): Unit = {
        eventHub.fireEvent(CollectAuthenticationMethodEvent(authMethods))
        eventHub.fireEvent(CollectParseRequestMethodEvent(parseMethods))
        eventHub.fireEvent(CollectActionMethodEvent(actionMethods))
        eventHub.fireEvent(CollectPutStreamMethodEvent(putMethods))
        eventHub.fireEvent(CollectGetStreamMethodEvent(getMethods))
      }
    })
  }

  override def destroy(): Unit = {}
}

trait ActionMethod {
  def accepts(request: DftpActionRequest): Boolean
  def doAction(request: DftpActionRequest, response: DftpActionResponse): Unit
}

trait ParseRequestMethod {
  def accepts(token: Array[Byte]): Boolean
  def parse(token: Array[Byte], principal: UserPrincipal): DftpGetStreamRequest
}

trait AccessLogger {
  def accepts(request: DftpRequest): Boolean
  def doLog(request: DftpRequest, response: DftpResponse): Unit
}

trait GetStreamMethod {
  def accepts(request: DftpGetStreamRequest): Boolean
  def doGetStream(request: DftpGetStreamRequest, response: DftpGetStreamResponse): Unit
}

trait PutStreamMethod {
  def accepts(request: DftpPutStreamRequest): Boolean
  def doPutStream(request: DftpPutStreamRequest, response: DftpPutStreamResponse): Unit
}

case class CollectAuthenticationMethodEvent(collector: Workers[AuthenticationMethod]) extends CrossModuleEvent {
  def collect = collector.add(_)
}

case class CollectParseRequestMethodEvent(collector: Workers[ParseRequestMethod]) extends CrossModuleEvent {
  def collect = collector.add(_)
}

case class CollectActionMethodEvent(collector: Workers[ActionMethod]) extends CrossModuleEvent {
  def collect = collector.add(_)
}

case class CollectPutStreamMethodEvent(collector: Workers[PutStreamMethod]) extends CrossModuleEvent {
  def collect = collector.add(_)
}

case class CollectGetStreamMethodEvent(collector: FilteredGetStreamMethods) extends CrossModuleEvent {
  def collect = collector.addMethod(_)

  def addFilter = collector.addFilter(_, _)
}

trait GetStreamFilter {
  def doFilter(request: DftpGetStreamRequest, response: DftpGetStreamResponse, chain: GetStreamFilterChain): Unit
}

trait GetStreamFilterChain {
  def doFilter(request: DftpGetStreamRequest, response: DftpGetStreamResponse): Unit
}

class FilteredGetStreamMethods {
  private val _workers = new Workers[GetStreamMethod]()
  private val _filters = new Filters[GetStreamFilter]()

  def addMethod(method: GetStreamMethod) = _workers.add(method)

  def addFilter(order: Int, filter: GetStreamFilter) = _filters.add(order, filter)

  def handle(request: DftpGetStreamRequest, response: DftpGetStreamResponse): Unit = {
    _filters.doFilter(
      new FilterRunner[GetStreamFilter, (DftpGetStreamRequest, DftpGetStreamResponse), Unit] {
        override def doFilter(filter: GetStreamFilter, args: (DftpGetStreamRequest, DftpGetStreamResponse), chain: FilterChain[(DftpGetStreamRequest, DftpGetStreamResponse), Unit]): Unit = {
          filter.doFilter(args._1, args._2, new GetStreamFilterChain {
            override def doFilter(request: DftpGetStreamRequest, response: DftpGetStreamResponse): Unit = {
              chain.doFilter(request -> response)
            }
          })
        }

        override def startChain(chain: FilterChain[(DftpGetStreamRequest, DftpGetStreamResponse), Unit]): Unit = {
          chain.doFilter(request -> response)
        }
      }, { args: (DftpGetStreamRequest, DftpGetStreamResponse) =>
        _workers.work(
          new TaskRunner[GetStreamMethod, Unit] {

            override def acceptedBy(worker: GetStreamMethod): Boolean = worker.accepts(args._1)

            override def executeWith(worker: GetStreamMethod): Unit = worker.doGetStream(args._1, args._2)

            override def handleFailure(): Unit = {
              response.sendError(404, s"requested resource not found")
            }
          })
      })
  }
}