package link.rdcn.user

import link.rdcn.server.module.RequireAuthenticatorEvent
import link.rdcn.server.{Anchor, CrossModuleEvent, DftpModule, EventHandler, ServerContext}

import scala.collection.mutable.ArrayBuffer

class AuthProviderModule(authProvider: AuthProvider) extends DftpModule {

  override def init(anchor: Anchor, serverContext: ServerContext): Unit =
    anchor.hook(new EventHandler {
    override def accepts(event: CrossModuleEvent): Boolean =
      event.isInstanceOf[RequireAuthProviderEvent] ||
        event.isInstanceOf[RequireAuthenticatorEvent]

    override def doHandleEvent(event: CrossModuleEvent): Unit = {
      event match {
        case require: RequireAuthProviderEvent =>
          require.add(authProvider)
        case require: RequireAuthenticatorEvent =>
          require.add(authProvider)
        case _ =>
      }
    }
  })

  override def destroy(): Unit = {}
}

case class RequireAuthProviderEvent(composite: CompositeAuthProvider) extends CrossModuleEvent {
  def add(service: AuthProvider) = composite.add(service)
}

class CompositeAuthProvider extends AuthProvider {
  val services = ArrayBuffer[AuthProvider]()

  def add(service: AuthProvider): Unit = services += service

  override def authenticate(credentials: Credentials): UserPrincipal = {
    val request = new AuthenticationRequest {
      override def getCredentials: Credentials = credentials
    }
    services.find(_.accepts(request)).map(_.authenticate(credentials)).find(_ != null).orNull
  }

  def checkPermission(user: UserPrincipal,
                      dataFrameName: String,
                      opList: List[DataOperationType] = List.empty): Boolean = {
    val request = new AuthProviderRequest {
      override def getCredentials: Credentials = null

      override def getUserPrincipal(): UserPrincipal = user
    }
    services.find(_.accepts(request)).exists(_.checkPermission(user, dataFrameName, opList))
  }

  override def accepts(request: AuthenticationRequest): Boolean = services.exists(_.accepts(request))
}

