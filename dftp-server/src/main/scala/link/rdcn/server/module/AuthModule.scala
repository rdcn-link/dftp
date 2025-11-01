package link.rdcn.server.module

import link.rdcn.server.{Anchor, CrossModuleEvent, DftpModule, EventHandler, ServerContext}
import link.rdcn.user.AuthenticationService

class AuthModule(authenticationService: AuthenticationService) extends DftpModule {

  override def init(anchor: Anchor, serverContext: ServerContext): Unit =
    anchor.hook(new EventHandler {
      override def accepts(event: CrossModuleEvent): Boolean =
        event.isInstanceOf[RequireAuthenticatorEvent]

      override def doHandleEvent(event: CrossModuleEvent): Unit = {
        event match {
          case require: RequireAuthenticatorEvent =>
            require.add(authenticationService)
        }
      }
    })

  override def destroy(): Unit = {}
}