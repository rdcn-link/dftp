package link.rdcn.server.module

import link.rdcn.server.{Anchor, CrossModuleEvent, DftpModule, EventHandler, ServerContext}
import link.rdcn.user.{AuthenticationService, Credentials, UserPrincipal}

class AuthModule(authenticationService: AuthenticationService) extends DftpModule {

  override def init(anchor: Anchor, serverContext: ServerContext): Unit =
    anchor.hook(new EventHandler {
      override def accepts(event: CrossModuleEvent): Boolean =
        event.isInstanceOf[RequireAuthenticatorEvent]

      override def doHandleEvent(event: CrossModuleEvent): Unit = {
        event match {
          case require: RequireAuthenticatorEvent =>
            require.holder.set(old =>
              new AuthenticationService {

                override def accepts(credentials: Credentials): Boolean = {
                  authenticationService.accepts(credentials) || (old != null && old.accepts(credentials))
                }

                override def authenticate(credentials: Credentials): UserPrincipal = {
                  if (authenticationService.accepts(credentials))
                    authenticationService.authenticate(credentials)
                  else if (old != null && old.accepts(credentials))
                    old.authenticate(credentials)
                  else
                    throw new Exception(s"unrecognized credentials: ${credentials}")
                }
              })
        }
      }
    })

  override def destroy(): Unit = {}
}