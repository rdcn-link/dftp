package link.rdcn.server.module

import link.rdcn.server.exception.AuthenticationFailedException
import link.rdcn.server.{Anchor, CrossModuleEvent, DftpModule, EventHandler, ServerContext}
import link.rdcn.user.{AuthenticationMethod, Credentials, UserPasswordAuthService, UserPrincipal, UserPrincipalWithCredentials, UsernamePassword}

class UserPasswordAuthModule(userPasswordAuthService: UserPasswordAuthService) extends DftpModule {

  override def init(anchor: Anchor, serverContext: ServerContext): Unit =
    anchor.hook(new EventHandler {
      override def accepts(event: CrossModuleEvent): Boolean =
        event.isInstanceOf[CollectAuthenticationMethodEvent]

      override def doHandleEvent(event: CrossModuleEvent): Unit = {
        event match {
          case require: CollectAuthenticationMethodEvent =>
            require.collector.add(
              new AuthenticationMethod {
                override def accepts(credentials: Credentials): Boolean = {
                  credentials match {
                    case u: UsernamePassword =>
                      userPasswordAuthService.accepts(u)

                    case Credentials.ANONYMOUS => true
                  }
                }

                override def authenticate(credentials: Credentials): UserPrincipal = {
                  credentials match {
                    case u: UsernamePassword =>
                        userPasswordAuthService.authenticate(credentials)

                    case Credentials.ANONYMOUS => UserPrincipalWithCredentials(Credentials.ANONYMOUS)
                  }
                }
              }
            )
        }
      }
    })

  override def destroy(): Unit = {}
}

class DefaultUserPasswordAuthService extends UserPasswordAuthService {

  override def accepts(credentials: Credentials): Boolean = credentials.isInstanceOf[UsernamePassword]

  override def authenticate(credentials: Credentials): UserPrincipal =
    UserPrincipalWithCredentials(credentials)
}