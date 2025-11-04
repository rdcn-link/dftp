package link.rdcn.server.module

import link.rdcn.server.{Anchor, CrossModuleEvent, DftpModule, EventHandler, ServerContext}
import link.rdcn.user.{AuthenticationService, Credentials, UserPasswordAuthService, UserPrincipal, UserPrincipalWithCredentials, UsernamePassword}

class UserPasswordAuthModule(userPasswordAuthService: UserPasswordAuthService) extends DftpModule {

  override def init(anchor: Anchor, serverContext: ServerContext): Unit =
    anchor.hook(new EventHandler {
      override def accepts(event: CrossModuleEvent): Boolean =
        event.isInstanceOf[RequireAuthenticatorEvent]

      override def doHandleEvent(event: CrossModuleEvent): Unit = {
        event match {
          case require: RequireAuthenticatorEvent =>
            require.holder.set(old =>
              new AuthenticationService {
                override type C = Credentials

                override def accepts(credentials: Credentials): Boolean = {
                  credentials match {
                    case u: UsernamePassword =>
                      userPasswordAuthService.accepts(u) || {
                        if(old!=null && old.isInstanceOf[UserPasswordAuthService])
                          old.accepts(credentials.asInstanceOf[old.C]) else false
                      }
                    case Credentials.ANONYMOUS => true
                    case _ => Option(old).exists(authService =>
                      authService.accepts(credentials.asInstanceOf[authService.C]))
                  }
                }

                override def authenticate(credentials: Credentials): UserPrincipal = {
                  credentials match {
                    case u: UsernamePassword =>
                      if(userPasswordAuthService.accepts(u)) userPasswordAuthService.authenticate(u)
                      else if(old!=null && old.isInstanceOf[UserPasswordAuthService])
                        old.authenticate(credentials.asInstanceOf[old.C])
                      else throw new SecurityException(s"$credentials Authentication failed")
                    case Credentials.ANONYMOUS => UserPrincipalWithCredentials(Credentials.ANONYMOUS)
                    case _ => if(old!=null && old.accepts(credentials.asInstanceOf[old.C]))
                      old.authenticate(credentials.asInstanceOf[old.C])
                      else throw new SecurityException(s"$credentials Authentication failed")
                  }
                }
              }
            )
        }
      }
    })

  override def destroy(): Unit = {}
}