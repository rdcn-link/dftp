package link.rdcn.dacp.cook

import link.rdcn.dacp.user.{KeyPairCredentials, KeyPairUserPrincipal}
import link.rdcn.server._
import link.rdcn.server.module.CollectAuthenticationMethodEvent
import link.rdcn.user.{AuthenticationMethod, Credentials, TokenAuth, UserPrincipal}

/**
 * @Author renhao
 * @Description:
 * @Data 2025/11/28 17:14
 * @Modified By:
 */
class KeyPairAuthModule extends DftpModule {

  override def init(anchor: Anchor, serverContext: ServerContext): Unit = {
    anchor.hook(new EventHandler {
      override def accepts(event: CrossModuleEvent): Boolean =
        event.isInstanceOf[CollectAuthenticationMethodEvent]

      override def doHandleEvent(event: CrossModuleEvent): Unit = {
        event match {
          case require: CollectAuthenticationMethodEvent =>
            require.collector.add(new AuthenticationMethod {
              override def accepts(credentials: Credentials): Boolean = {
                credentials match {
                  case _: KeyPairCredentials => true
                  case _ => false
                }
              }

              override def authenticate(credentials: Credentials): UserPrincipal = {
                credentials match {
                  case sig: KeyPairCredentials =>
                    KeyPairUserPrincipal(serverContext.getPublicKeyMap.get(sig.serverId), sig.serverId, sig.nonce, sig.issueTime, sig.validTo, sig.signature)
                }
              }
            })
          case _ =>
        }
      }
    })
  }

  override def destroy(): Unit = {}
}
