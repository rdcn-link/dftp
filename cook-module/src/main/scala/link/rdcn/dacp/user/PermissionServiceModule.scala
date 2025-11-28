package link.rdcn.dacp.user

import link.rdcn.server.module.{Workers, CollectAuthenticationMethodEvent}
import link.rdcn.server.{Anchor, CrossModuleEvent, DftpModule, EventHandler, ServerContext}
import link.rdcn.user.UserPrincipal

class PermissionServiceModule(permissionService: PermissionService) extends DftpModule {

  override def init(anchor: Anchor, serverContext: ServerContext): Unit =
    anchor.hook(new EventHandler {
      override def accepts(event: CrossModuleEvent): Boolean =
        event.isInstanceOf[RequirePermissionServiceEvent]

      override def doHandleEvent(event: CrossModuleEvent): Unit = {
        event match {
          case require: RequirePermissionServiceEvent =>
            require.holder.add(
              new PermissionService {
                override def accepts(user: UserPrincipal): Boolean =
                  permissionService.accepts(user)

                override def checkPermission(user: UserPrincipal, dataFrameName: String, opList: List[DataOperationType]): Boolean =
                  permissionService.checkPermission(user, dataFrameName, opList)
              }
            )
          case _ =>
        }
      }
    })

  override def destroy(): Unit = {}
}

case class RequirePermissionServiceEvent(holder: Workers[PermissionService]) extends CrossModuleEvent

