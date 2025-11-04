package link.rdcn.user

import link.rdcn.server.module.{ObjectHolder, RequireAuthenticatorEvent}
import link.rdcn.server.{Anchor, CrossModuleEvent, DftpModule, EventHandler, ServerContext}

class PermissionServiceModule(permissionService: PermissionService) extends DftpModule {

  override def init(anchor: Anchor, serverContext: ServerContext): Unit =
    anchor.hook(new EventHandler {
    override def accepts(event: CrossModuleEvent): Boolean =
      event.isInstanceOf[RequirePermissionServiceEvent]

    override def doHandleEvent(event: CrossModuleEvent): Unit = {
      event match {
        case require: RequirePermissionServiceEvent =>
          require.holder.set(old => {
            new PermissionService {
              override def accepts(user: UserPrincipal): Boolean =
                old!=null && old.accepts(user) || permissionService.accepts(user)

              override def checkPermission(user: UserPrincipal, dataFrameName: String, opList: List[DataOperationType]): Boolean =
                if(permissionService.accepts(user)) permissionService.checkPermission(user, dataFrameName, opList)
                else if(old!=null && old.accepts(user)) old.checkPermission(user, dataFrameName, opList) else false
            }
          })
        case _ =>
      }
    }
  })

  override def destroy(): Unit = {}
}

case class RequirePermissionServiceEvent(holder: ObjectHolder[PermissionService]) extends CrossModuleEvent

