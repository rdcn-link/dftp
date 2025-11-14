package link.rdcn.dacp.cook

import link.rdcn.dacp.user.{PermissionService, RequirePermissionServiceEvent}
import link.rdcn.server.{Anchor, CrossModuleEvent, DftpModule, EventHandler, EventHub, EventSource, ServerContext}
import link.rdcn.server.exception.{DataFrameAccessDeniedException, DataFrameNotFoundException}
import link.rdcn.server.module.{DataFrameProviderService, ObjectHolder, RequireDataFrameProviderEvent}
import link.rdcn.struct.DataFrame
import link.rdcn.user.UserPrincipal

/**
 * @Author renhao
 * @Description:
 * @Data 2025/11/14 09:47
 * @Modified By:
 */
class SecuredDataFrameProviderModule(dataFrameProvider: DataFrameProviderService) extends DftpModule {

  private val permissionHolder = new ObjectHolder[PermissionService]

  override def init(anchor: Anchor, serverContext: ServerContext): Unit = {
    anchor.hook(new EventHandler {
      override def accepts(event: CrossModuleEvent): Boolean =
        event.isInstanceOf[RequireDataFrameProviderEvent]

      override def doHandleEvent(event: CrossModuleEvent): Unit = {
        event match {
          case r: RequireDataFrameProviderEvent =>
            r.holder.set(old => {
              new DataFrameProviderService {
                override def accepts(dataFrameUrl: String): Boolean =
                  dataFrameProvider.accepts(dataFrameUrl) || old !=null && old.accepts(dataFrameUrl)

                override def getDataFrame(dataFrameUrl: String, userPrincipal: UserPrincipal)
                                         (implicit ctx: ServerContext): DataFrame = {
                  if(permissionHolder.invoke(!_.checkPermission(userPrincipal, dataFrameUrl), false)){
                    throw new DataFrameAccessDeniedException(dataFrameUrl)
                  }
                  if(dataFrameProvider.accepts(dataFrameUrl))
                    dataFrameProvider.getDataFrame(dataFrameUrl, userPrincipal)
                  else if(old!=null && old.accepts(dataFrameUrl))
                    old.getDataFrame(dataFrameUrl, userPrincipal)
                  else throw new DataFrameNotFoundException(dataFrameUrl)
                }
              }
            })
        }
      }
    })

    anchor.hook(new EventSource {
      override def init(eventHub: EventHub): Unit = {
        eventHub.fireEvent(RequirePermissionServiceEvent(permissionHolder))
      }
    })
  }

  override def destroy(): Unit = {}
}