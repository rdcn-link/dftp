package link.rdcn.dacp.cook

import link.rdcn.dacp.user.{PermissionService, RequirePermissionServiceEvent}
import link.rdcn.server.{Anchor, CrossModuleEvent, DftpModule, EventHandler, EventHub, EventSource, ServerContext}
import link.rdcn.server.exception.{DataFrameAccessDeniedException, DataFrameNotFoundException}
import link.rdcn.server.module.{DataFrameProviderService, TaskRunner, Workers, CollectDataFrameProviderEvent}
import link.rdcn.struct.DataFrame
import link.rdcn.user.UserPrincipal

/**
 * @Author renhao
 * @Description:
 * @Data 2025/11/14 09:47
 * @Modified By:
 */
class SecuredDataFrameProviderModule(dataFrameProvider: DataFrameProviderService) extends DftpModule {

  private val permissionHolder = new Workers[PermissionService]

  override def init(anchor: Anchor, serverContext: ServerContext): Unit = {
    anchor.hook(new EventHandler {
      override def accepts(event: CrossModuleEvent): Boolean =
        event.isInstanceOf[CollectDataFrameProviderEvent]

      override def doHandleEvent(event: CrossModuleEvent): Unit = {
        event match {
          case r: CollectDataFrameProviderEvent =>
            r.holder.add(
              new DataFrameProviderService {
                override def accepts(dataFrameUrl: String): Boolean =
                  dataFrameProvider.accepts(dataFrameUrl)

                override def getDataFrame(dataFrameUrl: String, userPrincipal: UserPrincipal)
                                         (implicit ctx: ServerContext): DataFrame = {
                  if (permissionHolder.work(new TaskRunner[PermissionService, Boolean] {
                    override def isReady(worker: PermissionService): Boolean = worker.accepts(userPrincipal)

                    override def executeWith(worker: PermissionService): Boolean = worker.checkPermission(userPrincipal, dataFrameUrl)

                    override def handleFailure(): Boolean = throw new DataFrameAccessDeniedException(dataFrameUrl)

                  })) {
                    dataFrameProvider.getDataFrame(dataFrameUrl, userPrincipal)
                  }
                  else {
                    throw new DataFrameAccessDeniedException(dataFrameUrl)
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