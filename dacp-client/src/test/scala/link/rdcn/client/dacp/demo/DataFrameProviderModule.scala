package link.rdcn.client.dacp.demo

import link.rdcn.server._
import link.rdcn.server.module.{CollectDataFrameProviderEvent, DataFrameProviderService}
import link.rdcn.struct.DataFrame
import link.rdcn.user.UserPrincipal

/**
 * @Author renhao
 * @Description:
 * @Data 2025/10/31 18:43
 * @Modified By:
 */
class DataFrameProviderModule(dataFrameProvider: DataFrameProviderService) extends DftpModule {

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
                  dataFrameProvider.getDataFrame(dataFrameUrl, userPrincipal)
                }
              }
            )
        }
      }
    })
  }

  override def destroy(): Unit = {}
}