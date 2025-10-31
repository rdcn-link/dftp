package link.rdcn.cook

import link.rdcn.server._
import link.rdcn.server.module.{DataFrameProvider, RequireDataFrameProviderEvent}

/**
 * @Author renhao
 * @Description:
 * @Data 2025/10/31 18:43
 * @Modified By:
 */
class DataFrameProviderModule(dataFrameProvider: DataFrameProvider) extends DftpModule {

  override def init(anchor: Anchor, serverContext: ServerContext): Unit = {
    anchor.hook(new EventHandler {
      override def accepts(event: CrossModuleEvent): Boolean =
        event.isInstanceOf[RequireDataFrameProviderEvent]

      override def doHandleEvent(event: CrossModuleEvent): Unit = {
        event match {
          case r: RequireDataFrameProviderEvent => r.add(dataFrameProvider)
        }
      }
    })
  }

  override def destroy(): Unit = {}
}
