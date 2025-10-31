package link.rdcn.catalog

import link.rdcn.server.module.RequireDataFrameProviderEvent
import link.rdcn.server.{Anchor, CrossModuleEvent, DftpModule, EventHandler, ServerContext}

/**
 * @Author renhao
 * @Description:
 * @Data 2025/10/31 18:55
 * @Modified By:
 */
class DataProviderModule(dataProvider: DataProvider) extends DftpModule {

  override def init(anchor: Anchor, serverContext: ServerContext): Unit = {
    anchor.hook(new EventHandler {
      override def accepts(event: CrossModuleEvent): Boolean =
        event.isInstanceOf[RequireDataProviderEvent] || event.isInstanceOf[RequireDataFrameProviderEvent]

      override def doHandleEvent(event: CrossModuleEvent): Unit = {
        event match {
          case r: RequireDataProviderEvent => r.add(dataProvider)
          case r: RequireDataFrameProviderEvent => r.add(dataProvider)
          case _ =>
        }
      }
    })
  }

  override def destroy(): Unit = {
  }
}
