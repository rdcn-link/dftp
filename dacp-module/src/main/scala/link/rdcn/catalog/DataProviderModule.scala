package link.rdcn.catalog

import link.rdcn.server._
import link.rdcn.server.module.RequireDataFrameProviderEvent
import link.rdcn.user.{CompositeAuthProvider, RequireAuthProviderEvent}

/**
 * @Author renhao
 * @Description:
 * @Data 2025/10/31 18:55
 * @Modified By:
 */
class DataProviderModule(dataProvider: DataProvider) extends DftpModule {

  private val authProviderHub = new CompositeAuthProvider

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

    anchor.hook(new EventSource {
      override def init(eventHub: EventHub): Unit = {
        eventHub.fireEvent(RequireAuthProviderEvent(authProviderHub))
        dataProvider.add(authProviderHub)
      }
    })
  }

  override def destroy(): Unit = {
  }
}
