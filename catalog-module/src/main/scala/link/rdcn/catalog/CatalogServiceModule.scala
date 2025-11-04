package link.rdcn.catalog

import link.rdcn.server._
import link.rdcn.server.module.{ObjectHolder, RequireDataFrameProviderEvent}
import link.rdcn.struct.{DataFrameDocument, DataFrameStatistics, DataStreamSource, StructType}
import org.apache.jena.rdf.model.Model

/**
 * @Author renhao
 * @Description:
 * @Data 2025/10/31 18:55
 * @Modified By:
 */
class CatalogServiceModule(catalogService: CatalogService) extends DftpModule {

  override def init(anchor: Anchor, serverContext: ServerContext): Unit = {
    anchor.hook(new EventHandler {
      override def accepts(event: CrossModuleEvent): Boolean =
        event.isInstanceOf[RequireCatalogServiceEvent]

      override def doHandleEvent(event: CrossModuleEvent): Unit = {
        event match {
          case r: RequireCatalogServiceEvent => r.holder.set(catalogService)
          case _ =>
        }
      }
    })
  }

  override def destroy(): Unit = {}
}