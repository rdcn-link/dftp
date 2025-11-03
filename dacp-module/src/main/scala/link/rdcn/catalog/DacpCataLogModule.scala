package link.rdcn.catalog

import link.rdcn.catalog.CatalogFormatter.{getDataFrameDocumentJsonString, getDataFrameStatisticsString}
import link.rdcn.server._
import link.rdcn.server.module.RequireActionHandlerEvent
import link.rdcn.struct.StructType
import org.apache.jena.rdf.model.{Model, ModelFactory}

import java.io.StringWriter

/**
 * @Author renhao
 * @Description:
 * @Data 2025/10/29 21:46
 * @Modified By:
 */

case class RequireDataProviderEvent(composite: CompositeDataProvider) extends CrossModuleEvent{
  def add(dataProvider: DataProvider) = composite.add(dataProvider)
}

class DacpCatalogModule() extends DftpModule {

  private val dataProviderHub = new CompositeDataProvider

  private val actionMethodService = new ActionHandler {

    override def accepts(request: DftpActionRequest): Boolean = true

    override def doAction(request: DftpActionRequest, response: DftpActionResponse): Unit = {
      try{
        val actionName = request.getActionName()
        val parameter = request.getParameter()
        actionName match {
          case name if name.startsWith("/getDataSetMetaData/") =>
            val model: Model = ModelFactory.createDefaultModel
            val prefix: String = "/getDataSetMetaData/"
            dataProviderHub.getDataSetMetaData(name.replaceFirst(prefix, ""), model)
            val writer = new StringWriter();
            model.write(writer, "RDF/XML");
            response.sendData(writer.toString.getBytes("UTF-8"))
          case name if name.startsWith("/getDataFrameMetaData/") =>
            val model: Model = ModelFactory.createDefaultModel
            val prefix: String = "/getDataFrameMetaData/"
            dataProviderHub.getDataFrameMetaData(name.replaceFirst(prefix, ""), model)
            val writer = new StringWriter();
            model.write(writer, "RDF/XML");
            response.sendData(writer.toString.getBytes("UTF-8"))
          case name if name.startsWith("/getDocument/") =>
            val document = dataProviderHub.getDocument(name.stripPrefix("/getDocument/"))
            val schema = dataProviderHub.getSchema(name.stripPrefix("/getDocument/"))
            response.sendData(getDataFrameDocumentJsonString(document, schema).getBytes("UTF-8"))
          case name if name.startsWith("/getStatistics/") =>
            val statistics = dataProviderHub.getStatistics(name.stripPrefix("/getStatistics/"))
            response.sendData(getDataFrameStatisticsString(statistics).getBytes("UTF-8"))
          case name if name.startsWith("getDataFrameSize") =>
            val prefix: String = "/getDataFrameSize/"
            response.sendData(dataProviderHub.getStatistics(name.stripPrefix(prefix)).rowCount.toString.getBytes("UTF-8"))
          case name if name.startsWith("/getSchema") =>
            response.sendData(dataProviderHub.getSchema(name.stripPrefix("/getSchema/"))
              .getOrElse(StructType.empty)
              .toString.getBytes("UTF-8"))
          case name if name.startsWith("/getDataFrameTitle") =>
            val dfName = name.stripPrefix("/getDataFrameTitle/")
            response.sendData(dataProviderHub.getDataFrameTitle(dfName)
              .getOrElse(dfName).getBytes("UTF-8"))
          case otherPath => response.sendError(400, s"Action $otherPath Invalid")
        }
      }catch {
        case e: Exception =>
          response.sendError(500, e.getMessage)
          throw e
      }

    }
  }

  override def init(anchor: Anchor, serverContext: ServerContext): Unit = {
    anchor.hook(new EventHandler {
      override def accepts(event: CrossModuleEvent): Boolean = {
        event match {
          case r: RequireActionHandlerEvent => true
          case _ => false
        }
      }

      override def doHandleEvent(event: CrossModuleEvent): Unit = {
        event match {
          case r: RequireActionHandlerEvent => r.holder.set(actionMethodService)
          case _ =>
        }
      }
    })
    anchor.hook(new EventSource {
      override def init(eventHub: EventHub): Unit =
        eventHub.fireEvent(RequireDataProviderEvent(dataProviderHub))
    })
  }

  override def destroy(): Unit = {
  }
}