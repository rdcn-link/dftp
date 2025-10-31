package link.rdcn.catalog

import link.rdcn.catalog.CatalogFormatter.{getDataFrameDocumentJsonString, getDataFrameStatisticsString}
import link.rdcn.server._
import link.rdcn.server.module.RequiresActionHandler
import link.rdcn.struct.ValueType.StringType
import link.rdcn.struct.{DataStreamSource, Row, StructType}
import link.rdcn.util.DataUtils
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.json.{JSONArray, JSONObject}

import java.io.StringWriter


/**
 * @Author renhao
 * @Description:
 * @Data 2025/10/29 21:46
 * @Modified By:
 */
class DacpCatalogModule(dataProvider: DataProvider) extends DftpModule {

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
            dataProvider.getDataSetMetaData(name.replaceFirst(prefix, ""), model)
            val writer = new StringWriter();
            model.write(writer, "RDF/XML");
            response.sendData(writer.toString.getBytes("UTF-8"))
          case name if name.startsWith("/getDataFrameMetaData/") =>
            val model: Model = ModelFactory.createDefaultModel
            val prefix: String = "/getDataFrameMetaData/"
            dataProvider.getDataFrameMetaData(name.replaceFirst(prefix, ""), model)
            val writer = new StringWriter();
            model.write(writer, "RDF/XML");
            response.sendData(writer.toString.getBytes("UTF-8"))
          case name if name.startsWith("/getDocument/") =>
            val document = dataProvider.getDocument(name.stripPrefix("/getDocument/"))
            val schema = dataProvider.getSchema(name.stripPrefix("/getDocument/"))
            response.sendData(getDataFrameDocumentJsonString(document, schema).getBytes("UTF-8"))
          case name if name.startsWith("/getStatistics/") =>
            val statistics = dataProvider.getStatistics(name.stripPrefix("/getStatistics/"))
            response.sendData(getDataFrameStatisticsString(statistics).getBytes("UTF-8"))
          case name if name.startsWith("getDataFrameSize") =>
            val prefix: String = "/getDataFrameSize/"
            response.sendData(dataProvider.getStatistics(name.stripPrefix(prefix)).rowCount.toString.getBytes("UTF-8"))
          case name if name.startsWith("/getSchema") =>
            response.sendData(dataProvider.getSchema(name.stripPrefix("/getSchema/"))
              .getOrElse(StructType.empty)
              .toString.getBytes("UTF-8"))
          case name if name.startsWith("/getDataFrameTitle") =>
            val dfName = name.stripPrefix("/getDataFrameTitle/")
            response.sendData(dataProvider.getDataFrameTitle(dfName)
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
          case r: RequiresActionHandler => true
          case _ => false
        }
      }

      override def doHandleEvent(event: CrossModuleEvent): Unit = {
        event match {
          case r: RequiresActionHandler => r.add(actionMethodService)
          case _ =>
        }
      }
    })
  }

  override def destroy(): Unit = {
  }
}