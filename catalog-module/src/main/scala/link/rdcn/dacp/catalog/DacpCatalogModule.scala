package link.rdcn.dacp.catalog

import CatalogFormatter.{getDataFrameDocumentJsonString, getDataFrameStatisticsString}
import link.rdcn.server._
import link.rdcn.server.module.{ActionMethod, CollectActionMethodEvent, CollectGetStreamMethodEvent, GetStreamMethod, Workers}
import link.rdcn.struct.StructType
import org.apache.jena.rdf.model.{Model, ModelFactory}

import java.io.StringWriter

/**
 * @Author renhao
 * @Description:
 * @Data 2025/10/29 21:46
 * @Modified By:
 */
case class RequireCatalogServiceEvent(holder: Workers[CatalogService]) extends CrossModuleEvent

class DacpCatalogModule() extends DftpModule {

  private val catalogServiceHolder = new Workers[CatalogService]


  private val actionMethodService = new ActionMethod {

    override def accepts(request: DftpActionRequest): Boolean = true

    override def doAction(request: DftpActionRequest, response: DftpActionResponse): Unit = {
      try {
        val actionName = request.getActionName()
        val parameter = request.getParameter()
        actionName match {
          case name if name.startsWith("/getDataSetMetaData/") =>
            val model: Model = ModelFactory.createDefaultModel
            val prefix: String = "/getDataSetMetaData/"
            catalogServiceHolder.invoke(_.getDataSetMetaData(name.stripPrefix(prefix), model),
              response.sendError(404, s"unknown action: ${request.getActionName()}"))
            val writer = new StringWriter();
            model.write(writer, "RDF/XML");
            response.sendData(writer.toString.getBytes("UTF-8"))
          case name if name.startsWith("/getDataFrameMetaData/") =>
            val model: Model = ModelFactory.createDefaultModel
            val prefix: String = "/getDataFrameMetaData/"
            catalogServiceHolder.invoke(_.getDataFrameMetaData(name.stripPrefix(prefix), model),
              response.sendError(404, s"unknown action: ${request.getActionName()}"))
            val writer = new StringWriter();
            model.write(writer, "RDF/XML");
            response.sendData(writer.toString.getBytes("UTF-8"))
          case name if name.startsWith("/getDocument/") =>
            val dataFrameName = name.stripPrefix("/getDocument/")
            catalogServiceHolder.invoke(c => {
              val document = c.getDocument(dataFrameName)
              val schema = c.getSchema(dataFrameName)
              response.sendData(getDataFrameDocumentJsonString(document, schema).getBytes("UTF-8"))
            }, response.sendError(404, s"unknown action: ${request.getActionName()}"))
          case name if name.startsWith("/getStatistics/") =>
            catalogServiceHolder.invoke(c => {
              val statistics = c.getStatistics(name.stripPrefix("/getStatistics/"))
              response.sendData(getDataFrameStatisticsString(statistics).getBytes("UTF-8"))
            }, response.sendError(404, s"unknown action: ${request.getActionName()}"))
          case name if name.startsWith("/getDataFrameSize/") =>
            catalogServiceHolder.invoke(c => {
              val prefix: String = "/getDataFrameSize/"
              response.sendData(c.getStatistics(name.stripPrefix(prefix)).rowCount.toString.getBytes("UTF-8"))
            }, response.sendError(404, s"unknown action: ${request.getActionName()}"))
          case name if name.startsWith("/getSchema") =>
            catalogServiceHolder.invoke(c => {
              response.sendData(c.getSchema(name.stripPrefix("/getSchema/"))
                .getOrElse(StructType.empty)
                .toString.getBytes("UTF-8"))
            }, response.sendError(404, s"unknown action: ${request.getActionName()}"))
          case name if name.startsWith("/getDataFrameTitle/") =>
            catalogServiceHolder.invoke(c => {
              val dfName = name.stripPrefix("/getDataFrameTitle/")
              response.sendData(c.getDataFrameTitle(dfName)
                .getOrElse(dfName).getBytes("UTF-8"))
            }, response.sendError(404, s"unknown action: ${request.getActionName()}"))
          case _ => response.sendError(404, s"unknown action: ${request.getActionName()}")
        }
      } catch {
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
          case _: CollectActionMethodEvent => true
          case _: CollectGetStreamMethodEvent => true
          case _ => false
        }
      }

      override def doHandleEvent(event: CrossModuleEvent): Unit = {
        event match {
          case r: CollectActionMethodEvent => r.collect(actionMethodService)
          case r: CollectGetStreamMethodEvent => r.collect(
            new GetStreamMethod {
              override def accepts(request: DftpGetStreamRequest): Boolean =
                request match {
                  case r: DftpGetPathStreamRequest => r.getRequestPath() match {
                    case "/listDataSets" => true
                    case path if path.startsWith("/listDataFrames") => true
                    case "/listHosts" => true
                    case _ => false
                  }
                }

              override def doGetStream(request: DftpGetStreamRequest, response: DftpGetStreamResponse): Unit = {
                request match {
                  case r: DftpGetPathStreamRequest => r.getRequestPath() match {
                    case "/listDataSets" => catalogServiceHolder.invoke(c =>
                      response.sendDataFrame(c.doListDataSets(serverContext.baseUrl))
                      , response.sendError(404, s"DataFrame ${r.getRequestPath()} not Found"))
                    case path if path.startsWith("/listDataFrames") => catalogServiceHolder.invoke(c =>
                      response.sendDataFrame(c.doListDataFrames(path, serverContext.baseUrl)),
                      response.sendError(404, s"DataFrame ${r.getRequestPath()} not Found"))
                    case "/listHosts" => catalogServiceHolder.invoke(c =>
                      response.sendDataFrame(c.doListHostInfo(serverContext)),
                      response.sendError(404, s"DataFrame ${r.getRequestPath()} not Found"))
                  }
                }
              }
            })
          case _ =>
        }
      }
    })

    anchor.hook(new EventSource {
      override def init(eventHub: EventHub): Unit =
        eventHub.fireEvent(RequireCatalogServiceEvent(catalogServiceHolder))
    })
  }

  override def destroy(): Unit = {
  }
}