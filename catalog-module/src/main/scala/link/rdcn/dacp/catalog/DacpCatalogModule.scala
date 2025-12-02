package link.rdcn.dacp.catalog

import CatalogFormatter.{getDataFrameDocumentJsonString, getDataFrameStatisticsString, getHostInfoString, getHostResourceString}
import link.rdcn.server._
import link.rdcn.server.module.{ActionMethod, CollectActionMethodEvent, CollectGetStreamMethodEvent, GetStreamFilter, GetStreamFilterChain, GetStreamMethod, TaskRunner, Workers}
import link.rdcn.struct.StructType
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.json.JSONObject

import java.io.StringWriter

/**
 * @Author renhao
 * @Description:
 * @Data 2025/10/29 21:46
 * @Modified By:
 */
case class CollectCatalogServiceEvent(holder: Workers[CatalogService]) extends CrossModuleEvent

class DacpCatalogModule extends DftpModule {

  private val catalogServiceHolder = new Workers[CatalogService]

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
          case r: CollectActionMethodEvent => r.collect(new ActionMethod {

            //FIXME: match each request and returns true or false
            override def accepts(request: DftpActionRequest): Boolean = true

            override def doAction(request: DftpActionRequest, response: DftpActionResponse): Unit = {
              try{
                val actionName = request.getActionName()
                val parameter = request.getParameterAsMap()
                actionName match {
                  case "getDataSetMetaData" =>
                    val model: Model = ModelFactory.createDefaultModel
                    catalogServiceHolder.work(_.getDataSetMetaData(parameter.get("dataSetName").get.toString, model),
                      response.sendError(404, s"unknown action: ${request.getActionName()}"))
                    val writer = new StringWriter();
                    model.write(writer, "RDF/XML");
                    response.sendData(writer.toString.getBytes("UTF-8"))
                  case "getDataFrameMetaData" =>
                    val model: Model = ModelFactory.createDefaultModel
                    catalogServiceHolder.work(_.getDataFrameMetaData(parameter.get("dataFrameName").get.toString, model),
                      response.sendError(404, s"unknown action: ${request.getActionName()}"))
                    val writer = new StringWriter();
                    model.write(writer, "RDF/XML");
                    response.sendData(writer.toString.getBytes("UTF-8"))
                  case "getDocument" =>
                    val dataFrameName = parameter.get("dataFrameName").get.toString
                    catalogServiceHolder.work(c => {
                      val document = c.getDocument(dataFrameName)
                      val schema = c.getSchema(dataFrameName)
                      response.sendData(getDataFrameDocumentJsonString(document, schema).getBytes("UTF-8"))
                    }, response.sendError(404, s"unknown action: ${request.getActionName()}"))
                  case "getDataFrameInfo" =>
                    val dataFrameName = parameter.get("dataFrameName").get.toString
                    catalogServiceHolder.work(c => {
                      val dataFrameTitle = c.getDataFrameTitle(dataFrameName).getOrElse(dataFrameName)
                      val statistics = c.getStatistics(dataFrameName)
                      val jo = new JSONObject()
                      jo.put("byteSize", statistics.byteSize)
                      jo.put("rowCount", statistics.rowCount)
                      jo.put("title", dataFrameTitle)
                      response.sendData(jo.toString().getBytes("UTF-8"))
                    }, response.sendError(404, s"unknown action: ${request.getActionName()}"))
                  case "getSchema" =>
                    val dataFrameName = parameter.get("dataFrameName").get.toString
                    catalogServiceHolder.work(c => {
                      response.sendData(c.getSchema(dataFrameName)
                        .getOrElse(StructType.empty)
                        .toString.getBytes("UTF-8"))
                    }, response.sendError(404, s"unknown action: ${request.getActionName()}"))
                  case "getHostInfo" => response.sendData(getHostInfoString(serverContext).getBytes("UTF-8"))
                  case "getServerInfo" => response.sendData(getHostResourceString().getBytes("UTF-8"))
                  //FIXME: remove case _, default case match error will be handled by outer level method, e.g Workers.work()
                  case _ => response.sendError(404, s"unknown action: ${request.getActionName()}")
                }
              }catch {
                case e: Exception =>
                  response.sendError(500, e.getMessage)
                  throw e
              }
            }
          })

          case r: CollectGetStreamMethodEvent =>
            r.collect(new GetStreamMethod {
              override def accepts(request: DftpGetStreamRequest): Boolean =
                request match {
                  case r: DftpGetPathStreamRequest =>
                    r.getRequestPath() match {
                      case "/listDataSets" => true
                      case path if path.startsWith("/listDataFrames") => true
                      case _ => false
                    }
                  case _ => false
                }

              override def doGetStream(request: DftpGetStreamRequest, response: DftpGetStreamResponse): Unit = {
                request match {
                  case r: DftpGetPathStreamRequest => r.getRequestPath() match {
                    case "/listDataSets" =>
                      catalogServiceHolder.work(new TaskRunner[CatalogService, Unit] {
                        override def acceptedBy(worker: CatalogService): Boolean = true

                        override def executeWith(worker: CatalogService): Unit =
                          response.sendDataFrame(worker.doListDataSets(serverContext.baseUrl))

                        override def handleFailure(): Unit =
                          response.sendError(404, s"DataFrame ${r.getRequestPath()} not Found")
                      })
                    case path if path.startsWith("/listDataFrames") =>
                      catalogServiceHolder.work(new TaskRunner[CatalogService, Unit] {
                        override def acceptedBy(worker: CatalogService): Boolean = worker.accepts(
                          new CatalogServiceRequest {
                            override def getDataSetId: String = null

                            override def getDataFrameUrl: String = serverContext.baseUrl + r.getRequestPath()
                          })

                        override def executeWith(worker: CatalogService): Unit =
                          response.sendDataFrame(worker.doListDataFrames(path, serverContext.baseUrl))

                        override def handleFailure(): Unit = response.sendError(404, s"DataFrame ${r.getRequestPath()} not Found")
                      })
                    case _ =>
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
        eventHub.fireEvent(CollectCatalogServiceEvent(catalogServiceHolder))
    })
  }

  override def destroy(): Unit = {
  }
}