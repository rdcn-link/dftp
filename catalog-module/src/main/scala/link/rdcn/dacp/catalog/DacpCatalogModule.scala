package link.rdcn.dacp.catalog

import CatalogFormatter.{getDataFrameDocumentJsonString, getDataFrameStatisticsString, getHostInfoString, getHostResourceString}
import link.rdcn.Logging
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

class DacpCatalogModule extends DftpModule with Logging {

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
            override def accepts(request: DftpActionRequest): Boolean =
              request.getActionName() match {
                case "getDataSetMetaData" => true
                case "getDataFrameMetaData" => true
                case "getDocument" => true
                case "getDataFrameInfo" => true
                case "getSchema" => true
                case "getHostInfo" => true
                case "getServerInfo" => true
                case _ => false
              }

            override def doAction(request: DftpActionRequest, response: DftpActionResponse): Unit = {
              val actionName = request.getActionName()
              val parameter = request.getParameterAsMap()
              catalogServiceHolder.work[Unit](new TaskRunner[CatalogService, Unit] {
                override def acceptedBy(worker: CatalogService): Boolean =
                  worker.accepts(new CatalogServiceRequest {
                    override def getDataSetId: String = parameter.get("dataSetName").map(_.toString).orNull

                    override def getDataFrameUrl: String = parameter.get("dataFrameName").map(_.toString).orNull
                  })

                override def executeWith(worker: CatalogService): Unit = {
                  actionName match {
                    case "getDataSetMetaData" =>
                      val model: Model = ModelFactory.createDefaultModel
                      worker.getDataSetMetaData(parameter("dataSetName").toString, model)
                      val writer = new StringWriter();
                      model.write(writer, "RDF/XML");
                      response.sendData(writer.toString.getBytes("UTF-8"))
                    case "getDataFrameMetaData" =>
                      val model: Model = ModelFactory.createDefaultModel
                      worker.getDataFrameMetaData(parameter("dataFrameName").toString, model)
                      val writer = new StringWriter();
                      model.write(writer, "RDF/XML");
                      response.sendData(writer.toString.getBytes("UTF-8"))
                    case "getDocument" =>
                      val dataFrameName = parameter("dataFrameName").toString
                      val document = worker.getDocument(dataFrameName)
                      val schema = worker.getSchema(dataFrameName)
                      response.sendData(getDataFrameDocumentJsonString(document, schema).getBytes("UTF-8"))
                    case "getDataFrameInfo" =>
                      val dataFrameName = parameter("dataFrameName").toString
                      val dataFrameTitle = worker.getDataFrameTitle(dataFrameName).getOrElse(dataFrameName)
                      val statistics = worker.getStatistics(dataFrameName)
                      val jo = new JSONObject()
                      jo.put("byteSize", statistics.byteSize)
                      jo.put("rowCount", statistics.rowCount)
                      jo.put("title", dataFrameTitle)
                      response.sendData(jo.toString().getBytes("UTF-8"))
                    case "getSchema" =>
                      val dataFrameName = parameter("dataFrameName").toString
                      response.sendData(worker.getSchema(dataFrameName)
                        .getOrElse(StructType.empty)
                        .toString.getBytes("UTF-8"))
                    case "getHostInfo" => response.sendData(getHostInfoString(serverContext).getBytes("UTF-8"))
                    case "getServerInfo" => response.sendData(getHostResourceString().getBytes("UTF-8"))
                  }
                }
                override def handleFailure(): Unit =
                  response.sendError(404, s"unknown action: ${request.getActionName()}")
              })
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