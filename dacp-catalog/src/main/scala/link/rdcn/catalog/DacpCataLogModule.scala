package link.rdcn.catalog

import link.rdcn.server.{ActionMethodService, Anchor, CrossModuleEvent, DftpActionRequest, DftpActionResponse, DftpModule, EventHandleService, ServerContext}
import link.rdcn.struct.{DataStreamSource, Row, StructType}
import link.rdcn.struct.ValueType.StringType
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
class DacpCatalogModule extends DftpModule {

  private var serverContext: ServerContext = _
  private var dataProvider: DataProvider = _

  private val eventHandleService: EventHandleService = new EventHandleService {
    override def accepts(event: CrossModuleEvent): Boolean =
      event match {
        case _: DacpCatalogModule => true
        case _ => false
      }

    override def doHandleEvent(event: CrossModuleEvent): Unit =
      dataProvider = event.asInstanceOf[DataProvider]
  }

  private val actionMethodService = new ActionMethodService {

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
            val prefix: String = "/getDocument/"
            response.sendData(getDataFrameDocumentJsonString(name.replaceFirst(prefix, "")).getBytes("UTF-8"))
          case name if name.startsWith("/getStatistics/") =>
            val prefix: String = "/getStatistics/"
            response.sendData(getDataFrameStatisticsString(name.replaceFirst(prefix, "")).getBytes("UTF-8"))
          case name if name.startsWith("getDataFrameSize") =>
            val prefix: String = "/getDataFrameSize/"
            response.sendData(dataProvider.getDataStreamSource(name.replaceFirst(prefix, "")).rowCount.toString.getBytes("UTF-8"))
          case otherPath => response.sendError(400, s"Action $otherPath Invalid")
        }
      }catch {
        case e: Exception =>
          response.sendError(500, e.getMessage)
          throw e
      }

    }

    private def getDataFrameSchemaString(dataFrameName: String): String = {
      val structType = getSchema(dataFrameName)
      val schema = StructType.empty.add("name", StringType).add("valueType", StringType).add("nullable", StringType)
      val stream = structType.columns.map(col => Seq(col.name, col.colType.name, col.nullable.toString))
        .map(seq => Row.fromSeq(seq))
      val ja = new JSONArray()
      stream.map(_.toJsonObject(schema)).foreach(ja.put(_))
      ja.toString()
    }

    private def getDataFrameStatisticsString(dataFrameName: String): String = {
      val statistics = dataProvider.getStatistics(dataFrameName)
      val jo = new JSONObject()
      jo.put("byteSize", statistics.byteSize)
      jo.put("rowCount", statistics.rowCount)
      jo.toString()
    }

    private def getDataFrameDocumentJsonString(dataFrameName: String): String = {
      val document = dataProvider.getDocument(dataFrameName)
      val schema = StructType.empty.add("SchemaUrl",StringType).add("DataFrameTitle", StringType).add("ColumnUrl", StringType).add("ColumnAlias", StringType).add("ColumnTitle", StringType)
      val stream = getSchema(dataFrameName).columns.map(col => col.name).map(name => Seq(document.getSchemaURL(), document.getDataFrameTitle(), document.getColumnURL(name).getOrElse(""),
          document.getColumnAlias(name).getOrElse(""), document.getColumnTitle(name).getOrElse("")))
        .map(seq => link.rdcn.struct.Row.fromSeq(seq))
      val ja = new JSONArray()
      stream.map(_.toJsonObject(schema)).foreach(ja.put(_))
      ja.toString()
    }

    private def getSchema(dataFrameName: String): StructType = {
      val dataStreamSource: DataStreamSource = dataProvider.getDataStreamSource(dataFrameName)
      var structType = dataStreamSource.schema
      if (structType.isEmpty()) {
        val dataStreamSource: DataStreamSource = dataProvider.getDataStreamSource(dataFrameName)
        val iter = dataStreamSource.iterator
        if (iter.hasNext) {
          structType = DataUtils.inferSchemaFromRow(iter.next())
        }
      }
      structType
    }
  }

  override def init(anchor: Anchor, serverContext: ServerContext): Unit = {

    this.serverContext = serverContext

    anchor.hook(eventHandleService)
    anchor.hook(actionMethodService)
  }

  override def destroy(): Unit = {
  }
}