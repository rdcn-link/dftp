/**
 * @Author Yomi
 * @Description:
 * @Data 2025/11/5 18:17
 * @Modified By:
 */
package link.rdcn.catalog

import link.rdcn.catalog.MockCatalogData.{mockDoc, mockStats}
import link.rdcn.dacp.catalog.{CatalogService, CatalogServiceRequest}
import link.rdcn.server.{Anchor, CrossModuleEvent, EventHandler, ServerContext}
import link.rdcn.struct.ValueType.{IntType, StringType}
import link.rdcn.struct.{DataFrame, DataFrameDocument, DataFrameStatistics, DefaultDataFrame, Row, StructType}
import org.apache.jena.rdf.model.{Model, ModelFactory}

import java.io.StringWriter

/**
 * 模拟一个 ServerContext
 */
class MockServerContext extends ServerContext {
  override def getHost(): String = "mock-host"
  override def getPort(): Int = 9999
  override def getProtocolScheme(): String = "dftp"
  override def getDftpHome(): Option[String] = None
}


/**
 * 模拟 CatalogService 返回的常量数据
 */
object MockCatalogData {
  val mockSchema: StructType = StructType.empty.add("id", IntType).add("name", StringType)
  val mockTitle: String = "My Mock Table"
  val mockStats: DataFrameStatistics = new DataFrameStatistics {

    override def rowCount: Long = 123L

    override def byteSize: Long = 456L
  }
  val mockDoc: DataFrameDocument = new DataFrameDocument() {
    override def getSchemaURL(): Option[String] = Some("SchemaURL")

    override def getDataFrameTitle(): Option[String] = Some("DataFrameTitle")

    override def getColumnURL(colName: String): Option[String] = Some("ColumnURL")

    override def getColumnAlias(colName: String): Option[String] = Some("ColumnAlias")

    override def getColumnTitle(colName: String): Option[String] = Some("ColumnTitle")
  }

  val mockListDataSetDF: DataFrame = DefaultDataFrame(
    StructType.empty.add("datasetName", StringType).add("datasetUrl", StringType),
    Seq(Row("TestSet", "dftp://host/TestSet")).iterator
  )
  val mockListDataFrameDF: DataFrame = DefaultDataFrame(
    StructType.empty.add("dataFrameName", StringType).add("dataFrameUrl", StringType),
    Seq(Row("my_table", "dftp://host/TestSet/my_table")).iterator
  )
  val mockListHostDF: DataFrame = DefaultDataFrame(
    StructType.empty.add("hostName", StringType).add("hostUrl", StringType),
    Seq(Row("localhost", "dftp://localhost:3105")).iterator
  )

  // 用于模拟 RDF/XML 响应
  def getMockModel: Model = {
    val model = ModelFactory.createDefaultModel()
    val resource = model.createResource("http://example.org/mock")
    val property = model.createProperty("http://example.org/prop#value")
    resource.addProperty(property, "MockValue")
    model
  }

  def getModelAsRdfXmlString(model: Model): String = {
    val writer = new StringWriter()
    model.write(writer, "RDF/XML")
    writer.toString
  }
}
/**
 * 模拟一个 Anchor，用于捕获被 hook 的 EventHandler
 */
class MockAnchor extends Anchor {
  var hookedHandler: EventHandler = null

  // 模拟 Anchor 的 hook(EventHandler) 方法
  override def hook(service: EventHandler): Unit = {
    this.hookedHandler = service
  }

  // 提供一个空实现以满足 trait
  override def hook(service: link.rdcn.server.EventSource): Unit = {}

}



/**
 * 模拟一个不相关的事件，用于测试 'accepts' 方法
 */
class OtherMockEvent extends CrossModuleEvent

/**
 * 模拟 CatalogService 的实现，为所有抽象方法提供可预测的返回值
 */
class MockCatalogServiceImpl(schemaToReturn: Option[StructType]) extends CatalogService {

  // --- 模拟抽象方法的实现 ---

  override def accepts(request: CatalogServiceRequest): Boolean = true

  override def listDataSetNames(): List[String] = List("dataset1", "dataset2")

  override def getDataSetMetaData(dataSetId: String, rdfModel: Model): Unit = {
    // 添加一些可预测的 RDF 数据
    val resource = rdfModel.createResource(s"http://example.org/set/$dataSetId")
    val property = rdfModel.createProperty("http://example.org/prop#name")
    resource.addProperty(property, s"Mock Name for $dataSetId")
  }

  override def getDataFrameMetaData(dataFrameName: String, rdfModel: Model): Unit = {
    // 未被测方法使用，提供空实现
  }

  override def listDataFrameNames(dataSetId: String): List[String] =
    List(s"${dataSetId}_table_a", s"${dataSetId}_table_b")

  override def getDocument(dataFrameName: String): DataFrameDocument =
    mockDoc

  override def getStatistics(dataFrameName: String): DataFrameStatistics =
    mockStats

  override def getSchema(dataFrameName: String): Option[StructType] =
    this.schemaToReturn // 返回在构造时指定的 Schema

  override def getDataFrameTitle(dataFrameName: String): Option[String] =
    Some(s"Title for $dataFrameName")
}

