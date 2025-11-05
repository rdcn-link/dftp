/**
 * @Author Yomi
 * @Description:
 * @Data 2025/11/5 18:17
 * @Modified By:
 */
package link.rdcn.catalog

import link.rdcn.server.ServerContext
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
